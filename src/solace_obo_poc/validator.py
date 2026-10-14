"""Validator service: the gatekeeper between the raw and the validated topic space.

Where it sits in the flow
-------------------------
    publisher --> poc/msg/{sender}/{thread} --> [validator] --> poc/msg-validate/{sender}/{thread}

The publisher attaches an OBO token to every event as the `obo-token` message
property. Nothing that only subscribes to `poc/msg-validate/>` can ever see an
event unless this service has verified that token first.

What it does for each message
-----------------------------
1. Consume it from the validator's own durable queue (`q.poc.validator`), which
   subscribes to `poc/msg/>`, i.e. every raw event.
2. Parse the topic to learn which sender-id and thread-id the event claims.
3. Read the token from the `obo-token` message property. No property -> reject.
4. Verify the token offline against the issuer's public keys (JWKS): signature,
   issuer, audience, expiry, required claims.
5. Bind the token to the topic: the token's `sub` must equal the sender-id in
   the topic, so a valid token for alice cannot carry an event labelled bob.
6. Republish the payload, byte for byte unchanged, to the validated topic, with
   the token still attached (downstream applications re-verify it themselves)
   plus an `obo-validated-at` timestamp property.

A message that fails any step is dropped and logged with the reason and a
masked token prefix. The payload is never opened here: the validator has no
business reading application data.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from solace_obo_poc.broker import Broker, Handler, Inbound, Outbound
from solace_obo_poc.config import broker_config, configure_logging, token_config
from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, TokenError, mask_token
from solace_obo_poc.topics import parse_topic, raw_subscription, validated_topic, validator_queue

log = logging.getLogger(__name__)

# Second message property added on republish: when (UTC, ISO-8601) this
# validator admitted the event. Informational; consumers do not rely on it.
VALIDATED_AT_PROPERTY = "obo-validated-at"


def validate_message(msg: Inbound, verifier: JwksVerifier) -> Outbound:
    """Pure decision function: raw inbound message -> validated outbound message.

    Raises TokenError (token problem) or ValueError (topic problem) with a
    human-readable reason instead of returning None, so the caller can log
    exactly why an event was refused. No I/O happens here, which is what makes
    the validator's rules unit-testable without a broker.
    """
    # Step 2: which sender-id / thread-id does the event claim? parse_topic
    # also validates both levels, so a crafted topic never gets further.
    parts = parse_topic(msg.topic)
    if not parts.is_raw:
        # Guard against a loop: if someone widened the subscription to the
        # validated space, we must not re-validate our own output.
        raise ValueError(f"validator only accepts raw topics, got {msg.topic!r}")

    # Steps 3 and 4: pull the token out of the message *properties* (not the
    # payload) and verify it. verify() raises TokenError when the property is
    # missing, the signature is wrong, the token expired, or iss/aud differ.
    claims = verifier.verify(msg.properties.get(TOKEN_PROPERTY))

    # Step 5: the token says who the event is on behalf of (`sub`); the topic
    # says who the publisher *claims* sent it. They must agree.
    if claims.subject != parts.sender_id:
        raise TokenError(
            f"token subject {claims.subject!r} does not match sender-id {parts.sender_id!r}"
        )

    # Step 6: build the republish. The original properties (including the
    # token) are carried over so the applications can run the same checks.
    properties = dict(msg.properties)
    properties[VALIDATED_AT_PROPERTY] = datetime.now(UTC).isoformat()
    return Outbound(
        topic=validated_topic(parts.sender_id, parts.thread_id),
        payload=msg.payload,  # untouched: the validator never decodes it
        properties=properties,
    )


def make_handler(publish: Callable[[Outbound], None], verifier: JwksVerifier) -> Handler:
    """Wrap validate_message() into the callback the broker invokes per message.

    `publish` is injected (normally Broker.publish) so tests can capture what
    would have been republished. A rejected message is logged and dropped;
    the handler returns normally so the broker wrapper acks it and the queue
    keeps flowing. One bad event must never stall the validator.
    """

    def handle(msg: Inbound) -> None:
        try:
            out = validate_message(msg, verifier)
        except (TokenError, ValueError) as exc:
            # mask_token() logs only the first characters: enough to correlate
            # with the issuer's logs, never enough to replay the token.
            log.warning(
                "dropped %s (token %s): %s",
                msg.topic,
                mask_token(msg.properties.get(TOKEN_PROPERTY)),
                exc,
            )
            return
        publish(out)
        log.info("validated %s -> %s", msg.topic, out.topic)

    return handle


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-validator`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="POC validator service")
    parser.parse_args(argv)
    configure_logging()

    # Fetch the issuer's public keys once at start-up. From here on every
    # verification is local: no call to the issuer per message.
    cfg = token_config()
    try:
        verifier = JwksVerifier.from_url(cfg.jwks_url, issuer=cfg.issuer, audience=cfg.audience)
    except TokenError as exc:
        log.error("%s", exc)
        return 2

    # Connect, bind to the validator queue (created on first start if the
    # broker allows it), and hand every message to the handler above.
    broker_cfg = broker_config()
    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(
            validator_queue(broker_cfg.queue_prefix),  # q.poc.validator
            [raw_subscription()],  # poc/msg/>
            make_handler(broker.publish, verifier),
        )
        broker.wait()
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
