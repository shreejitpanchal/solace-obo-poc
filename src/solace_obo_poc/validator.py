"""Validator service: forwards raw events into the matching validated topic space.

Where it sits in the flow
-------------------------
    publisher --> poc/msg/{sender}/{thread}  --> [validator] --> poc/msg-validate/{sender}/{thread}
    publisher --> poc/news/{sender}/{thread} --> [validator] --> poc/news-validate/{sender}/{thread}

What it does for each message
-----------------------------
1. Consume it from the validator's own durable queue (`q.poc.validator`), which
   subscribes to every raw space: `poc/msg/>` and `poc/news/>`.
2. Parse the topic to learn the sender-id and thread-id, which are needed to
   build the matching validated topic.
3. Republish the message unchanged (payload byte for byte, every user
   property including `obo-token`) to the validated space of the same kind
   (`poc/msg-validate/...` or `poc/news-validate/...`), adding an
   `obo-forwarded-at` timestamp property.

What it deliberately does NOT do
--------------------------------
It does not inspect or verify the OBO token. The token is carried through as
an opaque message property, and the authorization decision is made by each
consuming application (recipient, user-profiler, the news consumers) via
authz.authorize_event before the payload is read. The validator therefore
needs no issuer connection and no public keys.

The only messages it refuses are ones it cannot forward: a topic outside the
raw spaces (including its own validated output, which guards against a loop),
or a topic whose sender-id / thread-id levels are malformed. Those are
dropped and logged. The payload is never opened here.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable
from datetime import UTC, datetime

from solace_obo_poc.broker import Broker, Handler, Inbound, Outbound
from solace_obo_poc.config import broker_config, configure_logging
from solace_obo_poc.obo_token import TOKEN_PROPERTY, mask_token
from solace_obo_poc.topics import (
    parse_topic,
    validated_topic_for,
    validator_queue,
    validator_subscriptions,
)

log = logging.getLogger(__name__)

# Property added on republish: when (UTC, ISO-8601) this validator forwarded
# the event. Informational; consumers do not rely on it.
FORWARDED_AT_PROPERTY = "obo-forwarded-at"


def forward_message(msg: Inbound) -> Outbound:
    """Pure mapping: raw inbound message -> validated-topic outbound message.

    Raises ValueError with a human-readable reason when the topic is not a raw
    POC topic. No token handling, no I/O.
    """
    # parse_topic validates both levels, so a crafted topic never gets further.
    parts = parse_topic(msg.topic)
    if not parts.is_raw:
        # Guard against a loop: if someone widened the subscription to the
        # validated space, we must not forward our own output again.
        raise ValueError(f"validator only forwards raw topics, got {msg.topic!r}")

    # Carry every user property over untouched. The obo-token property is
    # what the downstream applications will verify; we do not look at it.
    properties = dict(msg.properties)
    properties[FORWARDED_AT_PROPERTY] = datetime.now(UTC).isoformat()
    return Outbound(
        topic=validated_topic_for(parts),  # same kind, validated space
        payload=msg.payload,  # untouched: the validator never decodes it
        properties=properties,
    )


def make_handler(publish: Callable[[Outbound], None]) -> Handler:
    """Wrap forward_message() into the callback the broker invokes per message.

    `publish` is injected (normally Broker.publish) so tests can capture what
    would have been republished. A message that cannot be forwarded is logged
    and dropped; the handler returns normally so the broker wrapper acks it
    and the queue keeps flowing.
    """

    def handle(msg: Inbound) -> None:
        token = mask_token(msg.properties.get(TOKEN_PROPERTY))
        log.info(
            "received %s (%d bytes, properties=%s, token %s)",
            msg.topic,
            len(msg.payload),
            sorted(msg.properties),
            token,
        )
        try:
            out = forward_message(msg)
        except ValueError as exc:
            # The token prefix is logged only for correlation; it is masked.
            log.warning(
                "dropped %s (token %s): %s",
                msg.topic,
                mask_token(msg.properties.get(TOKEN_PROPERTY)),
                exc,
            )
            return
        log.debug("forwarding to %s with properties=%s", out.topic, sorted(out.properties))
        publish(out)
        log.info("forwarded %s -> %s (token %s, not inspected)", msg.topic, out.topic, token)

    return handle


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-validator`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="POC validator service (forwarder)")
    parser.parse_args(argv)
    configure_logging()

    # Connect, bind to the validator queue (created on first start if the
    # broker allows it), and hand every message to the handler above.
    broker_cfg = broker_config()
    queue = validator_queue(broker_cfg.queue_prefix)  # q.poc.validator
    subscriptions = validator_subscriptions()  # ["poc/msg/>", "poc/news/>"]
    log.info(
        "validator starting: consume queue %s (subscriptions %s), forward each raw space "
        "to its validated space; the OBO token is passed through unverified",
        queue,
        subscriptions,
    )
    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(queue, subscriptions, make_handler(broker.publish))
        log.info("validator ready; waiting for events (Ctrl-C to stop)")
        broker.wait()
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
