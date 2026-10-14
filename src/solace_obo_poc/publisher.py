"""Publisher: the application that creates events on behalf of a user.

Where it sits in the flow
-------------------------
    [publisher] --> poc/msg/{sender-id}/{thread-id} --> validator --> ...

What it does
------------
1. Ask the issuer for an OBO token for the user (`--sender-id`), presented by
   this service (`--actor`). See fetch_token().
2. For each event, build one Solace message with
       topic        poc/msg/<sender-id>/<thread-id>
       property     obo-token = <the JWT>
       payload      the event data as JSON
   See build_message(). The token travels as a *message property*, never
   inside the payload, so every hop can check it without opening the data.
3. Publish persistently and wait for the broker's acknowledgement, so once
   publish() returns the event is safely spooled on the broker.

Everything taken from the command line that ends up in a topic is validated
first (see topics.validate_level), so an id like `alice/>` cannot rewrite the
topic structure.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime

import httpx

from solace_obo_poc.broker import Broker, Outbound
from solace_obo_poc.config import TokenConfig, broker_config, configure_logging, token_config
from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError, mask_token
from solace_obo_poc.topics import raw_topic, validate_level

log = logging.getLogger(__name__)


def fetch_token(
    cfg: TokenConfig, sender_id: str, actor: str, *, client: httpx.Client | None = None
) -> str:
    """Ask the issuer for a token on behalf of `sender_id`, presented by `actor`.

    POST <OBO_ISSUER_URL>/token  {"sender_id": ..., "actor": ...}
    -> {"access_token": "<JWT>", "token_type": "Bearer", "expires_in": 300}

    In production this is where the real issuer (or an existing OBO exchange)
    is called; the rest of the flow only needs the JWT string back.
    `client` is injectable for tests.
    """
    try:
        response = (client or httpx).post(
            cfg.token_url, json={"sender_id": sender_id, "actor": actor}, timeout=10.0
        )
        response.raise_for_status()
        token = response.json().get("access_token")
    except (httpx.HTTPError, ValueError) as exc:
        raise TokenError(
            f"cannot obtain OBO token from {cfg.token_url}: {exc}; is the issuer running "
            "and OBO_ISSUER_URL pointing at it?"
        ) from exc
    if not token:
        raise TokenError(f"issuer at {cfg.token_url} returned no access_token")
    return token


def build_message(token: str, sender_id: str, thread_id: str, text: str, seq: int) -> Outbound:
    """Assemble one event: token in the user properties, event data in the payload."""
    # The payload is whatever the business application wants to send. These
    # fields are illustrative; nothing downstream depends on them except that
    # the payload is JSON.
    payload = {
        "sender_id": sender_id,
        "thread_id": thread_id,
        "seq": seq,
        "text": text,
        "sent_at": datetime.now(UTC).isoformat(),
    }
    return Outbound(
        topic=raw_topic(sender_id, thread_id),  # poc/msg/<sender-id>/<thread-id>
        payload=json.dumps(payload),
        properties={TOKEN_PROPERTY: token},  # "obo-token": the JWT, as metadata
    )


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-publisher`). Publishes --count events and exits."""
    parser = argparse.ArgumentParser(description="Publisher")
    parser.add_argument("--sender-id", required=True, help="user the events are sent on behalf of")
    parser.add_argument("--thread-id", required=True, help="conversation the events belong to")
    parser.add_argument("--message", default="hello from poc")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--actor", default="poc-publisher", help="service presenting the token")
    args = parser.parse_args(argv)
    configure_logging()

    # Reject unsafe identifiers before any network call. These values become
    # topic levels and token claims.
    try:
        validate_level(args.sender_id, "--sender-id")
        validate_level(args.thread_id, "--thread-id")
        validate_level(args.actor, "--actor")
    except ValueError as exc:
        parser.error(str(exc))

    # Step 1: one token for the whole run; it is valid for OBO_TOKEN_TTL_SECONDS.
    cfg = token_config()
    try:
        token = fetch_token(cfg, args.sender_id, args.actor)
    except TokenError as exc:
        log.error("%s", exc)
        return 2
    log.info("obtained OBO token %s for sub=%s", mask_token(token), args.sender_id)

    # Steps 2 and 3: build and publish each event. Broker.publish() blocks
    # until the broker has acknowledged the message.
    broker = Broker(broker_config()).connect()
    try:
        for seq in range(1, args.count + 1):
            msg = build_message(token, args.sender_id, args.thread_id, args.message, seq)
            broker.publish(msg)
            log.info("published seq=%d to %s", seq, msg.topic)
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
