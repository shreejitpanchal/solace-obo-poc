"""Publisher: the application that creates events on behalf of a user.

Where it sits in the flow
-------------------------
    [publisher] --> poc/msg/{sender-id}/{thread-id} --> validator --> ...

What it does
------------
1. Ask the issuer for an OBO token for the user (`--sender-id`), presented by
   this service (`--actor`). See fetch_token(). The token has a lifetime; the
   publisher refreshes it shortly before it expires so a long run keeps
   producing events that verify.
2. Every PUBLISH_INTERVAL_SECONDS (default 5), build one Solace message with
       topic        poc/msg/<sender-id>/<thread-id>
       property     obo-token = <the JWT>
       payload      the event data as JSON
   See build_message(). The token travels as a *message property*, never
   inside the payload, so every hop can check it without opening the data.
3. Publish persistently and wait for the broker's acknowledgement, so once
   publish() returns the event is safely spooled on the broker.
4. Keep going until Ctrl-C, or until PUBLISH_COUNT events have been sent when
   that is set to a positive number.

Everything taken from the command line that ends up in a topic is validated
first (see topics.validate_level), so an id like `alice/>` cannot rewrite the
topic structure.
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from solace_obo_poc.broker import Broker, Outbound
from solace_obo_poc.config import (
    ConfigError,
    TokenConfig,
    broker_config,
    configure_logging,
    publisher_config,
    token_config,
)
from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError, mask_token
from solace_obo_poc.topics import raw_topic, validate_level

log = logging.getLogger(__name__)

# Refresh the token when less than this is left of its lifetime, so an event
# never leaves with a token that expires while the validator and the
# applications are still processing it.
REFRESH_MARGIN = timedelta(seconds=30)


@dataclass(frozen=True)
class IssuedToken:
    token: str
    expires_at: datetime

    def needs_refresh(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(UTC)) >= self.expires_at - REFRESH_MARGIN


def fetch_token(
    cfg: TokenConfig, sender_id: str, actor: str, *, client: httpx.Client | None = None
) -> IssuedToken:
    """Ask the issuer for a token on behalf of `sender_id`, presented by `actor`.

    POST <OBO_ISSUER_URL>/token  {"sender_id": ..., "actor": ...}
    -> {"access_token": "<JWT>", "token_type": "Bearer", "expires_in": 300}

    In production this is where the real issuer (or an existing OBO exchange)
    is called; the rest of the flow only needs the JWT string and its expiry.
    `client` is injectable for tests.
    """
    log.info("requesting OBO token from %s for sub=%s act=%s", cfg.token_url, sender_id, actor)
    try:
        response = (client or httpx).post(
            cfg.token_url, json={"sender_id": sender_id, "actor": actor}, timeout=10.0
        )
        response.raise_for_status()
        body = response.json()
        token = body.get("access_token")
        expires_in = int(body.get("expires_in", cfg.ttl_seconds))
    except (httpx.HTTPError, ValueError) as exc:
        raise TokenError(
            f"cannot obtain OBO token from {cfg.token_url}: {exc}; is the issuer running "
            "and OBO_ISSUER_URL pointing at it?"
        ) from exc
    if not token:
        raise TokenError(f"issuer at {cfg.token_url} returned no access_token")
    issued = IssuedToken(token=token, expires_at=datetime.now(UTC) + timedelta(seconds=expires_in))
    log.info(
        "obtained OBO token %s for sub=%s, valid for %ds (until %s)",
        mask_token(token),
        sender_id,
        expires_in,
        issued.expires_at.isoformat(timespec="seconds"),
    )
    return issued


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
    """Process entry point (`obo-publisher`). Publishes until Ctrl-C or --count events."""
    parser = argparse.ArgumentParser(description="Publisher")
    parser.add_argument("--sender-id", required=True, help="user the events are sent on behalf of")
    parser.add_argument("--thread-id", required=True, help="conversation the events belong to")
    parser.add_argument("--message", default="hello from poc")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="events to publish, then exit; 0 = until stopped (default: PUBLISH_COUNT or 0)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="seconds between events (default: PUBLISH_INTERVAL_SECONDS or 5)",
    )
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

    # Command-line flags win over the environment, which wins over defaults.
    try:
        pub_cfg = publisher_config()
    except ConfigError as exc:
        log.error("%s", exc)
        return 2
    count = pub_cfg.count if args.count is None else args.count
    interval = pub_cfg.interval_seconds if args.interval is None else args.interval
    if count < 0 or interval < 0:
        parser.error("--count and --interval must not be negative")

    # Step 1: first token. Later ones are fetched automatically before expiry.
    cfg = token_config()
    try:
        issued = fetch_token(cfg, args.sender_id, args.actor)
    except TokenError as exc:
        log.error("%s", exc)
        return 2

    log.info(
        "publishing to %s every %.1fs as sub=%s (%s); Ctrl-C to stop",
        raw_topic(args.sender_id, args.thread_id),
        interval,
        args.sender_id,
        f"{count} events" if count else "until stopped",
    )

    # Steps 2 to 4: build and publish on a timer. Broker.publish() blocks
    # until the broker has acknowledged each message. The Event makes the
    # pause interruptible so Ctrl-C stops the loop promptly.
    pause = threading.Event()
    broker = Broker(broker_config()).connect()
    seq = 0
    try:
        while count == 0 or seq < count:
            seq += 1
            if issued.needs_refresh():
                log.info("OBO token %s is about to expire; refreshing", mask_token(issued.token))
                try:
                    issued = fetch_token(cfg, args.sender_id, args.actor)
                except TokenError as exc:
                    log.error("%s", exc)
                    return 2
            msg = build_message(issued.token, args.sender_id, args.thread_id, args.message, seq)
            broker.publish(msg)
            log.info("published seq=%d to %s (token %s)", seq, msg.topic, mask_token(issued.token))
            if count and seq >= count:
                break
            pause.wait(interval)
    except KeyboardInterrupt:
        log.info("stopped by user after %d event(s)", seq)
    finally:
        broker.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
