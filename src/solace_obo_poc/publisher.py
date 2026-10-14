"""Publisher: fetch an OBO token, publish events with it as a message property."""

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
    """Ask the issuer for a token on behalf of `sender_id`, presented by `actor`."""
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
    """Token in the user properties, event data in the payload."""
    payload = {
        "sender_id": sender_id,
        "thread_id": thread_id,
        "seq": seq,
        "text": text,
        "sent_at": datetime.now(UTC).isoformat(),
    }
    return Outbound(
        topic=raw_topic(sender_id, thread_id),
        payload=json.dumps(payload),
        properties={TOKEN_PROPERTY: token},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publisher")
    parser.add_argument("--sender-id", required=True)
    parser.add_argument("--thread-id", required=True)
    parser.add_argument("--message", default="hello from poc")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--actor", default="poc-publisher")
    args = parser.parse_args(argv)
    configure_logging()
    try:
        validate_level(args.sender_id, "--sender-id")
        validate_level(args.thread_id, "--thread-id")
        validate_level(args.actor, "--actor")
    except ValueError as exc:
        parser.error(str(exc))

    cfg = token_config()
    try:
        token = fetch_token(cfg, args.sender_id, args.actor)
    except TokenError as exc:
        log.error("%s", exc)
        return 2
    log.info("obtained OBO token %s for sub=%s", mask_token(token), args.sender_id)

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
