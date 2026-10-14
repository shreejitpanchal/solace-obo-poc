"""Recipient application: filters validated events by thread-id, re-authorizes, then reads."""

from __future__ import annotations

import argparse
import logging

from solace_obo_poc import consumer
from solace_obo_poc.authz import AuthorizedEvent
from solace_obo_poc.config import configure_logging
from solace_obo_poc.topics import recipient_queue, recipient_subscription, validate_level

log = logging.getLogger(__name__)
NAME = "recipient"


def on_event(event: AuthorizedEvent) -> None:
    log.info("%s received: %s", NAME, consumer.describe(event))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recipient application")
    parser.add_argument("--thread-id", required=True)
    args = parser.parse_args(argv)
    configure_logging()
    try:
        validate_level(args.thread_id, "--thread-id")
    except ValueError as exc:
        parser.error(str(exc))
    return consumer.run(
        NAME,
        lambda prefix: recipient_queue(prefix, args.thread_id),
        recipient_subscription(args.thread_id),
        on_event,
    )


if __name__ == "__main__":
    raise SystemExit(main())
