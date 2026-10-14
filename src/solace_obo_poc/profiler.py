"""User-profiler application: filters validated events by sender-id, re-authorizes, then reads."""

from __future__ import annotations

import argparse
import logging

from solace_obo_poc import consumer
from solace_obo_poc.authz import AuthorizedEvent
from solace_obo_poc.config import configure_logging
from solace_obo_poc.topics import profiler_queue, profiler_subscription, validate_level

log = logging.getLogger(__name__)
NAME = "user-profiler"


def on_event(event: AuthorizedEvent) -> None:
    log.info("%s profiling: %s", NAME, consumer.describe(event))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="User-profiler application")
    parser.add_argument("--sender-id", required=True)
    args = parser.parse_args(argv)
    configure_logging()
    try:
        validate_level(args.sender_id, "--sender-id")
    except ValueError as exc:
        parser.error(str(exc))
    return consumer.run(
        NAME,
        lambda prefix: profiler_queue(prefix, args.sender_id),
        profiler_subscription(args.sender_id),
        on_event,
    )


if __name__ == "__main__":
    raise SystemExit(main())
