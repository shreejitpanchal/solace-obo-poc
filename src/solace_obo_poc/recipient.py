"""Recipient application: follows ONE conversation (thread-id), whoever sent the messages.

Where it sits in the flow
-------------------------
    validator --> poc/msg-validate/{sender-id}/{thread-id} --> [recipient]

How it selects events
---------------------
The recipient for thread `t-1` binds to its own durable queue
`q.poc.recipient.t-1`, whose topic subscription is `poc/msg-validate/*/t-1`:
any sender (`*`), this thread only. The broker does the filtering; the
application never sees events from other threads.

How it authorizes events
------------------------
It does NOT trust the validator hop. Every message goes through
consumer.make_handler -> authz.authorize_event, which verifies the
`obo-token` property against the issuer's public keys and checks that the
token's subject is the sender-id in the topic, before the payload is decoded.
Only then is on_event() below called with the readable event.
"""

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
    """The application's business logic. In the POC: log the authorized event.

    `event.payload` is the decoded JSON, `event.claims` says who the token was
    issued for and which service presented it, `event.topic` has the parsed
    sender-id / thread-id. A real recipient would deliver the message here.
    """
    log.info("%s received: %s", NAME, consumer.describe(event))


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-recipient --thread-id T`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="Recipient application")
    parser.add_argument("--thread-id", required=True, help="conversation to follow")
    args = parser.parse_args(argv)
    configure_logging()

    # The thread-id becomes a topic level and part of a queue name, so it is
    # validated before anything is built from it.
    try:
        validate_level(args.thread_id, "--thread-id")
    except ValueError as exc:
        parser.error(str(exc))

    # consumer.run() fetches the issuer keys, binds the queue with the
    # subscription, and calls on_event for every event that passes the gate.
    return consumer.run(
        NAME,
        lambda prefix: recipient_queue(prefix, args.thread_id),  # q.poc.recipient.<thread-id>
        recipient_subscription(args.thread_id),  # poc/msg-validate/*/<thread-id>
        on_event,
    )


if __name__ == "__main__":
    raise SystemExit(main())
