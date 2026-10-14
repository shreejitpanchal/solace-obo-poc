"""User-profiler application: follows ONE user (sender-id) across every conversation.

Where it sits in the flow
-------------------------
    validator --> poc/msg-validate/{sender-id}/{thread-id} --> [user-profiler]

How it selects events
---------------------
The profiler for user `alice` binds to its own durable queue
`q.poc.profiler.alice`, whose topic subscription is `poc/msg-validate/alice/*`:
this sender only, any thread (`*`). The broker does the filtering; the
application never sees other users' events.

How it authorizes events
------------------------
Same gate as the recipient: consumer.make_handler -> authz.authorize_event
verifies the `obo-token` property against the issuer's public keys and checks
that the token's subject is the sender-id in the topic, before the payload is
decoded. Only then is on_event() below called with the readable event.
"""

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
    """The application's business logic. In the POC: log the authorized event.

    A real profiler would update the user's profile from `event.payload` here,
    knowing from `event.claims` that the event really is on behalf of that user.
    """
    # By the time we are here the token has been verified and the payload decoded.
    payload = event.payload if isinstance(event.payload, dict) else {}
    log.info(
        "%s profiling %s: message seq=%s in thread %s: %r (on behalf of %s, presented by %s)",
        NAME,
        event.topic.sender_id,
        payload.get("seq", "?"),
        event.topic.thread_id,
        payload.get("text", event.payload),
        event.claims.subject,
        event.claims.actor,
    )
    log.debug("%s full event: %s", NAME, consumer.describe(event))


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-profiler --sender-id A`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="User-profiler application")
    parser.add_argument("--sender-id", required=True, help="user to profile")
    args = parser.parse_args(argv)
    configure_logging()

    # The sender-id becomes a topic level and part of a queue name, so it is
    # validated before anything is built from it.
    try:
        validate_level(args.sender_id, "--sender-id")
    except ValueError as exc:
        parser.error(str(exc))

    log.info(
        "%s following user %s: will receive that user's messages in every thread",
        NAME,
        args.sender_id,
    )

    # consumer.run() fetches the issuer keys, binds the queue with the
    # subscription, and calls on_event for every event that passes the gate.
    return consumer.run(
        NAME,
        lambda prefix: profiler_queue(prefix, args.sender_id),  # q.poc.profiler.<sender-id>
        profiler_subscription(args.sender_id),  # poc/msg-validate/<sender-id>/*
        on_event,
    )


if __name__ == "__main__":
    raise SystemExit(main())
