"""News consumer: one of the six applications that receive every validated news event.

Where it sits in the flow
-------------------------
    validator --> poc/news-validate/{sender-id}/{thread-id} --> [news consumer x6]

The six applications (topics.NEWS_CONSUMERS)
--------------------------------------------
    Kai-Go-Mobile, Kai-Go-Desktop, Kai-Signals-Engine, Kai-StoreFront, Kai-Cowork, Kai-Zen

Each runs as its own process (`obo-news-consumer --app <name>`) and binds to
its own durable queue `q.poc.news.<name>`, whose topic subscription is
`poc/news-validate/>`: every news event, any sender, any thread. Six queues
on one subscription is how the broker fans one news event out to six
independent applications, each with its own spool while it is down.

How it authorizes events
------------------------
Same gate as the recipient and the profiler: consumer.make_handler ->
authz.authorize_event verifies the `obo-token` property against the issuer's
public keys and checks that the token's subject is the sender-id in the
topic, before the payload is decoded. Only then is on_event() below called.
"""

from __future__ import annotations

import argparse
import logging

from solace_obo_poc import consumer
from solace_obo_poc.authz import AuthorizedEvent
from solace_obo_poc.config import configure_logging
from solace_obo_poc.topics import NEWS_CONSUMERS, news_queue, news_subscription

log = logging.getLogger(__name__)
NAME = "news-consumer"


def on_event(event: AuthorizedEvent) -> None:
    """The application's business logic. In the POC: log the authorized news event.

    A real application (Kai-Go-Mobile, Kai-Zen, ...) would render or store
    the news item here, knowing from `event.claims` whose behalf it was
    published on.
    """
    # By the time we are here the token has been verified and the payload decoded.
    payload = event.payload if isinstance(event.payload, dict) else {}
    log.info(
        "%s received news seq=%s in thread %s from %s: %r (on behalf of %s, presented by %s)",
        NAME,
        payload.get("seq", "?"),
        event.topic.thread_id,
        event.topic.sender_id,
        payload.get("text", event.payload),
        event.claims.subject,
        event.claims.actor,
    )
    log.debug("%s full event: %s", NAME, consumer.describe(event))


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-news-consumer --app Kai-Zen`). Runs until Ctrl-C."""
    parser = argparse.ArgumentParser(description="News consumer application")
    # choices= rejects anything outside the six names before any connection
    # is made; the name becomes part of a queue name, so it is a closed set.
    parser.add_argument("--app", required=True, choices=NEWS_CONSUMERS, help="which application")
    args = parser.parse_args(argv)
    configure_logging()

    name = f"{NAME}[{args.app}]"
    log.info("%s starting: will receive every validated news event", name)

    # consumer.run() fetches the issuer keys, binds the queue with the
    # subscription, and calls on_event for every event that passes the gate.
    return consumer.run(
        name,
        lambda prefix: news_queue(prefix, args.app),  # q.poc.news.<app>
        news_subscription(),  # poc/news-validate/>
        on_event,
    )


if __name__ == "__main__":
    raise SystemExit(main())
