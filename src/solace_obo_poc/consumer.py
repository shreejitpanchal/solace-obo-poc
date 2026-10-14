"""Shared consumer loop for the recipient and user-profiler services.

Both differ only in the subscription they filter on. Each event goes through
`authorize_event`, so a bad or missing token is rejected before the payload is
decoded, then the service-specific `on_event` callback runs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from solace_obo_poc.authz import AuthorizationError, AuthorizedEvent, PayloadError, authorize_event
from solace_obo_poc.broker import Broker, Handler, Inbound
from solace_obo_poc.config import broker_config, token_config
from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, TokenError, mask_token

log = logging.getLogger(__name__)

OnEvent = Callable[[AuthorizedEvent], None]


def describe(event: AuthorizedEvent) -> str:
    return (
        f"sender={event.topic.sender_id} thread={event.topic.thread_id} "
        f"sub={event.claims.subject} act={event.claims.actor} payload={event.payload!r}"
    )


def make_handler(name: str, verifier: JwksVerifier, on_event: OnEvent) -> Handler:
    def handle(msg: Inbound) -> None:
        try:
            event = authorize_event(msg.topic, msg.properties, msg.payload, verifier)
        except AuthorizationError as exc:
            token = mask_token(msg.properties.get(TOKEN_PROPERTY))
            log.warning("%s: %s (token %s)", name, exc, token)
            return
        except PayloadError as exc:
            log.warning("%s: %s", name, exc)
            return
        on_event(event)

    return handle


QueueName = Callable[[str], str]  # queue prefix -> this consumer's queue name


def run(name: str, queue_name: QueueName, subscription: str, on_event: OnEvent) -> int:
    """Connect, bind to the consumer's queue, and block until Ctrl-C. Returns an exit code."""
    cfg = token_config()
    try:
        verifier = JwksVerifier.from_url(cfg.jwks_url, issuer=cfg.issuer, audience=cfg.audience)
    except TokenError as exc:
        log.error("%s: %s", name, exc)
        return 2
    broker_cfg = broker_config()
    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(
            queue_name(broker_cfg.queue_prefix),
            [subscription],
            make_handler(name, verifier, on_event),
        )
        broker.wait()
    finally:
        broker.close()
    return 0
