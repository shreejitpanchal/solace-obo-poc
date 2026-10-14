"""Shared consumer loop for the recipient and the user-profiler applications.

The two applications are identical except for *which* validated events they
want, and that difference is expressed entirely on the broker side:

    recipient     queue q.poc.recipient.<thread-id>   subscription poc/msg-validate/*/<thread-id>
    user-profiler queue q.poc.profiler.<sender-id>    subscription poc/msg-validate/<sender-id>/*

Everything else -- fetching the issuer's keys, binding to the queue, verifying
the token on every message before reading it, logging rejections -- lives
here, so there is one implementation of the consumer-side security rule.

Per message, `make_handler` does:

    broker delivers Inbound(topic, payload, properties)
      -> authorize_event(): topic parsed, token verified, sub == sender-id,
                            and only then the payload is decoded
      -> on_event(AuthorizedEvent)   the application's own business logic

A rejected event is logged with a masked token and dropped; the handler
returns normally so the broker wrapper acks it and the queue keeps flowing.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from solace_obo_poc.authz import AuthorizationError, AuthorizedEvent, PayloadError, authorize_event
from solace_obo_poc.broker import Broker, Handler, Inbound
from solace_obo_poc.config import broker_config, token_config
from solace_obo_poc.obo_token import TOKEN_PROPERTY, JwksVerifier, TokenError, mask_token

log = logging.getLogger(__name__)

# The application's callback: receives an event that has already passed the
# token gate. It never sees a raw, unverified message.
OnEvent = Callable[[AuthorizedEvent], None]


def describe(event: AuthorizedEvent) -> str:
    """One-line, log-safe summary of an authorized event (no token material)."""
    return (
        f"sender={event.topic.sender_id} thread={event.topic.thread_id} "
        f"sub={event.claims.subject} act={event.claims.actor} payload={event.payload!r}"
    )


def make_handler(name: str, verifier: JwksVerifier, on_event: OnEvent) -> Handler:
    """Build the per-message callback: authorize first, then hand over to the application.

    `name` is only used in log lines so the two applications can be told apart.
    """

    def handle(msg: Inbound) -> None:
        token = mask_token(msg.properties.get(TOKEN_PROPERTY))
        log.info(
            "%s: received %s (%d bytes, properties=%s, token %s); authorizing before read",
            name,
            msg.topic,
            len(msg.payload),
            sorted(msg.properties),
            token,
        )
        try:
            # The whole security decision happens inside authorize_event().
            # If it returns, the token was valid for this sender and the
            # payload has been decoded; if it raises, nothing was read.
            event = authorize_event(msg.topic, msg.properties, msg.payload, verifier)
        except AuthorizationError as exc:
            # Bad or missing token, or token/topic mismatch. Log a masked
            # prefix only: tokens are credentials and never go to logs whole.
            log.warning("%s: %s (token %s); payload NOT read", name, exc, token)
            return
        except PayloadError as exc:
            # Token was fine but the payload is not JSON: a publisher bug,
            # not a security event. Still dropped, still logged.
            log.warning("%s: %s", name, exc)
            return
        log.info(
            "%s: authorized %s: token sub=%s matches sender-id, act=%s, expires=%s; "
            "handing payload to the application",
            name,
            msg.topic,
            event.claims.subject,
            event.claims.actor,
            event.claims.expires_at.isoformat(timespec="seconds"),
        )
        on_event(event)

    return handle


# Given the configured queue prefix (SOLACE_QUEUE_PREFIX), return this
# consumer's queue name. Each application supplies its own, e.g.
# lambda prefix: recipient_queue(prefix, thread_id).
QueueName = Callable[[str], str]


def run(name: str, queue_name: QueueName, subscription: str, on_event: OnEvent) -> int:
    """Connect, bind to the consumer's queue, and block until Ctrl-C. Returns an exit code."""
    # 1. Issuer public keys, fetched once. All later verification is offline.
    cfg = token_config()
    try:
        verifier = JwksVerifier.from_url(cfg.jwks_url, issuer=cfg.issuer, audience=cfg.audience)
    except TokenError as exc:
        log.error("%s: %s", name, exc)
        return 2

    # 2. Broker: bind to this consumer's durable queue. The queue (and its
    #    topic subscription) is created on first start when the broker allows
    #    it; afterwards it also spools events while the application is down.
    broker_cfg = broker_config()
    queue = queue_name(broker_cfg.queue_prefix)
    log.info("%s starting: queue=%s subscription=%s", name, queue, subscription)
    broker = Broker(broker_cfg).connect()
    try:
        broker.subscribe(queue, [subscription], make_handler(name, verifier, on_event))
        # 3. Messages now arrive on the broker's callback thread; the main
        #    thread just waits for Ctrl-C.
        log.info("%s ready; waiting for events (Ctrl-C to stop)", name)
        broker.wait()
    finally:
        broker.close()
    return 0
