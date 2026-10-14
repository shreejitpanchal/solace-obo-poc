"""Thin wrapper over the Solace Python API (solace-pubsubplus): what the POC needs, nothing else.

Why a wrapper
-------------
The services and their tests talk to `Broker`, `Inbound` and `Outbound`, not
to the SDK. That keeps the security logic (validator.py, authz.py,
consumer.py) free of broker details and unit-testable without a broker. The
SDK, which ships a native library, is imported inside the methods that need
it, so importing this module never requires a broker or the native library.

Messaging model
---------------
Guaranteed (persistent) messaging throughout:
  * publish() waits for the broker's acknowledgement, so a returned call means
    the event is spooled on the broker;
  * subscribe() binds a persistent receiver to a durable exclusive queue that
    carries the topic subscription, so events are spooled while the consumer
    is down and delivered when it rebinds;
  * each message is acknowledged after the handler returns.

Message shape used by the flow
------------------------------
    topic        poc/msg/<sender-id>/<thread-id>  or  poc/msg-validate/...
    properties   user properties, e.g. "obo-token": <JWT>  (metadata)
    payload      bytes, the application data (JSON in this POC)
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from solace_obo_poc.config import BrokerConfig

log = logging.getLogger(__name__)

# How long publish() waits for the broker's ack before raising.
PUBLISH_ACK_TIMEOUT_MS = 10_000


@dataclass(frozen=True)
class Inbound:
    """A received message, SDK-free: what handlers get."""

    topic: str  # the exact topic the message was published to
    payload: bytes
    properties: Mapping[str, str] = field(default_factory=dict)  # user properties


@dataclass(frozen=True)
class Outbound:
    """A message to publish."""

    topic: str
    payload: bytes | str
    properties: Mapping[str, str] = field(default_factory=dict)


# A handler receives one Inbound per message. It should return normally
# (the message is then acked) and never raise for a bad message: log and drop.
Handler = Callable[[Inbound], None]


def to_inbound(message: Any) -> Inbound:
    """Convert an SDK InboundMessage into the SDK-free shape handlers consume.

    get_properties() returns the user properties set by the publisher via
    with_property(); that is where the OBO token lives.
    """
    props = message.get_properties() or {}
    return Inbound(
        topic=message.get_destination_name(),
        payload=message.get_payload_as_bytes() or b"",
        properties={str(k): str(v) for k, v in props.items()},
    )


class Broker:
    """One connection to the broker, used for publishing and/or consuming."""

    def __init__(self, cfg: BrokerConfig) -> None:
        self._cfg = cfg
        self._service: Any = None  # MessagingService once connected
        self._publisher: Any = None  # PersistentMessagePublisher, created lazily
        self._receivers: list[Any] = []  # PersistentMessageReceiver per subscribe()
        self._stop = threading.Event()

    def connect(self) -> Broker:
        """Open the SMF connection using SOLACE_HOST / VPN / USERNAME / PASSWORD."""
        from solace.messaging.messaging_service import MessagingService, RetryStrategy

        props = {
            "solace.messaging.transport.host": self._cfg.host,
            "solace.messaging.service.vpn-name": self._cfg.vpn,
            "solace.messaging.authentication.scheme.basic.username": self._cfg.username,
            "solace.messaging.authentication.scheme.basic.password": self._cfg.password,
        }
        # Initial-connect retries (60 x 3 s) cover a broker container that is
        # still booting; reconnection retries cover a broker restart mid-run.
        self._service = (
            MessagingService.builder()
            .from_properties(props)
            .with_connection_retry_strategy(RetryStrategy.parametrized_retry(60, 3000))
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3000))
            .build()
        )
        self._service.connect()
        log.info("connected to broker %s", self._cfg.masked())  # password masked
        return self

    def publish(self, msg: Outbound) -> None:
        """Persistent publish; returns once the broker has spooled the message.

        User properties are attached with with_property(); for the flow that
        means {"obo-token": <JWT>}. The payload goes in as given.
        """
        from solace.messaging.resources.topic import Topic

        if self._publisher is None:
            self._publisher = self._service.create_persistent_message_publisher_builder().build()
            self._publisher.start()
        builder = self._service.message_builder()
        for key, value in msg.properties.items():
            builder = builder.with_property(key, value)
        log.debug(
            "publishing to %s: %d bytes, properties=%s, waiting for broker ack",
            msg.topic,
            len(msg.payload),
            sorted(msg.properties),
        )
        self._publisher.publish_await_acknowledgement(
            builder.build(msg.payload), Topic.of(msg.topic), PUBLISH_ACK_TIMEOUT_MS
        )
        log.debug("broker acknowledged %s (spooled to every matching queue)", msg.topic)

    def subscribe(self, queue_name: str, subscriptions: list[str], handler: Handler) -> None:
        """Bind to a durable exclusive queue carrying `subscriptions`; deliver to `handler`.

        With `create_queues` on (the default, SOLACE_CREATE_QUEUES=true) a
        missing queue and its subscriptions are created on start, which needs
        "allow guaranteed endpoint create" on the client profile (on in the
        default profile). Off means the queue must be pre-provisioned by an
        administrator, and a missing one fails loudly here.

        Delivery is asynchronous: the SDK calls the handler on its own thread.
        """
        from solace.messaging.config.missing_resources_creation_configuration import (
            MissingResourcesCreationStrategy,
        )
        from solace.messaging.receiver.message_receiver import MessageHandler
        from solace.messaging.resources.queue import Queue
        from solace.messaging.resources.topic_subscription import TopicSubscription

        strategy = (
            MissingResourcesCreationStrategy.CREATE_ON_START
            if self._cfg.create_queues
            else MissingResourcesCreationStrategy.DO_NOT_CREATE
        )
        receiver = (
            self._service.create_persistent_message_receiver_builder()
            .with_missing_resources_creation_strategy(strategy)
            .with_subscriptions([TopicSubscription.of(s) for s in subscriptions])
            .build(Queue.durable_exclusive_queue(queue_name))
        )

        class _Adapter(MessageHandler):
            """SDK callback -> our Handler, with ack handling."""

            def on_message(self, message: Any) -> None:
                # Handlers drop bad events deliberately and return normally. An
                # exception here is a bug, not a transient fault, so the message
                # is still acked: redelivering a poison message forever helps
                # nobody in a POC. Swap for settle(FAILED) if redelivery is wanted.
                inbound = to_inbound(message)
                log.debug("queue %s delivered a message on %s", queue_name, inbound.topic)
                try:
                    handler(inbound)
                except Exception:
                    log.exception("handler failed; message dropped")
                finally:
                    receiver.ack(message)
                    log.debug("acked message on %s from queue %s", inbound.topic, queue_name)

        receiver.start()
        receiver.receive_async(_Adapter())
        self._receivers.append(receiver)
        log.info("consuming queue %s with subscriptions %s", queue_name, subscriptions)

    def wait(self) -> None:
        """Block the main thread until Ctrl-C or `stop()`; messages arrive on SDK threads."""
        try:
            while not self._stop.wait(1.0):
                pass
        except KeyboardInterrupt:
            pass

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        """Terminate receivers and publisher, then disconnect. Safe to call twice."""
        for receiver in self._receivers:
            receiver.terminate()
        self._receivers.clear()
        if self._publisher is not None:
            self._publisher.terminate()
            self._publisher = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
