"""Thin wrapper over solace-pubsubplus: exactly what the POC needs, nothing else.

Guaranteed messaging throughout: publishers wait for the broker's ack, and
every consumer binds to its own durable queue that carries its topic
subscription, so events are spooled while a consumer is down.

Services and their tests depend on `Inbound`/`Outbound` and a handler callable,
not on the SDK. The SDK (and its native library) is imported inside the methods
that talk to a real broker, so importing this module never needs a broker.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from solace_obo_poc.config import BrokerConfig

log = logging.getLogger(__name__)

PUBLISH_ACK_TIMEOUT_MS = 10_000


@dataclass(frozen=True)
class Inbound:
    topic: str
    payload: bytes
    properties: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Outbound:
    topic: str
    payload: bytes | str
    properties: Mapping[str, str] = field(default_factory=dict)


Handler = Callable[[Inbound], None]


def to_inbound(message: Any) -> Inbound:
    """Convert an SDK InboundMessage into the SDK-free shape handlers consume."""
    props = message.get_properties() or {}
    return Inbound(
        topic=message.get_destination_name(),
        payload=message.get_payload_as_bytes() or b"",
        properties={str(k): str(v) for k, v in props.items()},
    )


class Broker:
    def __init__(self, cfg: BrokerConfig) -> None:
        self._cfg = cfg
        self._service: Any = None
        self._publisher: Any = None
        self._receivers: list[Any] = []
        self._stop = threading.Event()

    def connect(self) -> Broker:
        from solace.messaging.messaging_service import MessagingService, RetryStrategy

        props = {
            "solace.messaging.transport.host": self._cfg.host,
            "solace.messaging.service.vpn-name": self._cfg.vpn,
            "solace.messaging.authentication.scheme.basic.username": self._cfg.username,
            "solace.messaging.authentication.scheme.basic.password": self._cfg.password,
        }
        # Initial-connect retries cover a broker container that is still booting;
        # reconnection retries cover a restart mid-run.
        self._service = (
            MessagingService.builder()
            .from_properties(props)
            .with_connection_retry_strategy(RetryStrategy.parametrized_retry(60, 3000))
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3000))
            .build()
        )
        self._service.connect()
        log.info("connected to broker %s", self._cfg.masked())
        return self

    def publish(self, msg: Outbound) -> None:
        """Persistent publish; returns once the broker has spooled the message."""
        from solace.messaging.resources.topic import Topic

        if self._publisher is None:
            self._publisher = self._service.create_persistent_message_publisher_builder().build()
            self._publisher.start()
        builder = self._service.message_builder()
        for key, value in msg.properties.items():
            builder = builder.with_property(key, value)
        self._publisher.publish_await_acknowledgement(
            builder.build(msg.payload), Topic.of(msg.topic), PUBLISH_ACK_TIMEOUT_MS
        )

    def subscribe(self, queue_name: str, subscriptions: list[str], handler: Handler) -> None:
        """Bind to a durable exclusive queue carrying `subscriptions`; deliver to `handler`.

        With `create_queues` on (the default) a missing queue and its subscriptions
        are created on start, which needs "allow guaranteed endpoint create" on
        the client profile (on in the default profile). Off means the queue must
        be pre-provisioned, and a missing one fails loudly here.
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
            def on_message(self, message: Any) -> None:
                # Handlers drop bad events deliberately and return normally. An
                # exception here is a bug, not a transient fault, so the message
                # is still acked: redelivering a poison message forever helps
                # nobody in a POC. Swap for settle(FAILED) if redelivery is wanted.
                try:
                    handler(to_inbound(message))
                except Exception:
                    log.exception("handler failed; message dropped")
                finally:
                    receiver.ack(message)

        receiver.start()
        receiver.receive_async(_Adapter())
        self._receivers.append(receiver)
        log.info("consuming queue %s with subscriptions %s", queue_name, subscriptions)

    def wait(self) -> None:
        """Block until Ctrl-C or `stop()`."""
        try:
            while not self._stop.wait(1.0):
                pass
        except KeyboardInterrupt:
            pass

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        for receiver in self._receivers:
            receiver.terminate()
        self._receivers.clear()
        if self._publisher is not None:
            self._publisher.terminate()
            self._publisher = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
