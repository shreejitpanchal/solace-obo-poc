"""SDK-free unit tests plus an opt-in round trip against a live broker.

Run the integration test with a broker from `scripts/dev.sh up`:
    SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py
"""

import os
import threading

import pytest

from solace_obo_poc.broker import Broker, Inbound, Outbound, to_inbound
from solace_obo_poc.config import broker_config


class FakeSdkMessage:
    def __init__(self, topic, payload, props):
        self._topic, self._payload, self._props = topic, payload, props

    def get_destination_name(self):
        return self._topic

    def get_payload_as_bytes(self):
        return self._payload

    def get_properties(self):
        return self._props


def test_to_inbound_stringifies_properties():
    msg = FakeSdkMessage("poc/msg/a/t", b"{}", {"obo-token": "tok", "n": 1})
    assert to_inbound(msg) == Inbound("poc/msg/a/t", b"{}", {"obo-token": "tok", "n": "1"})


def test_to_inbound_tolerates_empty_payload_and_properties():
    assert to_inbound(FakeSdkMessage("t", None, None)) == Inbound("t", b"", {})


def test_broker_close_is_safe_before_connect():
    Broker(broker_config({})).close()


def test_stop_unblocks_wait():
    broker = Broker(broker_config({}))
    threading.Timer(0.1, broker.stop).start()
    broker.wait()  # returns instead of hanging


@pytest.mark.skipif(not os.environ.get("SOLACE_INTEGRATION"), reason="needs a live broker")
def test_round_trip_through_a_dynamically_created_queue():
    """Publish persistently, consume from a queue created on start; the queue survives."""
    received = []
    got = threading.Event()
    cfg = broker_config()
    queue = f"{cfg.queue_prefix}.itest"
    broker = Broker(cfg).connect()
    try:
        broker.subscribe(queue, ["poc/msg/itest/*"], lambda m: (received.append(m), got.set()))
        broker.publish(Outbound("poc/msg/itest/t-1", b'{"x":1}', {"obo-token": "tok"}))
        assert got.wait(10), f"no message received from {queue} within 10s"
    finally:
        broker.close()
    assert received[0] == Inbound("poc/msg/itest/t-1", b'{"x":1}', {"obo-token": "tok"})


@pytest.mark.skipif(not os.environ.get("SOLACE_INTEGRATION"), reason="needs a live broker")
def test_queue_spools_while_consumer_is_down():
    """The point of queues: an event published before the consumer binds is still delivered."""
    cfg = broker_config()
    queue = f"{cfg.queue_prefix}.itest-spool"
    # First bind creates the queue and its subscription, then goes away.
    setup = Broker(cfg).connect()
    try:
        setup.subscribe(queue, ["poc/msg/itest-spool/*"], lambda m: None)
    finally:
        setup.close()
    publisher = Broker(cfg).connect()
    try:
        publisher.publish(Outbound("poc/msg/itest-spool/t-1", b"spooled", {}))
    finally:
        publisher.close()
    received = []
    got = threading.Event()
    consumer = Broker(cfg).connect()
    try:
        consumer.subscribe(
            queue, ["poc/msg/itest-spool/*"], lambda m: (received.append(m), got.set())
        )
        assert got.wait(10), "spooled message was not delivered on rebind"
    finally:
        consumer.close()
    assert received[0].payload == b"spooled"
