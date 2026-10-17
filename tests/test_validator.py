"""The validator forwards raw events to the validated topic without inspecting the token."""

import logging

import pytest

from solace_obo_poc.broker import Inbound
from solace_obo_poc.obo_token import TOKEN_PROPERTY
from solace_obo_poc.validator import FORWARDED_AT_PROPERTY, forward_message, main, make_handler

RAW = "poc/msg/alice/t-1"


def test_event_is_forwarded_to_validated_topic_with_properties_intact(issue):
    token = issue("alice")
    out = forward_message(Inbound(RAW, b'{"x":1}', {TOKEN_PROPERTY: token, "other": "kept"}))
    assert out.topic == "poc/msg-validate/alice/t-1"
    assert out.payload == b'{"x":1}'
    assert out.properties[TOKEN_PROPERTY] == token
    assert out.properties["other"] == "kept"
    assert FORWARDED_AT_PROPERTY in out.properties


def test_forwarder_does_not_inspect_the_token():
    """A missing, garbage, or mismatched token is forwarded; consumers decide."""
    assert forward_message(Inbound(RAW, b"{}", {})).topic == "poc/msg-validate/alice/t-1"
    assert forward_message(Inbound(RAW, b"{}", {TOKEN_PROPERTY: "not.a.jwt"})).properties[
        TOKEN_PROPERTY
    ] == "not.a.jwt"


def test_forwarder_does_not_inspect_the_payload():
    out = forward_message(Inbound(RAW, b"not json at all", {}))
    assert out.payload == b"not json at all"


def test_news_event_is_forwarded_to_the_news_validated_topic(issue):
    token = issue("alice")
    out = forward_message(Inbound("poc/news/alice/t-1", b'{"h":1}', {TOKEN_PROPERTY: token}))
    assert out.topic == "poc/news-validate/alice/t-1"
    assert out.payload == b'{"h":1}'
    assert out.properties[TOKEN_PROPERTY] == token


@pytest.mark.parametrize("topic", ["poc/msg-validate/alice/t-1", "poc/news-validate/alice/t-1"])
def test_already_validated_topic_is_not_forwarded_again(topic):
    """Guards against a loop if the subscription is ever widened."""
    with pytest.raises(ValueError, match="raw topics"):
        forward_message(Inbound(topic, b"{}", {}))


@pytest.mark.parametrize("topic", ["other/topic", "poc/msg/alice", "poc/msg/a*b/t-1"])
def test_foreign_or_malformed_topics_are_refused(topic):
    with pytest.raises(ValueError):
        forward_message(Inbound(topic, b"{}", {}))


def test_handler_forwards_good_and_drops_unforwardable(issue, caplog):
    published = []
    handle = make_handler(published.append)
    with caplog.at_level(logging.INFO):
        handle(Inbound(RAW, b"{}", {TOKEN_PROPERTY: issue("alice")}))
        handle(Inbound(RAW, b"{}", {}))  # no token: still forwarded
        handle(Inbound("other/topic", b"{}", {TOKEN_PROPERTY: issue("alice")}))
    assert [m.topic for m in published] == ["poc/msg-validate/alice/t-1"] * 2
    assert sum("dropped" in r.message for r in caplog.records) == 1


def test_handler_log_never_contains_the_token(issue, caplog):
    token = issue("alice")
    handle = make_handler(lambda m: None)
    with caplog.at_level(logging.INFO):
        handle(Inbound(RAW, b"{}", {TOKEN_PROPERTY: token}))
    assert token not in caplog.text
    assert token[:8] in caplog.text


def test_main_subscribes_to_every_raw_space_and_closes(monkeypatch):
    calls = []

    class FakeBroker:
        def __init__(self, cfg):
            pass

        def connect(self):
            return self

        def publish(self, msg):
            calls.append(("publish", msg))

        def subscribe(self, queue, subs, handler):
            calls.append(("subscribe", queue, subs))

        def wait(self):
            calls.append(("wait",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr("solace_obo_poc.validator.Broker", FakeBroker)
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    assert main([]) == 0
    assert calls == [
        ("subscribe", "q.test.validator", ["poc/msg/>", "poc/news/>"]),
        ("wait",),
        ("close",),
    ]
