import logging

import pytest

from solace_obo_poc.broker import Inbound
from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError
from solace_obo_poc.validator import VALIDATED_AT_PROPERTY, main, make_handler, validate_message

RAW = "poc/msg/alice/t-1"


def test_valid_event_is_republished_on_validated_topic_with_token(issue, verifier):
    token = issue("alice")
    out = validate_message(Inbound(RAW, b'{"x":1}', {TOKEN_PROPERTY: token}), verifier)
    assert out.topic == "poc/msg-validate/alice/t-1"
    assert out.payload == b'{"x":1}'
    assert out.properties[TOKEN_PROPERTY] == token
    assert VALIDATED_AT_PROPERTY in out.properties


def test_missing_token_is_rejected(verifier):
    with pytest.raises(TokenError, match="obo-token"):
        validate_message(Inbound(RAW, b"{}", {}), verifier)


def test_token_for_other_sender_is_rejected(issue, verifier):
    with pytest.raises(TokenError, match="does not match sender-id"):
        validate_message(Inbound(RAW, b"{}", {TOKEN_PROPERTY: issue("mallory")}), verifier)


def test_already_validated_topic_is_not_revalidated(issue, verifier):
    """Guards against a loop if the subscription is ever widened."""
    with pytest.raises(ValueError, match="raw topics"):
        validate_message(
            Inbound("poc/msg-validate/alice/t-1", b"{}", {TOKEN_PROPERTY: issue("alice")}),
            verifier,
        )


def test_handler_publishes_good_and_drops_bad(issue, verifier, caplog):
    published = []
    handle = make_handler(published.append, verifier)
    with caplog.at_level(logging.WARNING):
        handle(Inbound(RAW, b"{}", {TOKEN_PROPERTY: issue("alice")}))
        handle(Inbound(RAW, b"{}", {TOKEN_PROPERTY: issue("alice", audience="x")}))
        handle(Inbound("other/topic", b"{}", {TOKEN_PROPERTY: issue("alice")}))
    assert len(published) == 1
    assert sum("dropped" in r.message for r in caplog.records) == 2


def test_handler_log_never_contains_the_token(issue, verifier, caplog):
    token = issue("alice", audience="x")
    handle = make_handler(lambda m: None, verifier)
    with caplog.at_level(logging.WARNING):
        handle(Inbound(RAW, b"{}", {TOKEN_PROPERTY: token}))
    assert token not in caplog.text
    assert token[:8] in caplog.text


def test_main_returns_2_when_jwks_unreachable(monkeypatch):
    def boom(*a, **k):
        raise TokenError("issuer down")

    def no_broker(cfg):
        pytest.fail("broker touched")

    monkeypatch.setattr("solace_obo_poc.validator.JwksVerifier.from_url", boom)
    monkeypatch.setattr("solace_obo_poc.validator.Broker", no_broker)
    assert main([]) == 2


def test_main_subscribes_to_raw_topics_and_closes(monkeypatch, verifier):
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

    monkeypatch.setattr("solace_obo_poc.validator.JwksVerifier.from_url", lambda *a, **k: verifier)
    monkeypatch.setattr("solace_obo_poc.validator.Broker", FakeBroker)
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    assert main([]) == 0
    assert calls == [("subscribe", "q.test.validator", ["poc/msg/>"]), ("wait",), ("close",)]
