import logging

import pytest

from solace_obo_poc import consumer, profiler, recipient
from solace_obo_poc.broker import Inbound
from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError

VALIDATED = "poc/msg-validate/alice/t-1"


def test_handler_delivers_authorized_events_only(issue, verifier, caplog):
    seen = []
    handle = consumer.make_handler("svc", verifier, seen.append)
    with caplog.at_level(logging.WARNING):
        handle(Inbound(VALIDATED, b'{"text":"ok"}', {TOKEN_PROPERTY: issue("alice")}))
        handle(Inbound(VALIDATED, b'{"text":"no"}', {}))  # missing token
        handle(Inbound(VALIDATED, b'{"text":"no"}', {TOKEN_PROPERTY: issue("mallory")}))
        handle(Inbound(VALIDATED, b"not json", {TOKEN_PROPERTY: issue("alice")}))
    assert [e.payload for e in seen] == [{"text": "ok"}]
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 3


def test_describe_is_log_safe(issue, verifier):
    seen = []
    consumer.make_handler("svc", verifier, seen.append)(
        Inbound(VALIDATED, b'{"text":"ok"}', {TOKEN_PROPERTY: issue("alice")})
    )
    text = consumer.describe(seen[0])
    assert "sender=alice" in text and "thread=t-1" in text and "sub=alice" in text


def test_run_returns_2_when_jwks_unreachable(monkeypatch):
    def boom(*a, **k):
        raise TokenError("issuer down")

    def no_broker(cfg):
        pytest.fail("broker touched")

    monkeypatch.setattr("solace_obo_poc.consumer.JwksVerifier.from_url", boom)
    monkeypatch.setattr("solace_obo_poc.consumer.Broker", no_broker)
    code = consumer.run("svc", lambda p: f"{p}.svc", "poc/msg-validate/*/t-1", lambda e: None)
    assert code == 2


@pytest.fixture
def fake_broker(monkeypatch, verifier):
    calls = []

    class FakeBroker:
        def __init__(self, cfg):
            pass

        def connect(self):
            return self

        def subscribe(self, queue, subs, handler):
            calls.append(("subscribe", queue, subs))

        def wait(self):
            calls.append(("wait",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr("solace_obo_poc.consumer.JwksVerifier.from_url", lambda *a, **k: verifier)
    monkeypatch.setattr("solace_obo_poc.consumer.Broker", FakeBroker)
    monkeypatch.setenv("SOLACE_QUEUE_PREFIX", "q.test")
    return calls


def test_recipient_consumes_its_own_queue_filtered_by_thread_id(fake_broker):
    assert recipient.main(["--thread-id", "t-1"]) == 0
    assert fake_broker == [
        ("subscribe", "q.test.recipient.t-1", ["poc/msg-validate/*/t-1"]),
        ("wait",),
        ("close",),
    ]


def test_profiler_consumes_its_own_queue_filtered_by_sender_id(fake_broker):
    assert profiler.main(["--sender-id", "alice"]) == 0
    assert fake_broker == [
        ("subscribe", "q.test.profiler.alice", ["poc/msg-validate/alice/*"]),
        ("wait",),
        ("close",),
    ]


@pytest.mark.parametrize(
    "entry, argv",
    [(recipient.main, ["--thread-id", "a/b"]), (profiler.main, ["--sender-id", ">"])],
)
def test_entry_points_reject_unsafe_ids_before_connecting(entry, argv, fake_broker):
    with pytest.raises(SystemExit) as exc:
        entry(argv)
    assert exc.value.code == 2
    assert fake_broker == []


def test_on_event_callbacks_log(issue, verifier, caplog):
    from solace_obo_poc.authz import authorize_event

    ev = authorize_event(VALIDATED, {TOKEN_PROPERTY: issue("alice")}, b"{}", verifier)
    with caplog.at_level(logging.INFO):
        recipient.on_event(ev)
        profiler.on_event(ev)
    assert "recipient received" in caplog.text
    assert "user-profiler profiling" in caplog.text
