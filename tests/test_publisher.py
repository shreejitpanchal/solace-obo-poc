import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError
from solace_obo_poc.publisher import IssuedToken, build_message, fetch_token, main


def test_build_message_puts_token_in_properties_and_data_in_payload(issue):
    token = issue("alice")
    msg = build_message(token, "alice", "t-1", "hello", 3)
    assert msg.topic == "poc/msg/alice/t-1"
    assert msg.properties == {TOKEN_PROPERTY: token}
    body = json.loads(msg.payload)
    assert body["sender_id"] == "alice"
    assert body["thread_id"] == "t-1"
    assert body["seq"] == 3
    assert body["text"] == "hello"
    assert "sent_at" in body
    assert token not in msg.payload  # the token travels only as a property


def test_build_message_rejects_unsafe_ids(issue):
    with pytest.raises(ValueError):
        build_message(issue(), "a/b", "t", "x", 1)


def test_fetch_token_posts_sender_and_actor_and_records_expiry(token_cfg):
    seen = {}

    def app(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 120})

    client = httpx.Client(transport=httpx.MockTransport(app))
    before = datetime.now(UTC)
    issued = fetch_token(token_cfg, "alice", "poc", client=client)
    assert issued.token == "tok"
    assert timedelta(seconds=115) < issued.expires_at - before <= timedelta(seconds=121)
    assert seen == {"path": "/token", "body": {"sender_id": "alice", "actor": "poc"}}


def test_fetch_token_falls_back_to_configured_ttl_when_expires_in_missing(token_cfg):
    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"access_token": "tok"}))
    )
    issued = fetch_token(token_cfg, "alice", "poc", client=client)
    remaining = issued.expires_at - datetime.now(UTC)
    assert timedelta(seconds=295) < remaining <= timedelta(seconds=token_cfg.ttl_seconds)


def test_fetch_token_http_error_is_actionable(token_cfg):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(400)))
    with pytest.raises(TokenError, match="OBO_ISSUER_URL"):
        fetch_token(token_cfg, "alice", "poc", client=client)


def test_fetch_token_empty_response_is_rejected(token_cfg):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    with pytest.raises(TokenError, match="no access_token"):
        fetch_token(token_cfg, "alice", "poc", client=client)


def test_needs_refresh_inside_the_margin():
    now = datetime.now(UTC)
    assert not IssuedToken("t", now + timedelta(seconds=300)).needs_refresh(now)
    assert IssuedToken("t", now + timedelta(seconds=29)).needs_refresh(now)
    assert IssuedToken("t", now - timedelta(seconds=1)).needs_refresh(now)


@pytest.mark.parametrize(
    "argv",
    [["--sender-id", "a/b", "--thread-id", "t"], ["--sender-id", "a", "--thread-id", "*"]],
)
def test_main_rejects_unsafe_ids_before_any_network(argv, monkeypatch):
    def never(*a, **k):
        pytest.fail("fetch_token called")

    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", never)
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2


def test_main_returns_2_when_issuer_unreachable(monkeypatch):
    def boom(*a, **k):
        raise TokenError("issuer down")

    def no_broker(cfg):
        pytest.fail("broker touched")

    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", boom)
    monkeypatch.setattr("solace_obo_poc.publisher.Broker", no_broker)
    assert main(["--sender-id", "alice", "--thread-id", "t-1"]) == 2


def test_main_returns_2_on_bad_publish_interval(monkeypatch):
    monkeypatch.setenv("PUBLISH_INTERVAL_SECONDS", "soon")
    monkeypatch.setattr("solace_obo_poc.publisher.Broker", lambda cfg: pytest.fail("broker"))
    assert main(["--sender-id", "alice", "--thread-id", "t-1"]) == 2


@pytest.fixture
def fake_broker(monkeypatch):
    published = []

    class FakeBroker:
        def __init__(self, cfg):
            pass

        def connect(self):
            return self

        def publish(self, msg):
            published.append(msg)

        def close(self):
            published.append("closed")

    monkeypatch.setattr("solace_obo_poc.publisher.Broker", FakeBroker)
    return published


def fresh(*a, **k):
    return IssuedToken("tok", datetime.now(UTC) + timedelta(seconds=300))


def test_main_publishes_count_messages_then_exits(monkeypatch, fake_broker):
    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", fresh)
    code = main(["--sender-id", "alice", "--thread-id", "t-1", "--count", "2", "--interval", "0"])
    assert code == 0
    assert [m.topic for m in fake_broker[:2]] == ["poc/msg/alice/t-1"] * 2
    assert [json.loads(m.payload)["seq"] for m in fake_broker[:2]] == [1, 2]
    assert fake_broker[-1] == "closed"


def test_env_count_and_interval_are_used_when_flags_absent(monkeypatch, fake_broker):
    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", fresh)
    monkeypatch.setenv("PUBLISH_COUNT", "3")
    monkeypatch.setenv("PUBLISH_INTERVAL_SECONDS", "0")
    assert main(["--sender-id", "alice", "--thread-id", "t-1"]) == 0
    assert len([m for m in fake_broker if m != "closed"]) == 3


def test_main_waits_interval_between_events(monkeypatch, fake_broker):
    waits = []
    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", fresh)
    monkeypatch.setattr("threading.Event.wait", lambda self, t=None: waits.append(t))
    main(["--sender-id", "alice", "--thread-id", "t-1", "--count", "3", "--interval", "5"])
    assert waits == [5.0, 5.0]  # no pause after the last event


def test_token_is_refreshed_before_expiry(monkeypatch, fake_broker):
    calls = []

    def expiring(*a, **k):
        calls.append(1)
        # Always inside the refresh margin, so every event triggers a refresh.
        return IssuedToken(f"tok{len(calls)}", datetime.now(UTC) + timedelta(seconds=10))

    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", expiring)
    main(["--sender-id", "alice", "--thread-id", "t-1", "--count", "3", "--interval", "0"])
    assert len(calls) == 4  # initial + one refresh per event
    tokens = [m.properties[TOKEN_PROPERTY] for m in fake_broker if m != "closed"]
    assert tokens == ["tok2", "tok3", "tok4"]


def test_unlimited_run_stops_cleanly_on_ctrl_c(monkeypatch, fake_broker):
    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", fresh)

    def interrupt(self, t=None):
        raise KeyboardInterrupt

    monkeypatch.setattr("threading.Event.wait", interrupt)
    assert main(["--sender-id", "alice", "--thread-id", "t-1"]) == 0  # count 0 = unlimited
    assert fake_broker == [fake_broker[0], "closed"]  # one event, then stopped and closed
