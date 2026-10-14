import json

import httpx
import pytest

from solace_obo_poc.obo_token import TOKEN_PROPERTY, TokenError
from solace_obo_poc.publisher import build_message, fetch_token, main


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


def test_fetch_token_posts_sender_and_actor(token_cfg):
    seen = {}

    def app(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"access_token": "tok"})

    client = httpx.Client(transport=httpx.MockTransport(app))
    assert fetch_token(token_cfg, "alice", "poc", client=client) == "tok"
    assert seen == {"path": "/token", "body": {"sender_id": "alice", "actor": "poc"}}


def test_fetch_token_http_error_is_actionable(token_cfg):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(400)))
    with pytest.raises(TokenError, match="OBO_ISSUER_URL"):
        fetch_token(token_cfg, "alice", "poc", client=client)


def test_fetch_token_empty_response_is_rejected(token_cfg):
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    with pytest.raises(TokenError, match="no access_token"):
        fetch_token(token_cfg, "alice", "poc", client=client)


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


def test_main_publishes_count_messages(monkeypatch):
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

    monkeypatch.setattr("solace_obo_poc.publisher.fetch_token", lambda *a, **k: "tok")
    monkeypatch.setattr("solace_obo_poc.publisher.Broker", FakeBroker)
    assert main(["--sender-id", "alice", "--thread-id", "t-1", "--count", "2"]) == 0
    assert [m.topic for m in published[:2]] == ["poc/msg/alice/t-1"] * 2
    assert published[-1] == "closed"
