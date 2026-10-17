"""Each of the six news consumers binds its own queue on the shared news subscription."""

import logging

import pytest

from solace_obo_poc import news_consumer
from solace_obo_poc.authz import authorize_event
from solace_obo_poc.obo_token import TOKEN_PROPERTY
from solace_obo_poc.topics import NEWS_CONSUMERS


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


@pytest.mark.parametrize("app", NEWS_CONSUMERS)
def test_each_app_consumes_its_own_queue_on_the_news_subscription(app, fake_broker):
    assert news_consumer.main(["--app", app]) == 0
    assert fake_broker == [
        ("subscribe", f"q.test.news.{app}", ["poc/news-validate/>"]),
        ("wait",),
        ("close",),
    ]


def test_six_apps_bind_six_distinct_queues(fake_broker):
    for app in NEWS_CONSUMERS:
        news_consumer.main(["--app", app])
    queues = [c[1] for c in fake_broker if c[0] == "subscribe"]
    assert len(queues) == 6 == len(set(queues))


@pytest.mark.parametrize("argv", [["--app", "Kai-Unknown"], ["--app", "a/b"], []])
def test_unknown_app_is_rejected_before_connecting(argv, fake_broker):
    with pytest.raises(SystemExit) as exc:
        news_consumer.main(argv)
    assert exc.value.code == 2
    assert fake_broker == []


def test_on_event_logs_the_news_item_without_the_token(issue, verifier, caplog):
    token = issue("alice")
    ev = authorize_event(
        "poc/news-validate/alice/t-1",
        {TOKEN_PROPERTY: token},
        b'{"seq": 7, "text": "headline"}',
        verifier,
    )
    with caplog.at_level(logging.INFO):
        news_consumer.on_event(ev)
    assert "news-consumer received news seq=7" in caplog.text
    assert "headline" in caplog.text
    assert token not in caplog.text
