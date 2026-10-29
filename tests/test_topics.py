import pytest

from solace_obo_poc import topics


def test_raw_and_validated_topics():
    assert topics.raw_topic("alice", "t-1") == "poc/msg/alice/t-1"
    assert topics.validated_topic("alice", "t-1") == "poc/msg-validate/alice/t-1"


def test_news_topics_live_in_their_own_spaces():
    assert topics.KINDS == ("msg", "news")
    assert topics.raw_topic("alice", "t-1", "news") == "poc/news/alice/t-1"
    assert topics.validated_topic("alice", "t-1", "news") == "poc/news-validate/alice/t-1"


@pytest.mark.parametrize("bad_kind", ["alerts", "", "msg/", None])
def test_topic_builders_reject_unknown_kinds(bad_kind):
    with pytest.raises(ValueError, match="kind"):
        topics.raw_topic("alice", "t-1", bad_kind)
    with pytest.raises(ValueError, match="kind"):
        topics.validated_topic("alice", "t-1", bad_kind)
    with pytest.raises(ValueError, match="kind"):
        topics.raw_subscription(bad_kind)


def test_validated_topic_for_maps_each_raw_space_to_its_own_validated_space():
    msg = topics.parse_topic("poc/msg/alice/t-1")
    news = topics.parse_topic("poc/news/alice/t-1")
    assert topics.validated_topic_for(msg) == "poc/msg-validate/alice/t-1"
    assert topics.validated_topic_for(news) == "poc/news-validate/alice/t-1"


def test_validated_topic_for_refuses_already_validated_topics():
    with pytest.raises(ValueError, match="not a raw space"):
        topics.validated_topic_for(topics.parse_topic("poc/news-validate/alice/t-1"))


def test_subscriptions_use_wildcards_only_where_the_diagram_does():
    assert topics.raw_subscription() == "poc/msg/>"
    assert topics.raw_subscription("news") == "poc/news/>"
    assert topics.validator_subscriptions() == ["poc/msg/>", "poc/news/>"]
    assert topics.recipient_subscription("t-1") == "poc/msg-validate/*/t-1"
    assert topics.profiler_subscription("alice") == "poc/msg-validate/alice/*"
    assert topics.news_subscription() == "poc/news-validate/>"


@pytest.mark.parametrize("bad", ["", "a/b", "*", ">", "x" * 65, "sp ace", "tab\t", None, 42])
def test_validate_level_rejects_unsafe_values(bad):
    with pytest.raises(ValueError, match="sender-id"):
        topics.validate_level(bad, "sender-id")


@pytest.mark.parametrize("bad", ["a/b", "*", ">"])
def test_topic_builders_never_emit_unvalidated_levels(bad):
    with pytest.raises(ValueError):
        topics.raw_topic(bad, "t")
    with pytest.raises(ValueError):
        topics.recipient_subscription(bad)
    with pytest.raises(ValueError):
        topics.profiler_subscription(bad)


def test_queue_names_carry_service_and_filter():
    assert topics.validator_queue("q.poc") == "q.poc.validator"
    assert topics.recipient_queue("q.poc", "t-1") == "q.poc.recipient.t-1"
    assert topics.profiler_queue("q.poc", "alice") == "q.poc.profiler.alice"
    assert topics.news_queue("q.poc", "Kai-Zen") == "q.poc.news.Kai-Zen"


def test_six_news_consumers_each_get_their_own_queue():
    assert topics.NEWS_CONSUMERS == (
        "Kai-Go-Mobile",
        "Kai-Go-Desktop",
        "Kai-Signals-Engine",
        "Kai-StoreFront",
        "Kai-Cowork",
        "Kai-Zen",
    )
    queues = {topics.news_queue("q.poc", app) for app in topics.NEWS_CONSUMERS}
    assert len(queues) == 6
    assert all(q.startswith("q.poc.news.") for q in queues)
    # Every name is also a safe queue-name level on its own.
    for app in topics.NEWS_CONSUMERS:
        assert topics.validate_level(app, "app") == app


def test_queue_specs_list_every_queue_with_its_subscriptions():
    specs = {s.name: s.subscriptions for s in topics.queue_specs("q.poc", "t-1", "alice")}
    assert len(specs) == 9
    assert specs["q.poc.validator"] == ("poc/msg/>", "poc/news/>")
    assert specs["q.poc.recipient.t-1"] == ("poc/msg-validate/*/t-1",)
    assert specs["q.poc.profiler.alice"] == ("poc/msg-validate/alice/*",)
    for app in topics.NEWS_CONSUMERS:
        assert specs[f"q.poc.news.{app}"] == ("poc/news-validate/>",)


def test_queue_specs_without_ids_cover_only_the_fixed_queues():
    names = [s.name for s in topics.queue_specs("q.poc")]
    assert names == ["q.poc.validator"] + [f"q.poc.news.{a}" for a in topics.NEWS_CONSUMERS]


def test_queue_specs_reject_unsafe_ids():
    with pytest.raises(ValueError):
        topics.queue_specs("q.poc", thread_id="a/b")
    with pytest.raises(ValueError):
        topics.queue_specs("q.poc", sender_id=">")


@pytest.mark.parametrize("bad", ["Kai-Unknown", "kai-zen", "", "a/b", ">"])
def test_news_queue_rejects_apps_outside_the_six(bad):
    with pytest.raises(ValueError, match="news consumer"):
        topics.news_queue("q.poc", bad)


@pytest.mark.parametrize("bad", ["a/b", "*", ">", ""])
def test_queue_names_reject_unsafe_filters(bad):
    with pytest.raises(ValueError):
        topics.recipient_queue("q", bad)
    with pytest.raises(ValueError):
        topics.profiler_queue("q", bad)


def test_parse_topic_round_trips():
    raw = topics.parse_topic("poc/msg/alice/t-1")
    assert raw == topics.TopicParts(topics.RAW_PREFIX, "alice", "t-1")
    assert raw.is_raw and not raw.is_validated
    validated = topics.parse_topic("poc/msg-validate/alice/t-1")
    assert validated.is_validated and not validated.is_raw


def test_parse_topic_recognises_the_news_spaces():
    raw = topics.parse_topic("poc/news/alice/t-1")
    assert raw == topics.TopicParts(topics.NEWS_RAW_PREFIX, "alice", "t-1")
    assert raw.is_raw and not raw.is_validated
    validated = topics.parse_topic("poc/news-validate/alice/t-1")
    assert validated.prefix == topics.NEWS_VALIDATED_PREFIX
    assert validated.is_validated and not validated.is_raw


@pytest.mark.parametrize(
    "topic",
    [
        "other/topic",
        "poc/msg/alice",
        "poc/msg/alice/t/extra",
        "poc/msg/a*b/t",
        "poc/news/alice",
        "poc/news-validate/alice/t/extra",
        "poc/newsletter/alice/t-1",
    ],
)
def test_parse_topic_rejects_foreign_or_malformed(topic):
    with pytest.raises(ValueError):
        topics.parse_topic(topic)
