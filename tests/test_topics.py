import pytest

from solace_obo_poc import topics


def test_raw_and_validated_topics():
    assert topics.raw_topic("alice", "t-1") == "poc/msg/alice/t-1"
    assert topics.validated_topic("alice", "t-1") == "poc/msg-validate/alice/t-1"


def test_subscriptions_use_wildcards_only_where_the_diagram_does():
    assert topics.raw_subscription() == "poc/msg/>"
    assert topics.recipient_subscription("t-1") == "poc/msg-validate/*/t-1"
    assert topics.profiler_subscription("alice") == "poc/msg-validate/alice/*"


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


@pytest.mark.parametrize(
    "topic",
    ["other/topic", "poc/msg/alice", "poc/msg/alice/t/extra", "poc/msg/a*b/t"],
)
def test_parse_topic_rejects_foreign_or_malformed(topic):
    with pytest.raises(ValueError):
        topics.parse_topic(topic)
