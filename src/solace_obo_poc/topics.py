"""Topic and queue layout for the POC, plus the single validation gate for topic levels.

Two kinds of event, each with a raw and a validated topic space
----------------------------------------------------------------
    kind "msg"   poc/msg/{sender-id}/{thread-id}             RAW: published by the publisher,
                 poc/msg-validate/{sender-id}/{thread-id}    VALIDATED: republished by the validator
    kind "news"  poc/news/{sender-id}/{thread-id}            RAW
                 poc/news-validate/{sender-id}/{thread-id}   VALIDATED

The validator consumes every raw space and republishes into the matching
validated space. Applications consume only validated spaces.

Putting sender-id and thread-id in the topic is what lets the broker route
by either one with a wildcard subscription:

    poc/msg-validate/*/t-1       every sender, thread t-1   (recipient)
    poc/msg-validate/alice/*     sender alice, every thread (user-profiler)
    poc/news-validate/>          every news event           (each news consumer)

Queues (one per consumer instance)
----------------------------------
    <prefix>.validator                 subscriptions poc/msg/>  and  poc/news/>
    <prefix>.recipient.<thread-id>     subscription  poc/msg-validate/*/<thread-id>
    <prefix>.profiler.<sender-id>      subscription  poc/msg-validate/<sender-id>/*
    <prefix>.news.<app>                subscription  poc/news-validate/>
                                       one queue per app in NEWS_CONSUMERS, so each
                                       of the six applications gets its own copy

Why validate_level() exists
---------------------------
sender-id and thread-id come from the command line and from inbound topics.
They are spliced into topic strings and queue names, so they must never
contain `/` (a new level), `*` or `>` (wildcards) or anything else that could
change what a subscription matches. Every function here runs its inputs
through validate_level() first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

RAW_PREFIX = "poc/msg"
VALIDATED_PREFIX = "poc/msg-validate"
NEWS_RAW_PREFIX = "poc/news"
NEWS_VALIDATED_PREFIX = "poc/news-validate"

# Event kind -> (raw prefix, validated prefix). The publisher picks a kind
# with --kind; the validator maps raw -> validated through this table, so
# adding a kind is one line here plus its consumer.
_PREFIXES: dict[str, tuple[str, str]] = {
    "msg": (RAW_PREFIX, VALIDATED_PREFIX),
    "news": (NEWS_RAW_PREFIX, NEWS_VALIDATED_PREFIX),
}
KINDS: tuple[str, ...] = tuple(_PREFIXES)
_RAW_PREFIXES = frozenset(raw for raw, _ in _PREFIXES.values())
_VALIDATED_PREFIXES = frozenset(validated for _, validated in _PREFIXES.values())
_VALIDATED_FOR_RAW = {raw: validated for raw, validated in _PREFIXES.values()}

# The six applications that consume news. Each binds its own queue
# (<prefix>.news.<app>) carrying the news subscription, so every one of them
# receives every validated news event. Names are also queue-name levels, so
# they are validated like any other identifier (see news_queue).
NEWS_CONSUMERS: tuple[str, ...] = (
    "Kai-Go-Mobile",
    "Kai-Go-Desktop",
    "Kai-Signals-Engine",
    "Kai-StoreFront",
    "Kai-Cowork",
    "Kai-Zen",
)

# Letters, digits, '.', '_' and '-', 1 to 64 characters. Nothing else.
_LEVEL = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def validate_level(value: str, name: str) -> str:
    """Return `value` if it is a safe literal topic level, else raise ValueError naming it."""
    if not isinstance(value, str) or not _LEVEL.match(value):
        raise ValueError(
            f"{name} must be 1-64 characters of letters, digits, '.', '_' or '-'; got {value!r}"
        )
    return value


def validate_kind(kind: str) -> str:
    """Return `kind` if it is a known event kind ("msg" or "news"), else raise ValueError."""
    if kind not in _PREFIXES:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}; got {kind!r}")
    return kind


def validate_news_consumer(app: str) -> str:
    """Return `app` if it is one of the six news consumers, else raise ValueError naming them."""
    if app not in NEWS_CONSUMERS:
        raise ValueError(f"news consumer must be one of {', '.join(NEWS_CONSUMERS)}; got {app!r}")
    return app


@dataclass(frozen=True)
class TopicParts:
    """A parsed topic: which space it is in, and the two identifiers it carries."""

    prefix: str
    sender_id: str
    thread_id: str

    @property
    def is_raw(self) -> bool:
        return self.prefix in _RAW_PREFIXES

    @property
    def is_validated(self) -> bool:
        return self.prefix in _VALIDATED_PREFIXES


# --- topic builders ----------------------------------------------------------


def raw_topic(sender_id: str, thread_id: str, kind: str = "msg") -> str:
    """Where the publisher sends: poc/msg/<sender-id>/<thread-id> (or poc/news/... for news)."""
    prefix, _ = _PREFIXES[validate_kind(kind)]
    return (
        f"{prefix}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


def validated_topic(sender_id: str, thread_id: str, kind: str = "msg") -> str:
    """Where the validator republishes: poc/msg-validate/<sender-id>/<thread-id> (or news)."""
    _, prefix = _PREFIXES[validate_kind(kind)]
    return (
        f"{prefix}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


def validated_topic_for(parts: TopicParts) -> str:
    """The validated topic a raw topic is forwarded to; ValueError if `parts` is not raw.

    Used by the validator so it never has to know which kind it is forwarding.
    """
    try:
        prefix = _VALIDATED_FOR_RAW[parts.prefix]
    except KeyError:
        raise ValueError(f"topic prefix {parts.prefix!r} is not a raw space") from None
    return f"{prefix}/{parts.sender_id}/{parts.thread_id}"


# --- subscriptions -----------------------------------------------------------


def raw_subscription(kind: str = "msg") -> str:
    """Everything the publisher sends for one kind, e.g. poc/msg/>. `>` = all remaining levels."""
    prefix, _ = _PREFIXES[validate_kind(kind)]
    return f"{prefix}/>"


def validator_subscriptions() -> list[str]:
    """Every raw space, in KINDS order: what the validator's queue subscribes to."""
    return [raw_subscription(kind) for kind in KINDS]


def recipient_subscription(thread_id: str) -> str:
    """Match by thread-id, any sender: poc/msg-validate/*/<thread-id>."""
    return f"{VALIDATED_PREFIX}/*/{validate_level(thread_id, 'thread-id')}"


def profiler_subscription(sender_id: str) -> str:
    """Match by sender-id, any thread: poc/msg-validate/<sender-id>/*."""
    return f"{VALIDATED_PREFIX}/{validate_level(sender_id, 'sender-id')}/*"


def news_subscription() -> str:
    """Every validated news event, any sender, any thread: poc/news-validate/>."""
    return f"{NEWS_VALIDATED_PREFIX}/>"


# --- consumer queues ---------------------------------------------------------
# One durable queue per consumer, named after the service and its filter, so
# two recipients on different threads never share (and steal from) one queue.
# The filter values are validated levels, so the names are safe to create.


def validator_queue(prefix: str) -> str:
    return f"{prefix}.validator"


def recipient_queue(prefix: str, thread_id: str) -> str:
    return f"{prefix}.recipient.{validate_level(thread_id, 'thread-id')}"


def profiler_queue(prefix: str, sender_id: str) -> str:
    return f"{prefix}.profiler.{validate_level(sender_id, 'sender-id')}"


def news_queue(prefix: str, app: str) -> str:
    """<prefix>.news.<app>, e.g. q.poc.news.Kai-Zen; `app` must be in NEWS_CONSUMERS."""
    return f"{prefix}.news.{validate_level(validate_news_consumer(app), 'news consumer')}"


# --- the full queue set, for provisioning ------------------------------------


@dataclass(frozen=True)
class QueueSpec:
    """One queue to exist on the broker and the topic subscriptions it carries."""

    name: str
    subscriptions: tuple[str, ...]


def queue_specs(
    prefix: str, thread_id: str | None = None, sender_id: str | None = None
) -> list[QueueSpec]:
    """Every POC queue with its subscriptions: validator, six news queues, and, when
    `thread_id` / `sender_id` are given, the recipient and profiler queues for them.

    This is the single list obo-provision creates on the broker; it is derived
    from the same builders the consumers use, so the two cannot drift.
    """
    specs = [QueueSpec(validator_queue(prefix), tuple(validator_subscriptions()))]
    if thread_id is not None:
        specs.append(
            QueueSpec(recipient_queue(prefix, thread_id), (recipient_subscription(thread_id),))
        )
    if sender_id is not None:
        specs.append(
            QueueSpec(profiler_queue(prefix, sender_id), (profiler_subscription(sender_id),))
        )
    specs.extend(QueueSpec(news_queue(prefix, app), (news_subscription(),)) for app in NEWS_CONSUMERS)
    return specs


# --- parsing inbound topics --------------------------------------------------


def parse_topic(topic: str) -> TopicParts:
    """Split an inbound topic into its parts; ValueError if it is not one of ours.

    Used by the validator and by authz.authorize_event to learn which
    sender-id an event claims, so the token's `sub` can be compared to it.
    """
    for prefix in sorted(_RAW_PREFIXES | _VALIDATED_PREFIXES):
        if topic.startswith(prefix + "/"):
            rest = topic[len(prefix) + 1 :].split("/")
            if len(rest) != 2:
                raise ValueError(
                    f"topic {topic!r} must be {prefix}/{{sender-id}}/{{thread-id}}"
                )
            return TopicParts(
                prefix,
                validate_level(rest[0], "sender-id"),
                validate_level(rest[1], "thread-id"),
            )
    raise ValueError(
        f"topic {topic!r} is outside the POC spaces "
        f"({', '.join(sorted(_RAW_PREFIXES | _VALIDATED_PREFIXES))})"
    )
