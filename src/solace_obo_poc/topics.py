"""Topic and queue layout for the POC, plus the single validation gate for topic levels.

Topics (two spaces, same shape)
-------------------------------
    poc/msg/{sender-id}/{thread-id}            RAW: published by the publisher,
                                               consumed only by the validator
    poc/msg-validate/{sender-id}/{thread-id}   VALIDATED: published by the validator,
                                               consumed by the applications

Putting sender-id and thread-id in the topic is what lets the broker route
by either one with a wildcard subscription:

    poc/msg-validate/*/t-1       every sender, thread t-1   (recipient)
    poc/msg-validate/alice/*     sender alice, every thread (user-profiler)

Queues (one per consumer instance)
----------------------------------
    <prefix>.validator                 subscription poc/msg/>
    <prefix>.recipient.<thread-id>     subscription poc/msg-validate/*/<thread-id>
    <prefix>.profiler.<sender-id>      subscription poc/msg-validate/<sender-id>/*

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

# Letters, digits, '.', '_' and '-', 1 to 64 characters. Nothing else.
_LEVEL = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def validate_level(value: str, name: str) -> str:
    """Return `value` if it is a safe literal topic level, else raise ValueError naming it."""
    if not isinstance(value, str) or not _LEVEL.match(value):
        raise ValueError(
            f"{name} must be 1-64 characters of letters, digits, '.', '_' or '-'; got {value!r}"
        )
    return value


@dataclass(frozen=True)
class TopicParts:
    """A parsed topic: which space it is in, and the two identifiers it carries."""

    prefix: str
    sender_id: str
    thread_id: str

    @property
    def is_raw(self) -> bool:
        return self.prefix == RAW_PREFIX

    @property
    def is_validated(self) -> bool:
        return self.prefix == VALIDATED_PREFIX


# --- topic builders ----------------------------------------------------------


def raw_topic(sender_id: str, thread_id: str) -> str:
    """Where the publisher sends: poc/msg/<sender-id>/<thread-id>."""
    return (
        f"{RAW_PREFIX}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


def validated_topic(sender_id: str, thread_id: str) -> str:
    """Where the validator republishes: poc/msg-validate/<sender-id>/<thread-id>."""
    return (
        f"{VALIDATED_PREFIX}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


# --- subscriptions -----------------------------------------------------------


def raw_subscription() -> str:
    """Everything the publisher sends; what the validator listens to. `>` = all remaining levels."""
    return f"{RAW_PREFIX}/>"


def recipient_subscription(thread_id: str) -> str:
    """Match by thread-id, any sender: poc/msg-validate/*/<thread-id>."""
    return f"{VALIDATED_PREFIX}/*/{validate_level(thread_id, 'thread-id')}"


def profiler_subscription(sender_id: str) -> str:
    """Match by sender-id, any thread: poc/msg-validate/<sender-id>/*."""
    return f"{VALIDATED_PREFIX}/{validate_level(sender_id, 'sender-id')}/*"


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


# --- parsing inbound topics --------------------------------------------------


def parse_topic(topic: str) -> TopicParts:
    """Split an inbound topic into its parts; ValueError if it is not one of ours.

    Used by the validator and by authz.authorize_event to learn which
    sender-id an event claims, so the token's `sub` can be compared to it.
    """
    for prefix in (VALIDATED_PREFIX, RAW_PREFIX):
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
    raise ValueError(f"topic {topic!r} is outside {RAW_PREFIX}/ and {VALIDATED_PREFIX}/")
