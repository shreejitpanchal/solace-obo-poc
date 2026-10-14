"""Topic layout for the POC and the single validation gate for topic levels.

    poc/msg/{sender-id}/{thread-id}            raw, published by the publisher
    poc/msg-validate/{sender-id}/{thread-id}   republished by the validator

sender-id and thread-id come from the command line and from inbound topics, so
they pass through `validate_level` before they are ever placed in a topic
string: a `/`, `*` or `>` would otherwise rewrite the subscription structure.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

RAW_PREFIX = "poc/msg"
VALIDATED_PREFIX = "poc/msg-validate"

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
    prefix: str
    sender_id: str
    thread_id: str

    @property
    def is_raw(self) -> bool:
        return self.prefix == RAW_PREFIX

    @property
    def is_validated(self) -> bool:
        return self.prefix == VALIDATED_PREFIX


def raw_topic(sender_id: str, thread_id: str) -> str:
    return (
        f"{RAW_PREFIX}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


def validated_topic(sender_id: str, thread_id: str) -> str:
    return (
        f"{VALIDATED_PREFIX}/{validate_level(sender_id, 'sender-id')}"
        f"/{validate_level(thread_id, 'thread-id')}"
    )


def raw_subscription() -> str:
    """Everything the publisher sends; what the validator listens to."""
    return f"{RAW_PREFIX}/>"


def recipient_subscription(thread_id: str) -> str:
    """Match by thread-id, any sender."""
    return f"{VALIDATED_PREFIX}/*/{validate_level(thread_id, 'thread-id')}"


def profiler_subscription(sender_id: str) -> str:
    """Match by sender-id, any thread."""
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


def parse_topic(topic: str) -> TopicParts:
    """Split an inbound topic into its parts; ValueError if it is not one of ours."""
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
