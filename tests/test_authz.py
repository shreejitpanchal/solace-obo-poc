import json

import pytest

from solace_obo_poc.authz import AuthorizationError, PayloadError, authorize_event
from solace_obo_poc.obo_token import TOKEN_PROPERTY

TOPIC = "poc/msg-validate/alice/t-1"
PAYLOAD = json.dumps({"text": "hi"}).encode()


class SpyDecoder:
    """Records whether the payload was ever touched."""

    def __init__(self):
        self.calls = 0

    def __call__(self, raw):
        self.calls += 1
        return json.loads(raw)


def test_valid_token_matching_sender_reads_payload(issue, verifier):
    decode = SpyDecoder()
    props = {TOKEN_PROPERTY: issue("alice")}
    event = authorize_event(TOPIC, props, PAYLOAD, verifier, decode=decode)
    assert event.payload == {"text": "hi"}
    assert event.topic.thread_id == "t-1"
    assert event.claims.subject == "alice"
    assert decode.calls == 1


def test_missing_token_never_touches_payload(verifier):
    decode = SpyDecoder()
    with pytest.raises(AuthorizationError, match="obo-token"):
        authorize_event(TOPIC, {}, PAYLOAD, verifier, decode=decode)
    assert decode.calls == 0


def test_invalid_token_never_touches_payload(issue, verifier):
    decode = SpyDecoder()
    props = {TOKEN_PROPERTY: issue("alice", audience="x")}
    with pytest.raises(AuthorizationError, match="rejected"):
        authorize_event(TOPIC, props, PAYLOAD, verifier, decode=decode)
    assert decode.calls == 0


def test_token_for_another_sender_is_refused(issue, verifier):
    decode = SpyDecoder()
    with pytest.raises(AuthorizationError, match="does not match sender-id"):
        authorize_event(TOPIC, {TOKEN_PROPERTY: issue("mallory")}, PAYLOAD, verifier, decode=decode)
    assert decode.calls == 0


def test_foreign_topic_is_refused_before_token_check(issue, verifier):
    decode = SpyDecoder()
    props = {TOKEN_PROPERTY: issue("alice")}
    with pytest.raises(AuthorizationError, match="outside"):
        authorize_event("other/topic", props, PAYLOAD, verifier, decode=decode)
    assert decode.calls == 0


def test_authorized_but_malformed_payload_is_a_payload_error(issue, verifier):
    with pytest.raises(PayloadError, match="not JSON"):
        authorize_event(TOPIC, {TOKEN_PROPERTY: issue("alice")}, b"not json", verifier)
