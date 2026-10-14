import pytest

from solace_obo_poc.config import ConfigError, broker_config, publisher_config, token_config


def test_broker_defaults_apply_only_when_unset():
    cfg = broker_config({})
    assert cfg.host == "tcp://localhost:55555"
    assert cfg.vpn == "default"
    overridden = broker_config({"SOLACE_HOST": "tcps://broker:55443", "SOLACE_PASSWORD": "s3cret"})
    assert overridden.host == "tcps://broker:55443"
    assert overridden.password == "s3cret"


def test_broker_masked_never_exposes_password():
    masked = broker_config({"SOLACE_PASSWORD": "s3cret"}).masked()
    assert masked["password"] == "***"
    assert "s3cret" not in str(masked)
    assert masked["queue_prefix"] == "q.poc"


def test_queue_settings_default_to_dynamic_creation():
    cfg = broker_config({})
    assert cfg.queue_prefix == "q.poc"
    assert cfg.create_queues is True


@pytest.mark.parametrize(
    "raw, expected",
    [("false", False), ("0", False), ("No", False), ("true", True), ("ON", True), ("", True)],
)
def test_create_queues_parses_booleans(raw, expected):
    assert broker_config({"SOLACE_CREATE_QUEUES": raw}).create_queues is expected


def test_create_queues_rejects_garbage():
    with pytest.raises(ConfigError, match="SOLACE_CREATE_QUEUES"):
        broker_config({"SOLACE_CREATE_QUEUES": "maybe"})


def test_queue_prefix_override():
    assert broker_config({"SOLACE_QUEUE_PREFIX": "q.custom"}).queue_prefix == "q.custom"


def test_token_defaults_and_derived_urls():
    cfg = token_config({})
    assert cfg.issuer_url == "http://127.0.0.1:8000"
    assert cfg.jwks_url == "http://127.0.0.1:8000/.well-known/jwks.json"
    assert cfg.token_url == "http://127.0.0.1:8000/token"
    assert cfg.ttl_seconds == 300


def test_token_config_strips_trailing_slash():
    cfg = token_config({"OBO_ISSUER_URL": "http://issuer:9000/"})
    assert cfg.token_url == "http://issuer:9000/token"


@pytest.mark.parametrize("bad", ["abc", "0", "-5"])
def test_token_ttl_errors_name_the_variable(bad):
    with pytest.raises(ConfigError, match="OBO_TOKEN_TTL_SECONDS"):
        token_config({"OBO_TOKEN_TTL_SECONDS": bad})


def test_publisher_defaults_every_5_seconds_until_stopped():
    cfg = publisher_config({})
    assert cfg.interval_seconds == 5.0
    assert cfg.count == 0


def test_publisher_overrides():
    cfg = publisher_config({"PUBLISH_INTERVAL_SECONDS": "0.5", "PUBLISH_COUNT": "10"})
    assert cfg.interval_seconds == 0.5
    assert cfg.count == 10


@pytest.mark.parametrize("bad", ["soon", "-1"])
def test_publisher_interval_errors_name_the_variable(bad):
    with pytest.raises(ConfigError, match="PUBLISH_INTERVAL_SECONDS"):
        publisher_config({"PUBLISH_INTERVAL_SECONDS": bad})


@pytest.mark.parametrize("bad", ["many", "-3", "1.5"])
def test_publisher_count_errors_name_the_variable(bad):
    with pytest.raises(ConfigError, match="PUBLISH_COUNT"):
        publisher_config({"PUBLISH_COUNT": bad})
