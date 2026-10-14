"""Environment-driven settings. A default applies only when its variable is unset."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass


class ConfigError(Exception):
    """A setting is present but unusable; the message names the variable and the fix."""


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    vpn: str
    username: str
    password: str
    queue_prefix: str  # consumer queues are named <prefix>.<service>[.<filter>]
    create_queues: bool  # create missing queues on start, or require pre-provisioned ones

    def masked(self) -> dict[str, str]:
        """Safe to log: the password is never echoed."""
        return {
            "host": self.host,
            "vpn": self.vpn,
            "username": self.username,
            "password": "***",
            "queue_prefix": self.queue_prefix,
            "create_queues": str(self.create_queues),
        }


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    value = raw.strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise ConfigError(f"{name} must be true or false, got {raw!r}")


@dataclass(frozen=True)
class TokenConfig:
    issuer_url: str  # base URL of the issuer service
    issuer: str  # expected `iss` claim
    audience: str  # expected `aud` claim
    ttl_seconds: int

    @property
    def jwks_url(self) -> str:
        return self.issuer_url.rstrip("/") + "/.well-known/jwks.json"

    @property
    def token_url(self) -> str:
        return self.issuer_url.rstrip("/") + "/token"


def broker_config(env: Mapping[str, str] | None = None) -> BrokerConfig:
    """Defaults match the solace/solace-pubsub-standard container in docker-compose.yml."""
    env = os.environ if env is None else env
    return BrokerConfig(
        host=env.get("SOLACE_HOST", "tcp://localhost:55555"),
        vpn=env.get("SOLACE_VPN", "default"),
        username=env.get("SOLACE_USERNAME", "default"),
        password=env.get("SOLACE_PASSWORD", "default"),
        queue_prefix=env.get("SOLACE_QUEUE_PREFIX", "q.poc"),
        create_queues=_env_bool(env, "SOLACE_CREATE_QUEUES", True),
    )


def token_config(env: Mapping[str, str] | None = None) -> TokenConfig:
    env = os.environ if env is None else env
    raw_ttl = env.get("OBO_TOKEN_TTL_SECONDS", "300")
    try:
        ttl = int(raw_ttl)
    except ValueError as exc:
        raise ConfigError(
            f"OBO_TOKEN_TTL_SECONDS must be an integer number of seconds, got {raw_ttl!r}; "
            "unset it for the default of 300"
        ) from exc
    if ttl <= 0:
        raise ConfigError(f"OBO_TOKEN_TTL_SECONDS must be positive, got {ttl}")
    return TokenConfig(
        issuer_url=env.get("OBO_ISSUER_URL", "http://127.0.0.1:8000"),
        issuer=env.get("OBO_ISSUER", "solace-obo-poc-issuer"),
        audience=env.get("OBO_AUDIENCE", "poc"),
        ttl_seconds=ttl,
    )


@dataclass(frozen=True)
class PublisherConfig:
    interval_seconds: float  # pause between events; 0 publishes as fast as the broker acks
    count: int  # events to publish before exiting; 0 means until stopped


def publisher_config(env: Mapping[str, str] | None = None) -> PublisherConfig:
    """PUBLISH_INTERVAL_SECONDS (default 5) and PUBLISH_COUNT (default 0 = run until Ctrl-C)."""
    env = os.environ if env is None else env
    raw_interval = env.get("PUBLISH_INTERVAL_SECONDS", "5")
    try:
        interval = float(raw_interval)
    except ValueError as exc:
        raise ConfigError(
            f"PUBLISH_INTERVAL_SECONDS must be a number of seconds, got {raw_interval!r}; "
            "unset it for the default of 5"
        ) from exc
    if interval < 0:
        raise ConfigError(f"PUBLISH_INTERVAL_SECONDS must not be negative, got {interval}")
    raw_count = env.get("PUBLISH_COUNT", "0")
    try:
        count = int(raw_count)
    except ValueError as exc:
        raise ConfigError(
            f"PUBLISH_COUNT must be an integer (0 = until stopped), got {raw_count!r}"
        ) from exc
    if count < 0:
        raise ConfigError(f"PUBLISH_COUNT must not be negative, got {count}")
    return PublisherConfig(interval_seconds=interval, count=count)


def configure_logging(level: int | None = None, env: Mapping[str, str] | None = None) -> None:
    """Root logger to stderr. LOG_LEVEL=DEBUG shows every step (token checks, broker I/O).

    INFO (the default) logs each activity once: token obtained, message
    received, token verified, event forwarded or delivered, event refused.
    DEBUG adds the detail behind each of those lines.
    """
    env = os.environ if env is None else env
    if level is None:
        name = env.get("LOG_LEVEL", "INFO").strip().upper()
        if name not in logging.getLevelNamesMapping():
            raise ConfigError(
                f"LOG_LEVEL must be one of DEBUG, INFO, WARNING, ERROR; got {name!r}"
            )
        level = logging.getLevelNamesMapping()[name]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # The Solace SDK logs a lot at INFO; keep it to warnings unless DEBUG is asked for.
    sdk_level = logging.DEBUG if level <= logging.DEBUG else logging.WARNING
    logging.getLogger("solace").setLevel(sdk_level)
