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


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
