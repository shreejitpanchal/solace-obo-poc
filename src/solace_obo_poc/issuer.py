"""Mock OBO issuer: mints RS256 tokens and serves the JWKS the other hops verify with.

Where it sits in the flow
-------------------------
    publisher --POST /token--> [issuer]            (step 1: get a token)
    validator / recipient / profiler --GET /.well-known/jwks.json--> [issuer]
                                                   (at start-up: get public keys)

It stands in for the customer's real issuer. Nothing else in the flow changes
as long as the real issuer produces a JWT with the same claims (see
obo_token.py) and exposes a JWKS endpoint.

Endpoints
---------
    POST /token                 {"sender_id": "alice", "actor": "poc-publisher"}
                                -> {"access_token": "<JWT>", "token_type": "Bearer",
                                    "expires_in": 300}
    GET  /.well-known/jwks.json the public key(s), standard JWKS shape
    GET  /healthz               liveness

The signing key is generated per process unless --key-file points at a PEM,
so tokens from a previous run stop verifying after a restart (they fail with
"unknown kid"); that is deliberate for a POC and useful for demonstrating a
rejection.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from solace_obo_poc.config import TokenConfig, configure_logging, token_config
from solace_obo_poc.obo_token import KeyPair, TokenError
from solace_obo_poc.topics import validate_level

log = logging.getLogger(__name__)


class TokenRequest(BaseModel):
    sender_id: str  # the user the token is issued on behalf of -> `sub`
    actor: str = "poc-publisher"  # the service that will present it -> `act.sub`


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int


def create_app(keypair: KeyPair, cfg: TokenConfig) -> FastAPI:
    """Build the FastAPI app around one signing key and the configured iss/aud/ttl."""
    app = FastAPI(title="OBO issuer (POC)")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/.well-known/jwks.json")
    def jwks() -> dict:
        # Public half only. Verifiers fetch this once at start-up.
        return keypair.jwks()

    @app.post("/token", response_model=TokenResponse)
    def token(req: TokenRequest) -> TokenResponse:
        # sender_id ends up in a topic level downstream and actor in a claim,
        # so both go through the same validation gate as everywhere else.
        try:
            validate_level(req.sender_id, "sender_id")
            validate_level(req.actor, "actor")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        # A real issuer would authenticate the caller and check that `actor`
        # is allowed to act for `sender_id` here. The mock trusts the request.
        access_token = keypair.issue(
            issuer=cfg.issuer,
            audience=cfg.audience,
            subject=req.sender_id,
            actor=req.actor,
            ttl_seconds=cfg.ttl_seconds,
        )
        log.info("issued OBO token for sub=%s act=%s", req.sender_id, req.actor)
        return TokenResponse(access_token=access_token, expires_in=cfg.ttl_seconds)

    return app


def load_keypair(key_file: Path | None) -> KeyPair:
    """Per-process key by default; a PEM file when tokens must survive restarts."""
    if key_file is None:
        return KeyPair.generate()
    try:
        return KeyPair.from_pem(key_file.read_bytes(), kid=key_file.stem)
    except OSError as exc:
        raise TokenError(f"cannot read --key-file {key_file}: {exc}") from exc


def main(argv: list[str] | None = None) -> int:
    """Process entry point (`obo-issuer`). Serves until Ctrl-C."""
    parser = argparse.ArgumentParser(description="Mock OBO token issuer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--key-file", type=Path, default=None, help="RSA private key PEM")
    args = parser.parse_args(argv)
    configure_logging()
    cfg = token_config()  # OBO_ISSUER, OBO_AUDIENCE, OBO_TOKEN_TTL_SECONDS
    try:
        keypair = load_keypair(args.key_file)
    except TokenError as exc:
        log.error("%s", exc)
        return 2
    log.info("issuer=%s audience=%s kid=%s", cfg.issuer, cfg.audience, keypair.kid)
    uvicorn.run(create_app(keypair, cfg), host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
