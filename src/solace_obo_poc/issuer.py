"""Mock OBO issuer: mints RS256 tokens and serves the JWKS the other hops verify with.

Stands in for the customer's real issuer. The signing key is generated per
process unless --key-file points at a PEM, so tokens from a previous run stop
verifying after a restart; that is deliberate for a POC.
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
    sender_id: str
    actor: str = "poc-publisher"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    expires_in: int


def create_app(keypair: KeyPair, cfg: TokenConfig) -> FastAPI:
    app = FastAPI(title="OBO issuer (POC)")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/.well-known/jwks.json")
    def jwks() -> dict:
        return keypair.jwks()

    @app.post("/token", response_model=TokenResponse)
    def token(req: TokenRequest) -> TokenResponse:
        try:
            validate_level(req.sender_id, "sender_id")
            validate_level(req.actor, "actor")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    if key_file is None:
        return KeyPair.generate()
    try:
        return KeyPair.from_pem(key_file.read_bytes(), kid=key_file.stem)
    except OSError as exc:
        raise TokenError(f"cannot read --key-file {key_file}: {exc}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Mock OBO token issuer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--key-file", type=Path, default=None, help="RSA private key PEM")
    args = parser.parse_args(argv)
    configure_logging()
    cfg = token_config()
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
