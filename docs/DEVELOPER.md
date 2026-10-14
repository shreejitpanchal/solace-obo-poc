# Developer guide

For developers who will run, change or extend this POC. The README explains
the use case and how to run the stack; this document explains the code.

Contents

1. [Repository layout](#1-repository-layout)
2. [Module map](#2-module-map)
3. [How a message moves through the code](#3-how-a-message-moves-through-the-code)
4. [Local development loop](#4-local-development-loop)
5. [Tests](#5-tests)
6. [Configuration](#6-configuration)
7. [Extending the POC](#7-extending-the-poc)
8. [Debugging](#8-debugging)
9. [CI and release](#9-ci-and-release)
10. [Conventions](#10-conventions)

---

## 1. Repository layout

```
src/solace_obo_poc/     the application package (one package, five entry points)
tests/                  pytest suite; mirrors the package module for module
scripts/dev.sh, dev.ps1 the only place that knows how to build, lint, test, scan
scripts/audit_gate.py   the scan gate shared by both scripts
docker/Dockerfile       service image (all five entry points)
docker-compose.yml      local stack: broker (optional), issuer, services
.env.example            every setting, with the two broker modes
.github/workflows/      ci.yml (gates) and tag.yml (release on tag)
pyproject.toml          dependencies, entry points, ruff/pytest/coverage config
uv.lock                 pinned dependency set; commit it
```

Python 3.12, managed with [uv](https://docs.astral.sh/uv/). `uv sync` creates
`.venv` from `uv.lock`; `uv run <command>` runs inside it.

---

## 2. Module map

| Module | Responsibility | Depends on |
|---|---|---|
| `config.py` | Reads settings from environment variables, applies defaults, validates them. Masks the password for logs. | stdlib |
| `topics.py` | Topic and queue naming, and `validate_level`, the one gate for identifiers that become topic levels. | stdlib |
| `obo_token.py` | `KeyPair` (issuer side: sign, export JWKS) and `JwksVerifier` (everyone else: verify offline). `TOKEN_PROPERTY` = `obo-token`. | PyJWT, cryptography, httpx |
| `authz.py` | `authorize_event`: parse topic, verify token, bind `sub` to sender-id, then decode payload. | topics, obo_token |
| `broker.py` | Wrapper over the Solace Python API. `Broker.publish` (persistent, waits for ack), `Broker.subscribe` (durable queue + topic subscription + ack). `Inbound`/`Outbound` are the SDK-free message shapes. | solace-pubsubplus (imported lazily) |
| `issuer.py` | FastAPI app: `POST /token`, `GET /.well-known/jwks.json`, `GET /healthz`. Entry point `obo-issuer`. | fastapi, uvicorn, obo_token |
| `publisher.py` | `fetch_token` (returns `IssuedToken` with expiry), `build_message`, and the timed publish loop with token refresh; entry point `obo-publisher`. | broker, obo_token, topics |
| `validator.py` | `forward_message` (pure mapping raw topic -> validated topic, token untouched), `make_handler`, entry point `obo-validator`. | broker, topics |
| `consumer.py` | Shared loop for the two applications: `make_handler` (authorize then `on_event`), `run`. | authz, broker, obo_token |
| `recipient.py` | Entry point `obo-recipient --thread-id T`; supplies queue, subscription and `on_event`. | consumer, topics |
| `profiler.py` | Entry point `obo-profiler --sender-id A`; same shape as recipient. | consumer, topics |

Dependency direction is strictly downwards in this table: nothing below
`broker.py` imports a service module, and no service imports another service.
`consumer.py` is the only shared piece between the two applications.

Every module's docstring opens with "where it sits in the flow"; read those
first when orienting.

---

## 3. How a message moves through the code

Function-level trace of one event, matching the five steps in the README.

```
publisher.main
  ├─ topics.validate_level(sender_id / thread_id / actor)      reject unsafe ids first
  ├─ config.publisher_config    PUBLISH_INTERVAL_SECONDS / PUBLISH_COUNT (flags override)
  ├─ publisher.fetch_token      POST OBO_ISSUER_URL/token  ──►  issuer: KeyPair.issue
  └─ loop (every interval, until Ctrl-C or count reached)
        ├─ IssuedToken.needs_refresh → publisher.fetch_token   30 s before expiry
        ├─ publisher.build_message    Outbound(topic=topics.raw_topic(...),
        │                                      properties={"obo-token": jwt},
        │                                      payload=json)
        └─ broker.Broker.publish      persistent publish, waits for ack

validator.main
  └─ broker.Broker.subscribe(q.poc.validator, ["poc/msg/>"], handler)
        └─ validator.make_handler → handle(Inbound)
              ├─ validator.forward_message
              │     ├─ topics.parse_topic             sender-id, thread-id from the topic
              │     └─ Outbound(topic=topics.validated_topic(...), payload unchanged,
              │                 properties unchanged + "obo-forwarded-at")
              └─ broker.Broker.publish              or log.warning("dropped ...") and return
                 (the token is NOT inspected here; see authz below)

recipient.main / profiler.main
  ├─ topics.validate_level(thread_id | sender_id)
  └─ consumer.run(name, queue_name, subscription, on_event)
        ├─ JwksVerifier.from_url                        (once)
        └─ Broker.subscribe(q.poc.recipient.<T> | q.poc.profiler.<A>, [subscription], handler)
              └─ consumer.make_handler → handle(Inbound)
                    ├─ authz.authorize_event
                    │     ├─ topics.parse_topic
                    │     ├─ JwksVerifier.verify
                    │     ├─ claims.subject == parts.sender_id
                    │     └─ decode(payload)             only reached if all above passed
                    └─ on_event(AuthorizedEvent)         the application's own logic
```

Two invariants to preserve when changing anything on this path:

- **Verify before read.** `authz.authorize_event` must not call `decode`
  before the token has passed. `tests/test_authz.py` asserts it with a spy
  decoder; keep that test green.
- **Validate before splice.** Any string that becomes a topic level or a
  queue name goes through `topics.validate_level`. Add new identifiers there,
  not with ad-hoc checks.

Threading: the SDK delivers messages on its own thread and calls the handler
there. `Broker.wait` only parks the main thread. Handlers must be quick and
must not raise for a bad message (log and return; the wrapper acks).

---

## 4. Local development loop

```sh
uv sync                                  # once, and after pyproject.toml changes (then uv lock)
./scripts/dev.sh vet test                # ruff + pytest, the inner loop
./scripts/dev.sh all                     # build vet test: what CI runs (plus scan)
./scripts/dev.sh full                    # + cov image scan graphify: before tagging
```

On Windows use `./scripts/dev.ps1` with the same task names. Both scripts
write `scripts/logs/<task>.log` and end each task with a footer line
(`<timestamp> | <task> | <seconds>s | OK|FAILED`).

Run a single service against the compose broker:

```sh
docker compose up -d solace              # broker only
uv run obo-issuer                        # then the others, each in its own terminal
uv run --env-file .env obo-validator     # .env for non-default settings
```

Formatting and linting are ruff, configured in `pyproject.toml` (line length
100). `uv run ruff format .` fixes formatting; `vet` only checks.

---

## 5. Tests

`uv run pytest -q` runs everything that needs no broker, in a few seconds.

| File | Covers |
|---|---|
| `test_topics.py` | topic/queue builders, `validate_level` rejections, `parse_topic` |
| `test_obo_token.py` | issue/verify round trip; expired, wrong `aud`, wrong `iss`, tampered signature, unknown `kid`, garbage; JWKS shape; `from_url`; PEM loading |
| `test_authz.py` | the gate order: decoder never called on missing/invalid/mismatched token |
| `test_issuer.py` | endpoints via FastAPI `TestClient`; 400 on unsafe ids |
| `test_publisher.py` | `build_message` shape, `fetch_token` and expiry via `httpx.MockTransport`, the publish loop (count, interval, env defaults, token refresh, Ctrl-C) with a fake broker |
| `test_validator.py` | `forward_message` forwards regardless of token, refuses foreign/validated topics, never logs a full token, `main` wiring |
| `test_consumer.py` | handler delivers only authorized events, `run` wiring, recipient/profiler queue and subscription names |
| `test_config.py` | defaults apply only when unset, password masking, boolean parsing |
| `test_broker.py` | `to_inbound`; plus two opt-in live-broker tests |
| `test_audit_gate.py` | the scan gate's fixable/unfixable rule |

Fixtures in `conftest.py`: a session `keypair`, a `token_cfg`, a `verifier`
built from the keypair's JWKS, and `issue(subject, **overrides)` to mint
tokens with any claim altered (`audience="x"`, `now=<past>`).

Broker tests are opt-in because they need a running broker:

```sh
docker compose up -d solace
SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py -v
```

They prove a round trip through a dynamically created queue and that a queue
spools while its consumer is down. They leave `q.poc.itest*` queues on the
broker; delete them in PubSub+ Manager if they bother you.

Testing pattern for anything that touches the broker: inject a fake. The
services take `Broker` from their module namespace, so
`monkeypatch.setattr("solace_obo_poc.validator.Broker", FakeBroker)` is
enough; see `test_validator.py` for a minimal fake.

---

## 6. Configuration

All settings are environment variables read by `config.py`; defaults apply
only when a variable is unset, so exported values and `.env` always win.

| Variable | Default | Read by |
|---|---|---|
| `SOLACE_HOST` | `tcp://localhost:55555` | every broker client |
| `SOLACE_VPN` | `default` | every broker client |
| `SOLACE_USERNAME` / `SOLACE_PASSWORD` | `default` / `default` | every broker client |
| `SOLACE_QUEUE_PREFIX` | `q.poc` | validator, recipient, profiler |
| `SOLACE_CREATE_QUEUES` | `true` | validator, recipient, profiler |
| `OBO_ISSUER_URL` | `http://127.0.0.1:8000` | publisher, recipient, profiler |
| `OBO_ISSUER` | `solace-obo-poc-issuer` | issuer (writes `iss`), verifiers (expect it) |
| `OBO_AUDIENCE` | `poc` | issuer (writes `aud`), verifiers (expect it) |
| `OBO_TOKEN_TTL_SECONDS` | `300` | issuer |
| `PUBLISH_INTERVAL_SECONDS` | `5` | publisher (`--interval` overrides) |
| `PUBLISH_COUNT` | `0` | publisher (`--count` overrides; 0 = until stopped) |
| `LOG_LEVEL` | `INFO` | every service (`DEBUG` for per-step detail) |

A bad value fails at start-up with a `ConfigError` naming the variable and
the fix. Add a new setting by extending the dataclass and the reader in
`config.py`, the table in `.env.example`, and the compose `x-app`
environment block.

---

## 7. Extending the POC

**Replace the mock issuer with the real one.** Keep the contract, not the
code: a JWT with `iss`, `aud`, `sub` (the user), `act.sub` (the service),
`iat`, `exp`, `jti`, signed RS256 with a `kid` header, and a JWKS endpoint.
Then point `OBO_ISSUER_URL`, `OBO_ISSUER` and `OBO_AUDIENCE` at it. If the
real token request differs from `POST /token {sender_id, actor}`, change
only `publisher.fetch_token`. If claims differ in name, change only
`JwksVerifier.verify` and `OboClaims`.

**Add a consuming application.** Copy `recipient.py`: define `NAME`, an
`on_event`, and in `main` pass a queue-name function, a subscription and
`on_event` to `consumer.run`. Add the queue and subscription builders to
`topics.py` and a console script to `pyproject.toml`. The token gate comes
for free from `consumer.make_handler`; do not bypass it.

**Change the topic layout.** `RAW_PREFIX`, `VALIDATED_PREFIX` and the
builders in `topics.py` are the single source; `parse_topic` must stay in
sync. Update `test_topics.py` alongside.

**Pre-provisioned queues.** Set `SOLACE_CREATE_QUEUES=false`; the receiver
then uses `DO_NOT_CREATE` and a missing queue fails loudly at bind time. The
queue must be durable, exclusive (or change `Queue.durable_exclusive_queue`
in `broker.py`), and carry the subscription from the table in the README.

**Per-service broker identities.** Today every service uses the one
`SOLACE_USERNAME`. To give each its own client-username and ACL, set the
variables per service in the compose `environment` blocks; no code change.

**Redeliver instead of dropping on handler errors.** In
`broker.Broker.subscribe`, the adapter acks in `finally`. To make unexpected
exceptions redeliver, settle the message with a failed outcome instead of
acking in the `except` branch, and set a max-redelivery count on the queue so
a poison message ends in a DMQ rather than looping.

**Add an event field.** The payload is opaque to every hop except
`on_event`; add fields in `publisher.build_message` and read them in the
application's `on_event`. Nothing else cares.

---

## 8. Debugging

- **Logs.** Every service logs at INFO with `configure_logging`. Rejections
  are WARNING with the reason and a masked token (`eyJhbGci...`); full tokens
  are never logged, by design. `docker compose logs -f validator recipient profiler`.
- **"connection refused" at start.** Expected while the broker container
  boots; `Broker.connect` retries for up to three minutes.
- **"token signed with unknown kid".** The issuer restarted and has a new key
  while the verifiers still hold the old JWKS. Restart recipient and
  profiler, or run the issuer with `--key-file` for a stable key.
- **"cannot fetch JWKS" / "cannot obtain OBO token".** `OBO_ISSUER_URL` does
  not reach the issuer. Inside compose it is `http://issuer:8000`; from the
  host the container is on port 8001.
- **Nothing arrives at an application.** Check the queue in PubSub+ Manager
  (Queues): does it exist, does it carry the expected subscription, is the
  consumer bound, are messages spooled? A `sender-id`/`thread-id` typo shows
  up here as a subscription that matches nothing.
- **Event refused by an application with "does not match sender-id".** The
  token's `sub` differs from the topic's sender-id: the publisher asked for a
  token for one user and published under another.
- **Exclusive queue "already bound".** Two instances with the same filter
  started; the queue is exclusive. Stop one, or use a different `thread-id`/`sender-id`.
- **Inspect a token.** `uv run python -c "import jwt,sys; print(jwt.decode(sys.argv[1], options={'verify_signature': False}))" <token>`.

---

## 9. CI and release

Nothing runs on ordinary pushes or pull requests, by design. Two workflows:

- `ci.yml` runs `./scripts/dev.sh all scan` on ubuntu and `./scripts/dev.ps1
  all scan` on windows, uploading `scripts/logs/`. It is called by the tag
  workflow, by manual dispatch, and on PRs that touch the workflows or the
  dev scripts (which is how Dependabot's action bumps get verified).
- `tag.yml` runs on `git push origin vX.Y.Z`: gates, then PyInstaller
  binaries for the targets in the `BUILD_TARGETS` repo variable
  (linux/amd64; PyInstaller does not cross-compile), then a GitHub release
  with checksums. A failure anywhere leaves no release.

Before tagging, run `./scripts/dev.sh full` on a clean checkout: it is the
only check that catches a file that was never committed.

`scan` audits `uv.lock` with pip-audit; a vulnerability with a fix version
fails the task, one without a fix warns. Bump the dependency in
`pyproject.toml`, run `uv lock`, re-run `scan`.

---

## 10. Conventions

- **Validate at the boundary, once.** Identifiers go through
  `topics.validate_level`; settings through `config.py`. No ad-hoc parsing of
  security-relevant input elsewhere.
- **Errors say what, why, and what to do.** Every `TokenError`, `ConfigError`
  and `AuthorizationError` names the offending value (masked where secret)
  and the fix or variable. Keep that bar for new errors.
- **No secrets in logs.** Use `mask_token` and `BrokerConfig.masked`.
- **SDK stays behind `broker.py`.** Services never import `solace.*`.
- **Tests next to behaviour.** A new branch gets a test in the mirroring
  test file in the same change; a removed branch loses its test.
- **Docs next to behaviour.** A change in how someone runs or extends the
  code updates the README or this guide in the same change.
