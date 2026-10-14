# Solace OBO Python POC

A proof of concept showing how an **on-behalf-of (OBO) token** travels with an
event through Solace PubSub+, is checked by an intermediate validator service,
and is checked again by every consuming application before it reads the
event. The security decision is made per message, from the token carried in
the message itself, and not from the identity of the connection that delivered it.

Contents

1. [The use case](#1-the-use-case)
2. [End-to-end walkthrough](#2-end-to-end-walkthrough)
3. [What the POC demonstrates](#3-what-the-poc-demonstrates)
4. [Topics and queues on the broker](#4-topics-and-queues-on-the-broker)
5. [Components](#5-components)
6. [Run it with Docker](#6-run-it-with-docker)
7. [Run it on the host](#7-run-it-on-the-host)
8. [Settings](#8-settings)
9. [Develop and release](#9-develop-and-release)

---

## 1. The use case

Users of the application send messages to each other. Each message is an event: it
belongs to a **sender** (`sender-id`) and to a **conversation** (`thread-id`).
Two kinds of application consume those events:

- the **Recipient** application, which follows one conversation and needs
  every message in that thread, whoever sent it;
- the **User-Profiler** application, which follows one user and needs every
  message that user sent, in whichever thread.

Before any of these applications is allowed to read an event, the platform
must be sure that the event was really published **on behalf of** the user it
claims to come from. That proof is the OBO token: a signed token issued by a
trusted issuer that says "service X is acting on behalf of user Y, until time T".

The POC puts five pieces together:

| Piece | Stands for |
|---|---|
| **Issuer** | The customer's token issuer. In the POC it is a small mock service that signs tokens and publishes its public keys. |
| **Publisher** | The application that creates events for a user. |
| **Solace PubSub+ broker** | The event mesh that routes and spools the events. |
| **Validator** | A service in the middle that admits only events carrying a valid token into the "validated" topic space. |
| **Recipient** and **User-Profiler** | The consuming applications, each filtering the validated events differently and each re-checking the token itself. |

```
                          ┌──────────┐
            1. token      │  Issuer  │   JWKS (public keys)
         ┌───────────────►│          │◄───────────────┬───────────────┐
         │                └──────────┘                │               │
         │                                            │               │
 ┌───────┴───────┐  2. publish                 ┌──────┴─────┐         │
 │   Publisher   │  poc/msg/{sender}/{thread}│            │         │
 │  application  ├─────────────► Solace ───────►│ Validator  │         │
 └───────────────┘   obo-token in properties    │ 3. verify  │         │
                                                └──────┬─────┘         │
                                   4. republish        │               │
                       poc/msg-validate/{sender}/{thread}           │
                                                       │               │
                                 ┌──── Solace ─────────┴────────┐      │
                                 │                              │      │
                          ┌──────┴───────┐              ┌───────┴──────┴─┐
                          │  Recipient   │              │ User-Profiler  │
                          │  */{thread}  │              │  {sender}/*    │
                          │  5. verify,  │              │  5. verify,    │
                          │     then read│              │     then read  │
                          └──────────────┘              └────────────────┘
```

---

## 2. End-to-end walkthrough

The numbers below match the diagram. Every step names the exact topic,
message property and check involved, so the behaviour can be followed in the
service logs and in PubSub+ Manager.

### Step 1: the publisher obtains an OBO token

When the publisher wants to send on behalf of user `alice`, it first
calls the issuer:

```http
POST /token
{"sender_id": "alice", "actor": "poc-publisher"}
```

The issuer answers with a signed JWT (RS256). Its claims are the contract that
every later hop relies on:

| Claim | Example | Meaning |
|---|---|---|
| `iss` | `solace-obo-poc-issuer` | Who issued the token. Every verifier requires this exact value. |
| `aud` | `poc` | Which system the token is for. Every verifier requires this exact value. |
| `sub` | `alice` | **The user the event is sent on behalf of.** Must equal the `sender-id` in the topic. |
| `act.sub` | `poc-publisher` | The service presenting the token (the "actor" in the on-behalf-of relationship). |
| `iat` / `exp` | timestamps | Issued at and expiry. The default lifetime is 300 seconds. |
| `jti` | random id | Unique token id, useful for audit correlation. |
| `kid` (header) | key id | Which issuer key signed it, so verifiers pick the right public key. |

The issuer publishes its public keys at `/.well-known/jwks.json`. The private
key never leaves the issuer; the publisher, validator and consumers only ever
hold public keys.

> In production the mock issuer is replaced by the customer's real issuer.
> Nothing else changes as long as it produces a JWT with these claims and
> exposes a JWKS endpoint.

### Step 2: the publisher creates and publishes the event

The publisher builds one Solace message per event. The token and the business
data travel in **different parts** of the message:

| Part of the message | Content |
|---|---|
| **Topic** | `poc/msg/alice/t-1`, that is `poc/msg/{sender-id}/{thread-id}` |
| **User property `obo-token`** | the JWT from step 1 |
| **Payload** | the event data as JSON |

A concrete example:

```
Topic:       poc/msg/alice/t-1
Properties:  obo-token = eyJhbGciOiJSUzI1NiIsImtpZCI6Ij...   (the JWT)
Payload:     {
               "sender_id": "alice",
               "thread_id": "t-1",
               "seq": 1,
               "text": "hello from poc",
               "sent_at": "2026-10-14T09:30:00+00:00"
             }
```

Why a message property and not the payload? Properties are metadata the
broker and any hop can read without opening the payload, and the payload
stays exactly what the business application produced. The rule in this POC
is: **nobody opens the payload until the property has been verified.**

The publisher sends the message as a persistent (guaranteed) message and
waits for the broker's acknowledgement, so once `publish` returns the event
is safely spooled on the broker.

Identifiers that end up in a topic (`sender-id`, `thread-id`) are validated
before use: only letters, digits, `.`, `_` and `-`, at most 64 characters.
This stops a crafted id such as `alice/>` from rewriting the topic structure.

### Step 3: the validator checks the token and admits the event

The validator consumes from its own queue, `q.poc.validator`, which
subscribes to `poc/msg/>` (everything the publisher sends). For each
message it:

1. **Parses the topic** and extracts `sender-id` and `thread-id` from it.
2. **Extracts the token** from the `obo-token` message property. A message
   without that property is rejected immediately.
3. **Verifies the token** against the issuer's JWKS, which it fetched at
   start-up:
   - the signature is valid for the key named by `kid` (an unknown `kid` is a rejection),
   - `iss` and `aud` are the expected values,
   - the token has not expired,
   - `sub`, `jti`, `iat` and `exp` are all present.
4. **Binds the token to the topic**: the token's `sub` must equal the
   `sender-id` in the topic. A valid token for `alice` on a topic claiming
   `bob` is rejected. This is what prevents one user's token from being
   reused to speak for another user.
5. **Republishes** the event, payload byte-for-byte unchanged, to the
   validated topic `poc/msg-validate/alice/t-1`. The `obo-token` property
   is carried along, and a second property `obo-validated-at` records when
   the validator admitted it.

A message that fails any check is **not** republished. It is acknowledged on
the validator's queue (so it does not loop), and a warning is logged with the
reason and a masked token prefix (never the full token). The validator never
opens the payload.

Example log lines:

```
validated poc/msg/alice/t-1 -> poc/msg-validate/alice/t-1
dropped poc/msg/bob/t-1 (token eyJhbGci...): token subject 'alice' does not match sender-id 'bob'
dropped poc/msg/alice/t-1 (token <none>): no OBO token: message property 'obo-token' is missing
```

### Step 4: the broker fans the validated event out by topic

The validated topic space `poc/msg-validate/{sender-id}/{thread-id}` is
what the consuming applications subscribe to. Each consumer binds to its own
durable queue, and the queue's topic subscription does the filtering on the
broker:

| Application | Wants | Queue | Subscription |
|---|---|---|---|
| Recipient for thread `t-1` | every message in that thread | `q.poc.recipient.t-1` | `poc/msg-validate/*/t-1` |
| User-Profiler for user `alice` | every message alice sent | `q.poc.profiler.alice` | `poc/msg-validate/alice/*` |

So an event from `alice` in `t-1` lands in both queues; an event from `bob`
in `t-1` lands only in the recipient's queue; an event from `alice` in `t-2`
lands only in the profiler's queue. Because the queues are durable, events
published while a consumer is down wait on the broker and are delivered when
it reconnects.

### Step 5: each consumer verifies again, then reads the payload

The Recipient and the User-Profiler do **not** trust the validator hop. Each
one repeats the verification from step 3 with its own copy of the issuer's
public keys: token present, signature, `iss`, `aud`, expiry, and `sub` equal
to the `sender-id` in the topic. Only when all of that passes does the
application decode the JSON payload and hand it to its business logic.

The order is enforced in code: the payload decoder is not even called until
the token has passed. A test asserts exactly that. A rejected event is logged
with a masked token and acknowledged, and the application keeps consuming.

Example log lines:

```
recipient received: sender=alice thread=t-1 sub=alice act=poc-publisher payload={'sender_id': 'alice', 'thread_id': 't-1', 'seq': 1, 'text': 'hello from poc', ...}
user-profiler profiling: sender=alice thread=t-1 sub=alice act=poc-publisher payload={...}
```

Why verify twice? The validator keeps unverified traffic out of the validated
topic space, which protects every consumer at once and gives one place to
audit. The consumer's own check means that a misconfigured or compromised
hop, or an application that subscribes to the raw topics by mistake, still
cannot read an event without a valid token for its sender. Each application
is responsible for its own authorization decision.

---

## 3. What the POC demonstrates

- **Token travels with the event, not with the connection.** The broker
  connection identity is irrelevant to authorization; the per-message token
  is what counts.
- **Payload is opaque until authorized.** No hop decodes the business data
  before the token has passed.
- **Token is bound to the topic.** `sub` must match `sender-id`, so a token
  cannot be replayed under another user's name.
- **Verification is stateless and offline.** Every verifier needs only the
  issuer's public keys; there is no call back to the issuer per message.
- **Rejections are safe and visible.** Bad events are dropped, acknowledged,
  and logged with the reason and a masked token. Tokens are never logged in full.
- **Topic design does the routing.** Thread-based and sender-based consumers
  are just two subscriptions on the same validated topic space.
- **Delivery is guaranteed.** Persistent publishing and durable queues mean a
  restarting consumer loses nothing.

Things the POC deliberately leaves out, which a production rollout adds:

- a real issuer (replace the mock; the contract is the claims table in step 1);
- per-service broker identities and ACLs, so that for example only the
  validator may publish into `poc/msg-validate/>`;
- TLS to the broker, and token lifetime and key rotation policy.

---

## 4. Topics and queues on the broker

```
poc/msg/{sender-id}/{thread-id}            raw: published by the publisher, consumed only by the validator
poc/msg-validate/{sender-id}/{thread-id}   validated: published by the validator, consumed by applications
```

Messaging is guaranteed end to end: the publisher and the validator wait for
the broker's ack, and each consumer binds to its own durable exclusive queue
that carries its topic subscription. A consumer acks each message after
handling it, including events it drops for a bad token.

| Queue | Owner | Topic subscription |
|---|---|---|
| `q.poc.validator` | validator | `poc/msg/>` |
| `q.poc.recipient.<thread-id>` | recipient for that thread | `poc/msg-validate/*/<thread-id>` |
| `q.poc.profiler.<sender-id>` | user-profiler for that sender | `poc/msg-validate/<sender-id>/*` |

By default (`SOLACE_CREATE_QUEUES=true`) a consumer creates its queue and
subscription on start if they are missing, which the broker allows when the
client profile has "allow guaranteed endpoint create" enabled, as the default
profile does. On a broker where an administrator provisions queues, set
`SOLACE_CREATE_QUEUES=false`: a missing queue then fails loudly at start
instead of being created. `SOLACE_QUEUE_PREFIX` changes the `q.poc` prefix.
Queues are durable and persist after the consumer disconnects; delete them in
PubSub+ Manager (Queues) when a thread-id or sender-id is no longer needed.

---

## 5. Components

| Command | Role |
|---|---|
| `obo-issuer` | Mock issuer. Mints tokens (`POST /token`) and serves `/.well-known/jwks.json`. |
| `obo-publisher --sender-id A --thread-id T` | Publisher. Fetches a token for A, publishes events with it in the `obo-token` property. |
| `obo-validator` | Consumes `q.poc.validator`, verifies the token and that `sub` matches the sender-id, republishes to the validated topic. |
| `obo-recipient --thread-id T` | Consumes `q.poc.recipient.T`, re-verifies, then reads the payload. |
| `obo-profiler --sender-id A` | Consumes `q.poc.profiler.A`, re-verifies, then reads the payload. |

All five are entry points of one Python package (`src/solace_obo_poc/`),
built on the Solace Python API (`solace-pubsubplus`) and PyJWT.

---

## 6. Run it with Docker

The service image is built from `docker/Dockerfile`; the stack is
`docker-compose.yml`, configured through `.env`.

### Prerequisites

Docker Desktop (or Docker Engine with the compose plugin) and uv.

```sh
uv lock                       # writes uv.lock; commit it
cp .env.example .env          # pick the broker mode below by editing .env
```

The image build installs from `uv.lock` with `--locked`, so it is built from
pinned versions. Skipping `uv lock` fails the build with
`"/uv.lock": not found`; a stale lock after editing `pyproject.toml` fails
the `uv sync` step, and re-running `uv lock` fixes both.

### Option A: run the Solace broker as part of the stack

The default. `.env` ships with `COMPOSE_PROFILES=local-broker`, which enables
the `solace` service; leave `SOLACE_HOST` unset so the services use the
container on the compose network.

The service is a PubSub+ Enterprise single node (`solace-pubsub-enterprise`,
the customer's release). That image is not on a public registry: load it
first and set `SOLACE_IMAGE` in `.env` to the tag `docker image ls` shows.
Its spool lives in the `solace-ent-storage-group` volume, so queues and
spooled messages survive `docker compose down`; `down -v` wipes them. It
publishes the full broker port set on the host (SMF 55555, Manager 8080,
MQTT, AMQP, REST, TLS variants), so the issuer is published on host port
8001 instead of 8000.

```sh
docker compose up -d --build          # broker, issuer, validator, recipient (t-1), profiler (alice)
docker compose ps                     # wait until solace shows (healthy): 1-2 minutes
```

The broker's PubSub+ Manager is at http://localhost:8080 (admin / the
`SOLACE_ADMIN_PASSWORD` from `.env`); the issuer answers on
http://localhost:8001/healthz. The services start before the broker is
ready and retry their connection for up to three minutes, so early
`connection refused` lines in their logs are expected.

### Option B: reuse a Solace broker that is already running

Disable the broker container and point every service at the existing broker.
In `.env`:

```sh
COMPOSE_PROFILES=                           # empty: no solace service
SOLACE_HOST=tcp://broker.example.com:55555  # or tcps://...:55443
SOLACE_VPN=default
SOLACE_USERNAME=default
SOLACE_PASSWORD=...
```

Then:

```sh
docker compose up -d --build          # issuer, validator, recipient, profiler only
```

Where the broker runs decides the host name:

| Broker location | `SOLACE_HOST` |
|---|---|
| Another machine, cloud, or PubSub+ Cloud | `tcp://<host>:55555` or `tcps://<host>:55443` |
| A container started outside this compose project | `tcp://host.docker.internal:55555` (its SMF port must be published) |
| Installed natively on this machine | `tcp://host.docker.internal:55555` |

`host.docker.internal` resolves on Docker Desktop and, through the
`extra_hosts` entry in the compose file, on Linux as well. The client
username's ACL profile needs publish and subscribe rights on `poc/>`, and
its client profile needs guaranteed messaging send and receive plus, unless the
queues are pre-provisioned, guaranteed endpoint create (see section 4).

### Walk through the flow (both options)

Watch the three services in one terminal and publish from another:

```sh
docker compose logs -f validator recipient profiler       # terminal 1
docker compose run --rm publisher                         # terminal 2: 3 events as alice on thread t-1
```

Expected: the validator logs `validated poc/msg/alice/t-1 -> poc/msg-validate/alice/t-1`
three times, and both the recipient and the profiler log the three payloads
(steps 3 to 5 above). In PubSub+ Manager, Queues shows the three queues with
their subscriptions.

**Guaranteed delivery.** Stop the recipient, publish, start it again, and
watch it receive the events that arrived while it was down:

```sh
docker compose stop recipient
docker compose run --rm publisher
docker compose start recipient && docker compose logs -f recipient
```

**Topic filtering.** Change the sender or the thread and see which
application receives the event:

```sh
SENDER_ID=bob docker compose run --rm publisher    # recipient sees it (thread t-1), profiler (alice) does not
THREAD_ID=t-2 docker compose run --rm publisher    # profiler sees it (sender alice), recipient (t-1) does not
```

`SENDER_ID`, `THREAD_ID`, `COUNT` and `MESSAGE` override the publisher's
arguments; `SENDER_ID` and `THREAD_ID` also set the profiler and recipient
filters at `up` time, so change them in `.env` and `up` again to re-filter.

**A rejected token.** The issuer generates a new signing key every time it
starts, while the validator and the consumers keep the public keys they
fetched at their own start. Restart only the issuer and publish again: the
new token is signed by a key the validator does not know, so it is dropped
with `dropped poc/msg/alice/t-1 (token eyJhbGci...): token signed with unknown kid`
and nothing reaches the applications. Restart the three services to pick up
the new keys and the flow recovers.

```sh
docker compose restart issuer
docker compose run --rm publisher                          # validator drops all 3
docker compose restart validator recipient profiler
docker compose run --rm publisher                          # flows again
```

```sh
docker compose down                   # stop everything; add -v to drop the broker's data
```

---

## 7. Run it on the host

```sh
docker compose up -d solace                           # broker only, on localhost:55555
uv run obo-issuer                                     # terminal 1
uv run obo-validator                                  # terminal 2
uv run obo-recipient --thread-id t-1                  # terminal 3
uv run obo-profiler --sender-id alice                 # terminal 4
uv run obo-publisher --sender-id alice --thread-id t-1 --count 3
docker compose down
```

---

## 8. Settings

Settings are environment variables; defaults match the compose broker and a
local issuer. Copy `.env.example` to `.env` to override them: compose reads it
automatically, and host runs can use `uv run --env-file .env obo-<service>`.

| Variable | Default | Meaning |
|---|---|---|
| `SOLACE_HOST` | `tcp://localhost:55555` | Broker SMF address (`tcp://solace:55555` inside compose) |
| `SOLACE_VPN` | `default` | Message VPN |
| `SOLACE_USERNAME` / `SOLACE_PASSWORD` | `default` / `default` | Client username and password |
| `SOLACE_QUEUE_PREFIX` | `q.poc` | Prefix of the consumer queues |
| `SOLACE_CREATE_QUEUES` | `true` | Create missing queues on start, or require pre-provisioned ones |
| `OBO_ISSUER_URL` | `http://127.0.0.1:8000` | Base URL of the issuer (`http://issuer:8000` inside compose; port 8001 on the host for the container) |
| `OBO_ISSUER` | `solace-obo-poc-issuer` | Expected `iss` claim |
| `OBO_AUDIENCE` | `poc` | Expected `aud` claim |
| `OBO_TOKEN_TTL_SECONDS` | `300` | Token lifetime the issuer grants |

---

## 9. Develop and release

`scripts/dev.sh` and `scripts/dev.ps1` own every build command; CI only calls
task names. Logs land in `scripts/logs/<task>.log`.

```sh
./scripts/dev.sh all      # build vet test   (what CI runs, plus scan)
./scripts/dev.sh full     # all + cov image scan graphify, before tagging
```

`build` produces one PyInstaller binary per service in `dist/`, named
`obo-<service>-<os>-<arch>`. PyInstaller does not cross-compile, so the
release pipeline's `BUILD_TARGETS` must list only targets its runner can build
(linux/amd64 on the ubuntu runner).

The broker round-trip tests are opt-in: `SOLACE_INTEGRATION=1 uv run pytest tests/test_broker.py`
with the compose broker running. They prove a round trip through a
dynamically created queue and that a queue spools while its consumer is down.

Push a tag and the pipeline in `.github/workflows/tag.yml` runs the gates on
ubuntu and windows, builds the binaries and creates the GitHub release:

```sh
git tag v0.1.0 && git push origin v0.1.0
```
