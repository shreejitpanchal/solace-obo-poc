# Solace OBO Python POC

A proof of concept showing how an **on-behalf-of (OBO) token** travels with an
event through Solace PubSub+, is forwarded by an intermediate validator
service into a validated topic space, and is verified by every consuming
application before it reads the event. The security decision is made per message, from the token carried in
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

The POC puts these pieces together:

| Piece | Stands for |
|---|---|
| **Issuer** | The customer's token issuer. In the POC it is a small mock service that signs tokens and publishes its public keys. |
| **Publisher** | The application that creates events for a user. |
| **Solace PubSub+ broker** | The event mesh that routes and spools the events. |
| **Validator** | A service in the middle that moves events from the raw topic space into the "validated" topic space the applications subscribe to, carrying the token along untouched. |
| **Recipient** and **User-Profiler** | The consuming applications for messages, each filtering the validated events differently and each re-checking the token itself. |
| **Six news consumers** | Kai-Go-Mobile, Kai-Go-Desktop, Kai-Signals-Engine, Kai-StoreFront, Kai-Cowork and Kai-Zen: each receives every validated news event on its own queue and re-checks the token the same way (section 4). |

```
                          ┌──────────┐
            1. token      │  Issuer  │   JWKS (public keys)
         ┌───────────────►│          │◄───────────────┬───────────────┐
         │                └──────────┘                │               │
         │                                            │               │
 ┌───────┴───────┐  2. publish                 ┌──────┴─────┐         │
 │   Publisher   │  poc/msg/{sender}/{thread}│            │         │
 │  application  ├─────────────► Solace ───────►│ Validator  │         │
 └───────────────┘   obo-token in properties    │ 3. forward │         │
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

News follows the same five steps in its own topic spaces, with six consumers
instead of two:

```
 ┌───────────────┐  2. publish --kind news        ┌────────────┐
 │   Publisher   │  poc/news/{sender}/{thread}    │ Validator  │  4. republish
 │  application  ├─────────────► Solace ─────────►│ 3. forward ├──► poc/news-validate/{sender}/{thread}
 └───────────────┘   obo-token in properties      └────────────┘               │
                                                                               │
          ┌──────────────┬──────────────┬─────── Solace ──────┬──────────────┬─┴────────────┐
          │              │              │                     │              │              │
   q.poc.news.     q.poc.news.    q.poc.news.          q.poc.news.    q.poc.news.    q.poc.news.
   Kai-Go-Mobile   Kai-Go-Desktop Kai-Signals-Engine   Kai-StoreFront Kai-Cowork     Kai-Zen
   (poc/news-validate/>  on every queue; each application 5. verifies the token, then reads)
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
key never leaves the issuer; the validator carries the token without reading
it, and the consuming applications only ever
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
is safely spooled on the broker. It then waits `PUBLISH_INTERVAL_SECONDS`
(default 5) and repeats, until stopped or until `PUBLISH_COUNT` events have
been sent. Because a run can outlive the token's 300-second lifetime, the
publisher asks the issuer for a fresh token 30 seconds before the current one
expires, so every event leaves with a token that will still verify downstream.

Identifiers that end up in a topic (`sender-id`, `thread-id`) are validated
before use: only letters, digits, `.`, `_` and `-`, at most 64 characters.
This stops a crafted id such as `alice/>` from rewriting the topic structure.

### Step 3: the validator forwards the event into the validated topic space

The validator consumes from its own queue, `q.poc.validator`, which
subscribes to `poc/msg/>` and `poc/news/>` (everything either publisher
sends). For each message it:

1. **Parses the topic** and extracts `sender-id` and `thread-id` from it.
2. **Republishes** the event, payload byte-for-byte unchanged and every
   message property carried along, to the validated topic
   `poc/msg-validate/alice/t-1`. A property `obo-forwarded-at` records when
   it was forwarded.

The validator does **not** inspect or verify the `obo-token` property. The
token passes through it as opaque metadata, and the authorization decision is
made by each consuming application in step 5. The validator therefore has no
connection to the issuer and holds no keys; its job is to move events from
the raw topic space, which only it subscribes to, into the validated topic
space that the applications subscribe to.

The only messages it refuses are ones it cannot forward: a topic outside
`poc/msg/` (including its own output, which guards against a loop) or a
malformed `sender-id`/`thread-id`. Those are acknowledged and logged with a
masked token prefix. The validator never opens the payload.

Example log lines:

```
forwarded poc/msg/alice/t-1 -> poc/msg-validate/alice/t-1 (token eyJhbGci...)
dropped other/topic (token eyJhbGci...): topic 'other/topic' is outside poc/msg/ and poc/msg-validate/
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
| News consumer `Kai-Zen` (one of six) | every news event | `q.poc.news.Kai-Zen` | `poc/news-validate/>` |

So an event from `alice` in `t-1` lands in both queues; an event from `bob`
in `t-1` lands only in the recipient's queue; an event from `alice` in `t-2`
lands only in the profiler's queue. Because the queues are durable, events
published while a consumer is down wait on the broker and are delivered when
it reconnects.

News is a second kind of event with its own topic spaces (`poc/news/...`
raw, `poc/news-validate/...` validated). The same validator forwards it. Six
applications consume news (Kai-Go-Mobile, Kai-Go-Desktop, Kai-Signals-Engine,
Kai-StoreFront, Kai-Cowork, Kai-Zen); each has its own queue carrying the
same `poc/news-validate/>` subscription, so one news event is spooled into
six queues and every application receives its own copy. The recipient and
the profiler never see news, because their subscriptions are in the
`poc/msg-validate` space.

### Step 5: each consumer verifies the token, then reads the payload

This is where the OBO token is checked. The Recipient and the User-Profiler
each verify it with their own copy of the issuer's public keys (JWKS,
fetched once at start-up):

- the `obo-token` property is present,
- the signature is valid for the key named by `kid` (an unknown `kid` is a rejection),
- `iss` and `aud` are the expected values,
- the token has not expired, and `sub`, `jti`, `iat`, `exp` are present,
- the token's `sub` equals the `sender-id` in the topic, so a valid token
  for `alice` on a topic claiming `bob` is rejected. This prevents one user's
  token from being reused to speak for another user. Only when all of that passes does the
application decode the JSON payload and hand it to its business logic.

The order is enforced in code: the payload decoder is not even called until
the token has passed. A test asserts exactly that. A rejected event is logged
with a masked token and acknowledged, and the application keeps consuming.

Example log lines:

```
recipient received: sender=alice thread=t-1 sub=alice act=poc-publisher payload={'sender_id': 'alice', 'thread_id': 't-1', 'seq': 1, 'text': 'hello from poc', ...}
user-profiler profiling: sender=alice thread=t-1 sub=alice act=poc-publisher payload={...}
```

Why at the consumer and not at the validator? The application is the party
that acts on the event, so it owns the authorization decision: it does not
have to trust any hop in between, and a misconfigured intermediate cannot
grant access it should not. Verification is offline and cheap (a signature
check against a cached public key), so doing it at the point of use costs
nothing. The validator stays a plain forwarder that never needs keys.

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
- **One event, many independent consumers.** The six news applications each
  own a queue with the same `poc/news-validate/>` subscription, so the broker
  spools one news event into six queues and each application consumes,
  verifies and acks at its own pace.
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
poc/news/{sender-id}/{thread-id}           raw news: `obo-publisher --kind news`, consumed only by the validator
poc/news-validate/{sender-id}/{thread-id}  validated news: published by the validator, consumed by the six news consumers
```

Messaging is guaranteed end to end: the publisher and the validator wait for
the broker's ack, and each consumer binds to its own durable exclusive queue
that carries its topic subscription. A consumer acks each message after
handling it, including events it drops for a bad token.

| Queue | Owner | Topic subscription |
|---|---|---|
| `q.poc.validator` | validator | `poc/msg/>` and `poc/news/>` |
| `q.poc.recipient.<thread-id>` | recipient for that thread | `poc/msg-validate/*/<thread-id>` |
| `q.poc.profiler.<sender-id>` | user-profiler for that sender | `poc/msg-validate/<sender-id>/*` |
| `q.poc.news.Kai-Go-Mobile` | news consumer Kai-Go-Mobile | `poc/news-validate/>` |
| `q.poc.news.Kai-Go-Desktop` | news consumer Kai-Go-Desktop | `poc/news-validate/>` |
| `q.poc.news.Kai-Signals-Engine` | news consumer Kai-Signals-Engine | `poc/news-validate/>` |
| `q.poc.news.Kai-StoreFront` | news consumer Kai-StoreFront | `poc/news-validate/>` |
| `q.poc.news.Kai-Cowork` | news consumer Kai-Cowork | `poc/news-validate/>` |
| `q.poc.news.Kai-Zen` | news consumer Kai-Zen | `poc/news-validate/>` |

By default (`SOLACE_CREATE_QUEUES=true`) a consumer creates its queue and
subscription on start if they are missing, which the broker allows when the
client profile has "allow guaranteed endpoint create" enabled, as the default
profile does. On a broker where an administrator provisions queues, set
`SOLACE_CREATE_QUEUES=false`: a missing queue then fails loudly at start
instead of being created. `SOLACE_QUEUE_PREFIX` changes the `q.poc` prefix.
Queues are durable and persist after the consumer disconnects; delete them in
PubSub+ Manager (Queues) when a thread-id or sender-id is no longer needed.

### How a queue's topic subscription drives delivery

Nothing in the POC sends a message *to* a queue. Publishers only publish to
a topic, and consumers only bind to a queue. The link between the two is the
topic subscription attached to each queue, which the broker evaluates on
every published message:

1. **Publish.** A publisher sends a persistent message to a topic, for
   example `poc/news-validate/alice/t-1`. It knows nothing about queues and
   waits only for the broker's acknowledgement.
2. **Match.** The broker compares that topic with the subscription on every
   queue in the VPN. A subscription is matched level by level: a literal
   level must be equal, `*` matches exactly one level of any value, and `>`
   at the end matches one or more remaining levels.
3. **Spool.** For each queue whose subscription matches, the broker places
   its own copy of the message in that queue. A queue with no match receives
   nothing. A queue that matches receives the message whether or not its
   consumer is connected: the copy waits in the queue's spool.
4. **Deliver and ack.** A consumer bound to the queue receives the message
   on its callback, handles it (here: verify the token, then read), and acks
   it; the ack removes that copy from that queue only. Other queues' copies
   are unaffected. If the consumer disconnects before acking, the message is
   redelivered when it rebinds.

Because matching happens per queue, the number of consumers that receive an
event is simply the number of queues whose subscription matches its topic.
Adding or removing a consumer is adding or removing a queue; no publisher
changes.

| Published topic | `q.poc.validator` `poc/msg/>` `poc/news/>` | `q.poc.recipient.t-1` `poc/msg-validate/*/t-1` | `q.poc.profiler.alice` `poc/msg-validate/alice/*` | `q.poc.news.<app>` x6 `poc/news-validate/>` |
|---|---|---|---|---|
| `poc/msg/alice/t-1` (publisher) | yes | no | no | no |
| `poc/msg-validate/alice/t-1` (validator) | no | yes | yes | no |
| `poc/msg-validate/bob/t-1` | no | yes | no | no |
| `poc/msg-validate/alice/t-2` | no | no | yes | no |
| `poc/news/alice/t-1` (publisher `--kind news`) | yes | no | no | no |
| `poc/news-validate/alice/t-1` (validator) | no | no | no | yes, all six |
| `poc/news-validate/bob/t-9` | no | no | no | yes, all six |

Reading the rows:

- The validator's queue carries two subscriptions, so it attracts every raw
  message and every raw news event, and nothing from the validated spaces.
  That is what prevents it from consuming its own output.
- `poc/msg-validate/*/t-1` matches any sender in thread `t-1`; the `*`
  consumes exactly the sender level. `poc/msg-validate/alice/*` fixes the
  sender and leaves the thread open. The same validated message can match
  both, which is why `alice` in `t-1` lands in two queues.
- `poc/news-validate/>` has no literal level after the prefix, so every
  validated news event matches it. Six queues carry that one subscription,
  so one news event becomes six spooled copies, one per application. Each
  application acks only its own copy; Kai-Zen being offline never holds up
  Kai-Go-Mobile.
- No queue in the `msg` family subscribes to `poc/news-validate/...` and
  vice versa, so message traffic and news traffic never cross.

In PubSub+ Manager, Queues shows each queue's subscription under its
*Subscriptions* tab, and the *Messages Queued* count rises on every matching
queue while its consumer is stopped, which is the spooling in step 3 made
visible.

---

## 5. Components

| Command | Role |
|---|---|
| `obo-issuer` | Mock issuer. Mints tokens (`POST /token`) and serves `/.well-known/jwks.json`. |
| `obo-publisher --sender-id A --thread-id T` | Publisher. Fetches a token for A and publishes one event every `PUBLISH_INTERVAL_SECONDS` (default 5) with it in the `obo-token` property, refreshing the token before it expires. `--count N` publishes N and exits. |
| `obo-validator` | Consumes `q.poc.validator`, republishes each event unchanged to the validated topic. Does not inspect the token. |
| `obo-recipient --thread-id T` | Consumes `q.poc.recipient.T`, verifies the token, then reads the payload. |
| `obo-profiler --sender-id A` | Consumes `q.poc.profiler.A`, verifies the token, then reads the payload. |
| `obo-news-consumer --app X` | One of the six news applications (`Kai-Go-Mobile`, `Kai-Go-Desktop`, `Kai-Signals-Engine`, `Kai-StoreFront`, `Kai-Cowork`, `Kai-Zen`). Consumes `q.poc.news.X`, verifies the token, then reads the payload. |

`obo-publisher --kind news ...` publishes to `poc/news/A/T` instead of
`poc/msg/A/T`; everything else about it is the same.

All six are entry points of one Python package (`src/solace_obo_poc/`),
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
docker compose up -d --build          # broker, issuer, validator, publisher, recipient (t-1), profiler (alice),
                                      # news publisher and the six news consumers
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
docker compose up -d --build          # everything except the broker: issuer, validator, publishers, all consumers
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

Once the stack is up the publisher emits one event every 5 seconds as `alice`
in thread `t-1`. Watch all four services together:

```sh
docker compose logs -f publisher validator recipient profiler
```

Expected, repeating every 5 seconds with an increasing `seq`:

- publisher: `published seq=N to poc/msg/alice/t-1 (token eyJhbGci...)`
- validator: `forwarded poc/msg/alice/t-1 -> poc/msg-validate/alice/t-1 (token eyJhbGci..., not inspected)`
- recipient and profiler: `token eyJhbGci... verified: sub=alice ...`, then
  `authorized poc/msg-validate/alice/t-1 ...`, then the application line with
  the message text (steps 3 to 5 above).

Every 270 seconds the publisher logs that it is refreshing its token, and the
`kid`/`jti` in the consumers' lines change accordingly. In PubSub+ Manager,
Queues shows the validator, recipient, profiler and six news queues with
their subscriptions.

**The news flow.** The `news-publisher` service runs alongside with
`--kind news`, so every 5 seconds one news event goes to
`poc/news/alice/t-1`, the validator forwards it to
`poc/news-validate/alice/t-1`, and all six news consumers receive it while
the recipient and profiler stay silent:

```sh
docker compose logs -f news-publisher news-kai-go-mobile news-kai-zen     # or any of the six news-* services
```

Expected on each news consumer: `news-consumer received news seq=N in thread t-1 from alice: 'breaking news from poc in docker' ...`.

**Guaranteed delivery.** Stop the recipient for a while, then start it again
and watch it receive every event that arrived while it was down:

```sh
docker compose stop recipient            # publisher keeps publishing
sleep 20
docker compose start recipient && docker compose logs -f recipient
```

**Topic filtering.** Run a one-off burst with a different sender or thread
alongside the continuous publisher and see which application receives it:

```sh
SENDER_ID=bob PUBLISH_COUNT=3 docker compose run --rm publisher    # recipient sees it (thread t-1), profiler (alice) does not
THREAD_ID=t-2 PUBLISH_COUNT=3 docker compose run --rm publisher    # profiler sees it (sender alice), recipient (t-1) does not
```

`SENDER_ID`, `THREAD_ID` and `MESSAGE` set the publisher's arguments
(`NEWS_MESSAGE` the news publisher's text); `PUBLISH_INTERVAL_SECONDS` and
`PUBLISH_COUNT` the cadence of both; and `SENDER_ID` and `THREAD_ID` also set
the profiler and recipient filters at `up` time.
Change them in `.env` and `up` again to re-filter. (On PowerShell, set them
in `.env` rather than as a command prefix.)

**A rejected token.** The issuer generates a new signing key every time it
starts, while the consumers keep the public keys they fetched at their own
start. Restart the issuer together with the publisher so the publisher
fetches a token signed by the new key: the validator forwards the events as
always, but each application refuses them with
`refusing event on poc/msg-validate/alice/t-1: ... token signed with unknown kid`
and never reads the payload. Restart the two applications to pick up the new
keys and the flow recovers.

```sh
docker compose restart issuer publisher                    # recipient and profiler now refuse every event
docker compose restart recipient profiler                  # flows again
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
uv run obo-news-consumer --app Kai-Zen                # terminal 5; repeat per app for all six
uv run obo-publisher --sender-id alice --thread-id t-1          # one event every 5 s; add --count 3 for a burst
uv run obo-publisher --kind news --sender-id alice --thread-id t-1   # news events; only the news consumers receive them
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
| `PUBLISH_INTERVAL_SECONDS` | `5` | Seconds between the publisher's events (decimal allowed) |
| `PUBLISH_COUNT` | `0` | Events to publish before exiting; `0` runs until stopped |
| `NEWS_MESSAGE` | `breaking news from poc in docker` | Text the compose `news-publisher` service sends (compose only) |
| `LOG_LEVEL` | `INFO` | `DEBUG` logs every token check and broker call; tokens stay masked |

---

## 9. Develop and release

The code itself is documented in [docs/DEVELOPER.md](docs/DEVELOPER.md):
module map, a function-level trace of one event, tests, how to extend the
POC (real issuer, new consumers, pre-provisioned queues), and debugging.

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
