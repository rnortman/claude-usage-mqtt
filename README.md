# claude-usage-mqtt

Polls Claude Code's usage windows for one or more login profiles and publishes
them to MQTT, so anything on the network can see how much headroom each account
has and get a ping when a 5-hour window resets.

It is the `usage-watchdog` idea from the
[`review-chain`](https://github.com/rnortman/claude-plugins/tree/main/plugins/review-chain)
plugin, generalized: multiple profiles instead of one session's, a broker
instead of stdout, and a throttle that assumes the API's rate limit is
stricter than we can see.

## Where it runs

On the machine where the Claude Code profiles live: the daemon reads each
profile's `.credentials.json`. The broker can be anywhere the daemon can reach
over the network.

## Credentials: read-only, on purpose

The daemon **never writes `.credentials.json` itself.** Claude Code owns that
file and rotates the refresh token when it renews; a second writer racing it can
leave the losing side holding a dead refresh token and force an interactive
re-login.

Claude Code only refreshes a profile's token **when that profile is actually
run**, so a profile nobody has used in days has an expired access token. The
daemon fixes that by running the profile — it launches the Claude Code CLI with
`CLAUDE_CONFIG_DIR` pointing at the profile (which is all the `claude-profile`
wrapper does) and a throwaway `-p /usage` prompt, lets the CLI renew and rewrite
its own credentials, then re-reads the file. The file still has exactly one
writer. The CLI's output is discarded — the structured reading comes from the
API.

If the refresh does not produce a usable token — the refresh token is dead too,
the CLI is not installed, `[refresh]` is turned off — nothing is papered over:
the profile publishes `auth = stale` and keeps serving the last known values
flagged with a `stale_reason` until a human logs in again. Attempts are rate-limited per
profile (`refresh.min_interval_s`) and capped at one per tick, so a profile that
cannot heal costs one subprocess every 15 minutes rather than one per tick, and a
host that wakes with every profile expired heals them one tick at a time instead
of stalling the loop on all of them at once. A token the API *refused* — as
opposed to one that merely expired — gets a single attempt, because Claude Code
has no reason to rotate a token that has not expired; after that it waits for a
re-login. Note that a run is a real Claude Code session: it does not consume the
daemon's usage-endpoint budget, but it does send requests to Anthropic on that
profile's account, which count against the 5-hour window. See `[refresh]` in
`config.example.toml`.

Three things the refresh path deliberately does *not* do. It does not run a
profile whose credentials file was rewritten in the last five minutes: that means
someone is using the profile right now and it will heal itself, and two Claude
Code processes racing the same refresh-token exchange is how one of them ends up
holding a rotated-out token and needing an interactive login. It does not retry a
refresh command that does not exist — that is a configuration error, so it is
logged once at `error` and refresh stays off until the config is reloaded; run
`--check` to see whether the command resolves on the daemon's `PATH`, which under
systemd is not your shell's. And it does not `SIGKILL` a refresh in flight on
shutdown: the child gets SIGTERM and ten seconds to finish writing, because a
kill between "the server rotated the refresh token" and "the client wrote it" is
exactly the failure that forces a re-login. Stopping the daemon mid-refresh can
therefore take that long.

Because the refresh cooldown must outlive a restart for the same reason
`last_poll_at` does, it rides on the profile's retained bookkeeping topic.

The shipped unit file has to leave `$HOME` writable and `MemoryDenyWriteExecute`
off for any of this to work — the CLI runs inside the daemon's sandbox and has to
rewrite its own credentials, and it is a JIT runtime. Set `refresh.enabled =
false` if you would rather keep the stricter sandbox and re-login by hand.

## Topic tree

Everything is retained **except** `reset/ping` (see below). Retained means a
consumer connecting at any moment immediately gets the last known value rather
than waiting out the poll interval.

```
<prefix>/daemon/status                              online | offline   (retained, LWT)
<prefix>/daemon/throttle                            {"spacing_s":60,"consecutive_ok":0,
                                                     "rate_limit_hits":0,"published_at":...,
                                                     "last_api_at":...}
<prefix>/daemon/sync                                internal startup marker; ignore

<prefix>/profile/<name>/auth                        ok | stale | missing | rejected
<prefix>/profile/<name>/snapshot                    full JSON reading (below)
<prefix>/profile/<name>/window/<key>/utilization    e.g. "42.50"
<prefix>/profile/<name>/window/<key>/resets_at      e.g. "2026-07-17T18:51:42Z"
<prefix>/profile/<name>/reset/next                  {"next_reset_at":...,"source":"api"}
<prefix>/profile/<name>/reset/ping                  reset event  (NOT retained)
<prefix>/profile/<name>/bookkeeping                 daemon's own state; ignore
```

An **empty payload on any `profile/<name>/` topic means the value was
withdrawn**, not that its content is empty: that is how MQTT deletes a retained
message, and the daemon publishes one on every topic of a profile that has left
its config (see "State lives in MQTT"). Consumers must skip empty payloads
rather than decode them — a live subscriber sees one publish per withdrawn
topic, a later subscriber sees the topic simply gone.

Window keys are `five_hour`, `seven_day`, and one per model- or surface-scoped
cap, e.g. `7-day_opus_4_8`. The scoped ones come and go as Anthropic changes the
limits, so subscribe with a wildcard rather than hardcoding the list.

### Snapshot payload

```json
{
  "profile": "personal",
  "auth": "ok",
  "updated_at": "2026-07-17T13:51:42Z",
  "next_poll_at": "2026-07-17T13:59:42Z",
  "poll_interval_s": 480,
  "subscription_type": "max",
  "windows": [
    {"key":"five_hour","label":"5-hour","utilization":42.5,"resets_at":"2026-07-17T18:51:42Z"},
    {"key":"7-day_opus_4_8","label":"7-day (Opus 4.8)","utilization":61.0,"resets_at":"..."}
  ]
}
```

A retained message has no inherent age, so **judge freshness from the payload,
not from the fact that you received it.** `updated_at` and `next_poll_at` are
there for exactly that.

`updated_at` is the **time of validity of what the payload says**, never the time
it was published. When the payload carries usage numbers, it is when those
numbers were read from the API — a republish of values already read does not move
it, so a snapshot's age is the age of its data and nothing else. When the payload
carries no numbers, only an error, it is when that error state was last known to
exist. `next_poll_at` is the one forward-looking field: the profile's *deadline*
(`last_poll_at + poll_interval_s`), the earliest the daemon will poll it. Under
contention the actual poll can be later, and a `next_poll_at` in the past means
"overdue, waiting for a slot"; judge staleness by `updated_at`.

"Are these numbers a reading taken for this publish?" is `stale_reason` absent.
When `stale_reason` is present, the windows are the last known good values carried
forward, `updated_at` is when they were read, and the string says why this
publish could not take a new one. With no windows, the string says why nothing
has ever been read and `updated_at` is when that error was observed — not a
reading time. Values are deliberately preserved rather than nulled: a real number
with an honest reason beats no number.

`fresh` and `stale_since` were removed in 0.2.0: `fresh` is `stale_reason`
absent, and `stale_since` duplicated `updated_at`. Deploy 0.2.0 `claude-usage`
and `watchdog` before the 0.2.0 daemon — a 0.1.0 client cannot decode a 0.2.0
snapshot, while a 0.2.0 client reads a 0.1.0 snapshot unchanged.

The `auth` value distinguishes four states, each with a different fix:

- `ok` — a live token; readings are current.
- `stale` — the access token aged out. The daemon runs the profile's CLI to renew
  it (see above); this value persists only when that did not work, or when
  `[refresh]` is off, and then the fix is to run or re-login the profile.
- `missing` — no credentials for this profile (never logged in / API-key
  session). Log in with this profile to start readings. The refresh path skips
  these: there is no refresh token to renew from, and `/login` is interactive.
- `rejected` — the token looked live but the API refused it. One CLI run is tried
  per refused token in case it can renew, but this usually needs an interactive
  re-login. The daemon remembers the rejected token and stops re-spending API
  calls on it until the credentials file changes.

The `auth` topic is written only when the value changes, and a write the broker
does not accept is not retried immediately: the daemon re-attempts it the next
time it classifies that profile's credentials — that profile's next poll cycle,
up to one `poll_interval_s` away — or at the next reconnect repair. So if a
publish is lost while the link still looks up (a stalled broker), the `auth`
topic can lag the snapshot's `auth` field for that long. The snapshot is the
newer of the two whenever they disagree; the journal logs `auth publish failed`
with the classification it could not announce.

`<prefix>/daemon/status` is the liveness check. It is the connection's last will,
so it flips to `offline` even if the daemon is killed. Without it you cannot tell
"utilization really is still 12%" from "the daemon died three hours ago at 12%".

While the broker is unreachable the daemon **pauses**: no API calls, no reset
pings, nothing new queued. A reading it cannot publish would spend rate-limit
budget for nothing, and its bookkeeping lives on the broker, so running ahead of
it only creates state a crash would lose. The journal shows one `warn` when the
pause starts and one `info` when it ends.

On reconnect the daemon repairs the tree. `daemon/status` is republished, because
the broker published the will while the connection was gone. So is every other retained
topic it owns — `auth`, `bookkeeping`, `snapshot`, `reset/next`, and the
per-window `utilization` and `resets_at` scalars, plus `daemon/throttle` — because
anything handed to the MQTT client in the window between the socket dying and the
client noticing (up to one keep-alive on a silent network partition) is discarded
rather than delivered, with no error to the daemon. None of that costs an API
call. The one exception is `snapshot` and `auth`: a profile whose credentials
this process has not yet classified is skipped. There are two ways to be in that
state — the daemon started while the broker was down and has not yet read any
credentials file, or a profile's credentials file has been unreadable on every
read since this process started — and both leave the previous process's readings
standing rather than replacing them with an auth state nobody established. A daemon that started while the broker was down also reads the retained
tree at this point: until then it is running on cold defaults it invented, and if
that re-read fails too it republishes nothing rather than overwrite the tree with
them. If the broker stops taking publishes while the repair is running — the link
drops again, or the broker stalls — the daemon stops there and resumes from that
profile: on the next tick if the link still looks up, after the next reconnect
otherwise (a new connection restarts the repair from the first profile, since
nothing published on the old one reached it). A stop or reload request also ends
the repair at the next profile, so a broker that is merely slow cannot hold the
daemon in it.

Stopping during an outage still completes in about a second — or up to about ten
seconds if the link drops in the middle of the reconnect repair, which is one
publish timeout plus the teardown; reloading takes a few, because the replacement
daemon waits out one rehydration attempt against the down broker.

### The reset ping

`reset/ping` fires once, when a 5-hour window rolls:

```json
{"profile":"personal","boundary":"2026-07-17T18:51:42Z",
 "fired_at":"2026-07-17T18:52:42Z","source":"api"}
```

**It is not retained, by design.** A retained ping would be redelivered on every
reconnect, so anything that restarts would act on a reset that happened hours
ago. If you cannot afford to miss one, connect with an MQTT5 persistent session
(`clean_start=false` plus a session expiry) and the broker will queue it for you
while you are away.

`source` matters:

* `"api"` — the API confirmed the roll. Trust it.
* `"extrapolated"` — the API was unreachable, so the boundary was projected
  forward from the last known one in 5-hour steps. **This is a guess.** The
  5-hour window anchors to first use after an idle gap, not to a fixed global
  grid, so if you were idle across the boundary the real window starts when you
  next make a request. Treat an extrapolated ping as a hint, not a fact.

The ping and the retained `reset/next` update fire one minute after the
boundary, configurable via `reset_settle_s` (default 60s). The API reports the
roll before the new window actually accepts requests; firing early wakes a
consumer straight back into the throttled old window and wastes the wake-up.

`reset/next` is the retained companion: the next expected boundary and its
`source`, so a consumer arriving mid-window can schedule locally without waiting
for the event. During the settle interval `reset/next` still reports the
just-passed boundary — a consumer may see a past timestamp for up to a minute
before the roll is announced; this is intentional.

## Cadence and the rate limit

Two limits decide when a profile is polled:

* **Per-profile cadence** — `(99 - utilization)` minutes, clamped to
  `[60s, 480s]`. Relaxed when there is headroom, tightening toward the floor as
  the 5-hour window approaches its cap. Cadence follows the 5-hour window only —
  it is the only window that moves fast enough for poll frequency to matter.
  Weekly caps are still published, they just do not drive cadence.
  At **100%** the reading cannot change before the window resets, so the next
  poll is due at the reset boundary instead (still clamped to `[60s, 480s]`,
  so a boundary hours away is polled at the ceiling like a cold profile). If
  the boundary is unknown, the floor applies.
* **Global spacing** — a floor between any two API calls across *all* profiles.

The global one exists because Anthropic does not document the limit and we do not
know whether it is per-credential or per-IP. Assuming per-credential is the
expensive guess: two profiles at 60s each would double the request rate from one
address. So spacing is global, and with N hot profiles the effective per-profile
floor is `min_api_interval_s * N`.

Since the real limit is unknown, the throttle adapts: every 429 doubles the
spacing (up to `backoff_max_s`), and it takes several consecutive good polls to
halve it back. Widening is fast, relaxing is slow — sitting slightly too slow is
cheap; re-entering the penalty box is not.

A profile with no usable token never spends an API call at all, and never queues
behind the global gate — reading a credentials file is free.

### Which profile gets the slot

The spacing affords one API call at a time; the scheduler picks among the
profiles that are due by **earliest deadline first**, where a profile's deadline
is `last_poll_at + poll_interval_s` and does not move until it is actually
polled. A profile that has never been polled has no deadline and goes ahead of
everyone; ties break in config order.

A polled profile's deadline is re-based on the *poll time*, so it lands strictly
after every deadline that is currently due and cannot be a candidate again until
everyone else who was waiting has been served. With N profiles, a profile waits
at most N slots — `N * spacing_s` — between falling due and being polled; longer
while a backoff is widening the spacing, which is the throttle doing its job.
Hot profiles ask for slots more often and so get proportionally more of them,
but they cannot monopolise the budget.

## State lives in MQTT

The daemon has **no local state file**. Its bookkeeping — last poll time, current
cadence, backoff level, reset tracker — goes to retained topics, and on startup
it subscribes and rehydrates before polling anything.

That is what makes restarts free: `last_poll_at` comes off the broker, so the
scheduler counts from when the API was *really* last called rather than from
process start. A unit crash-looping every few seconds cannot turn into a request
flood, which is the failure mode that got us rate-limited to begin with. It also
means a reimaged workstation picks up exactly where it left off. The global
spacing clock (`last_api_at` on `daemon/throttle`) is retained too, so a restart
within one spacing window waits out the remainder before its first call.

A restart publishes **nothing** for a profile until this process has attempted to
measure that profile — read its credentials, or called the API for it. Until
then its retained snapshot stands exactly as the previous process left it, which
is still the most recent thing anyone knows. A restart does not change the age of
the data, and `updated_at` already says how old it is.

Because the state is the tree, the tree must mirror the config: on startup the
daemon clears the retained topics of any profile that is no longer in its config.
**Run one daemon per prefix** — two daemons sharing a prefix already overwrite
each other's `daemon/status` (one last will per connection) and `daemon/throttle`,
and now they would clear each other's profiles as well.

## Reading the data

Anything that speaks MQTT can subscribe to the topic tree above. For consumers
that would rather not re-encode the topics and payload structs, two crates in
this workspace share the daemon's own definitions:

- **`claude-usage-mqtt-core`** — the topic builder/parser and payload schemas.
  The daemon and the client both depend on it, so the wire format cannot drift.
- **`claude-usage-mqtt-client`** — a small client library (`UsageClient` with
  `next_event` / `fetch_retained`, and a typed `Event` per topic) plus a
  `claude-usage` CLI for shell scripts:

  ```sh
  claude-usage snapshot          # retained tree -> JSON
  claude-usage get WINDOW        # bare utilization number, e.g.
                                 #   claude-usage get five_hour
  claude-usage watch             # events as JSON lines, forever
  claude-usage wait-reset        # block until the reset ping (exit 0),
                                 #   124 on timeout
  claude-usage watchdog          # alert JSON lines as thresholds are crossed
  ```

  Connection flags mirror the daemon's `[mqtt]` config (`--host`, `--port`,
  `--tls`, `--username`, `--prefix`); the broker password comes only from
  `CLAUDE_USAGE_MQTT_PASSWORD`, never a flag.

### Which profile a command acts on

  Every subcommand acts on a single profile by default: the one the calling
  shell is already inside, derived from `CLAUDE_CONFIG_DIR` (see
  [`scripts/claude-profile`](#running-multiple-logins-claude-profile) for the
  convention that sets it). Two global flags override that:

  - `--profile <name>` — act on this profile explicitly (must be topic-safe,
    `[A-Za-z0-9._-]`).
  - `--all` — drop the profile filter and act across every profile.

  With neither flag, the profile is read from `CLAUDE_CONFIG_DIR`:

  - unset, empty, or `~/.claude` → `default`;
  - `<profiles>/<name>` (where `<profiles>` is `$CLAUDE_PROFILES_DIR`, else
    `~/.claude-profiles`) → `<name>`;
  - anything else → the profile is **unknown**, and the command exits 2 asking
    for `--profile` or `--all`.

  Per subcommand: `snapshot` and `watch` filter their output to the scoped
  profile (`--all` restores the unfiltered tree); `get` requires a single
  profile (`--all` is an error); `wait-reset` waits for the scoped profile's
  ping (`--all` = the first ping from any profile); `watchdog` guards the
  scoped profile(s), and under `--all` its alert lines carry the profile.

  Exit codes follow `timeout(1)`: `0` ok, `1` connection/usage error, `2`
  profile/argument error (undeterminable current profile, invalid `--profile`,
  or `get --all`), `124` timeout.

### `watchdog` — threshold alerts under a supervisor

  `watchdog` subscribes and emits one JSON line per event as utilization
  crosses configured thresholds, so a supervising process (e.g. Claude Code's
  Monitor tool) knows when to wind down and wait for reset. It reads from MQTT,
  so there is no API polling or rate-limit backoff — the broker pushes every
  daemon reading. It runs until killed.

  ```sh
  claude-usage watchdog                        # defaults: thresholds 90/94/97, window five_hour
  claude-usage watchdog --threshold 80 --threshold 95
  claude-usage watchdog --window five_hour --window seven_day
  ```

  `--threshold` (percent, repeatable) replaces the built-in `90 94 97` tiers
  entirely; each must be finite and in `(0, 100]`. `--window` (repeatable)
  selects which window keys to guard; default `five_hour`.

  Line shapes (all `type`-tagged JSON):

  ```json
  {"type":"watchdog","state":"armed","profile":"default","window":"five_hour","utilization":42.0}
  {"type":"usage_alert","profile":"default","window":"five_hour","utilization":94.2,"threshold":94,"resets_at":"2026-07-18T21:00:00Z"}
  {"type":"usage_reset","profile":"default","boundary":"2026-07-18T21:00:00Z","source":"api","fired_at":"..."}
  {"type":"watchdog","state":"degraded","reason":"daemon_offline"}
  {"type":"watchdog","state":"degraded","profile":"default","reason":"stale_data","updated_at":"2026-07-18T16:12:00Z","stale_reason":"..."}
  {"type":"watchdog","state":"recovered","profile":"default","reason":"fresh_data"}
  ```

  An `armed` line confirms the watchdog is live once the first reading for each
  scoped (profile, window) arrives. `usage_alert` fires when utilization reaches
  a threshold and again on each higher integer percent; `threshold` is the
  highest configured tier at or below the reading, so a consumer can key
  severity off it. `usage_reset` fires on the daemon's reset ping (`source`
  `api`/`extrapolated`), or — if the non-retained ping was missed — is inferred
  when utilization drops below half of the last alerted value (`source`
  `inferred`, no `boundary`). `degraded`/`recovered` lines are edge-triggered
  from broker facts: daemon offline (LWT), a profile's auth leaving `ok`, or a
  snapshot going stale (`stale_reason` present); stale readings still feed the alert
  rule. A `stale_data` line carries the snapshot's `updated_at` and its
  `stale_reason`; `updated_at` keeps its payload meaning — when the carried
  values were read, or, for a profile with no reading at all, when the error was
  observed.

### Running multiple logins: `claude-profile`

  [`scripts/claude-profile`](scripts/claude-profile) runs Claude Code under
  separate logins that share history and config. Each profile gets its own
  `CLAUDE_CONFIG_DIR` (`~/.claude-profiles/<name>`, or `$CLAUDE_PROFILES_DIR`)
  holding a private `.credentials.json`, while the shareable state
  (sessions, settings, plugins, skills) symlinks back to the canonical
  `~/.claude`. `claude-profile run <name>` launches Claude with that dir set,
  which is exactly the `CLAUDE_CONFIG_DIR` convention `claude-usage` reads to
  scope commands to the current profile.

  ```sh
  scripts/claude-profile create <name>    # make a profile
  scripts/claude-profile login  <name>    # launch to run /login
  scripts/claude-profile run    <name>    # run claude under that profile
  scripts/claude-profile list             # profiles + which account each is in
  scripts/claude-profile path   <name>    # print the CLAUDE_CONFIG_DIR
  ```

  With no flags the CLI reads its broker section from a config file, so a
  co-located install just works:

  ```
  claude-usage snapshot     # no flags: discovers the config file
  ```

  Discovery order (first that exists wins; a file that exists but fails to
  parse is a hard error, never a silent fall-through):

  1. `--config <path>` — an explicit file; must exist.
  2. `~/.config/claude-usage-mqtt/client.toml` — a client-only install
     (see `client.example.toml`): just an `[mqtt]` table.
  3. `~/.config/claude-usage-mqtt/config.toml` — the daemon's own config,
     for the co-located case; only its `[mqtt]` section is read.

  Precedence is defaults < config file < flags: a flag overrides the file,
  the file overrides the built-in defaults. `--tls false` forces plaintext.
  The password still comes only from `CLAUDE_USAGE_MQTT_PASSWORD` or, when a
  config file sets `password_file`, that file — never a flag. The file's
  `client_id` is deliberately ignored; the CLI always uses a fresh
  per-invocation id so it cannot evict the running daemon from the broker.

## Install

```sh
make rust-check     # fmt, clippy, tests (needs mosquitto for the e2e suite)
make install        # ~/.local/bin + systemd --user unit
cargo install --locked --path crates/claude-usage-mqtt-client  # the claude-usage CLI

mkdir -p ~/.config/claude-usage-mqtt
cp config.example.toml ~/.config/claude-usage-mqtt/config.toml
$EDITOR ~/.config/claude-usage-mqtt/config.toml

# Broker password — never goes in the config file.
echo 'CLAUDE_USAGE_MQTT_PASSWORD=...' > ~/.config/claude-usage-mqtt/env
chmod 600 ~/.config/claude-usage-mqtt/env

claude-usage-mqtt --check      # validate config + report each profile's token
systemctl --user enable --now claude-usage-mqtt
journalctl --user -u claude-usage-mqtt -f
```

`--check` makes no API call and no broker connection; it just proves the config
parses and tells you which profiles have live tokens.

### Reloading

```sh
$EDITOR ~/.config/claude-usage-mqtt/config.toml
claude-usage-mqtt --check
systemctl --user reload claude-usage-mqtt
```

A reload (SIGHUP) is an in-process restart: the daemon stops, reconnects, and
rehydrates from the retained topics, so it costs no API call a restart would not
have made. `daemon/status` blips `offline` then `online` for the length of one
connect. A config that fails to load or validate is rejected with a log line and
the running one keeps going — which is why `--check` first is only a courtesy to
yourself.

Deploying to another host over ssh: `make deploy DEPLOY_HOST=user@host`.

## Broker setup

Needs a user with write access to `<prefix>/#`. Mosquitto ACL:

```
user claude-usage
topic write claude/usage/#
topic read claude/usage/#
```

Read access is required, not optional: the daemon subscribes to its own
bookkeeping topics to rehydrate on startup.

## Development

```sh
make check        # what the pre-commit hook runs: rust-check + brenn-scrub init --check
make rust-check   # what CI runs: fmt, clippy, tests
cargo test        # unit + e2e
```

The e2e tests spawn a real mosquitto on an ephemeral port and mock the usage
endpoint in-process, so they never touch `api.anthropic.com` and can assert on
the exact number of API calls made. They skip if mosquitto is not installed or will
not start; under CI (`CI` set) they fail instead of quietly skipping.
