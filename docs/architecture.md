# Architecture

## Purpose

RocotoActor isolates each actor in its own operating-system process. The extra
processes and threads are intentional: a blocked actor must not block the
application or unrelated actors. The library is designed for a handful of
long-lived actors, not thousands of short-lived actors.

`RocotoActor::ActorBroker` is the only component that creates actors. Public
code receives opaque `ActorHandle` values; sockets, processes, and
`Reference` objects remain private to the application process.

## Process model

Each actor has this physical structure:

```text
application
  ActorBroker
    Reference
      Unix socket
      reader thread
      writer thread
      reaper thread
    watchdog process (process-group leader)
      actor worker process
      actor-created descendants
```

The watchdog kills its process group when the application or worker exits.
The broker owns restart and hierarchy policy; the watchdog only provides
process containment.

Actor parent/child relationships are logical broker metadata. Every watchdog
is a direct child of the application, so cross-branch communication and
lifecycle ownership remain centralized.

## Component responsibilities

### `ActorBroker`

Owns the logical actor registry and all cross-actor policy:

- actor paths, parents, children, states, and generations
- brokered calls and tells
- route limits, deadlines, and synchronous-call deadlock detection
- child spawning and recursive stopping
- restart policy
- watches and application lifecycle events
- actor-owned timers

The broker has one mutex protecting this state. It never calls a `Reference`
method that takes the reference mutex while holding the broker mutex.

### `Reference`

Owns one actor connection and process group:

- bounded outbound mailbox
- request IDs and pending futures
- socket reader and writer threads
- process reaping and forced termination
- broker responses sent back to the actor

All mutable connection state is protected by one reference mutex. Callbacks
run after that mutex is released.

### `Launcher` and `Runner`

`Launcher` creates the socket pair, watchdog process, and parent-side
`Reference`. `Runner` is the child entry point: it forks the actor worker,
loads the actor source, constructs the actor, and executes one message at a
time. The watchdog observes both the worker and application.

### `BrokerClient`, `ActorContext`, and handles

Inside a worker, one `BrokerClient` serializes broker requests over the
actor's existing socket. `ActorContext` exposes child spawning, watching, and
self-scheduling. `ActorHandle` and `Timer` are opaque IDs bound either to the
application broker or to the worker's broker client.

No socket descriptor or `Reference` crosses an actor boundary.

### `Transport` and `Future`

`Protocol` defines operation names and constructs request, success, failure,
and broker-response envelopes. `Transport` implements length-prefixed tagged
JSON with frame, type, cycle, and nesting validation. `Future` provides a
single permanent result, error, or timeout and invokes resolution callbacks
outside its mutex.

## Threads owned by the broker

- **Service thread:** route expiries, timer firing, delayed restart work.
- **Lifecycle pool:** blocking child spawn/stop and relaunch operations.
- **Event thread:** ordered application callbacks and watch notifications.

These remain separate because service deadlines must not be delayed by
blocking lifecycle operations or slow event consumers.

## Lock and callback rules

1. The broker mutex is never held while entering a `Reference` method that can
   take the reference mutex.
2. A `Reference` never calls the broker while holding its mutex.
3. `Future#on_resolve`, `Reference#on_exit`, and broker-response completion
   callbacks run outside locks.
4. Node state, generation, reference replacement, and hierarchy mutations are
   atomic under the broker mutex.
5. Blocking socket I/O belongs only to per-actor reader or writer threads.

See [REVIEW.md](../REVIEW.md) for the complete concurrency invariants and
interleavings that reviews must preserve.

## Delivery and failure semantics

- Delivery is at-most-once; the library never retries messages.
- A failed call can have an ambiguous side-effect outcome.
- Application protocols own idempotency and deduplication.
- Requests queued or executing when an incarnation dies are rejected and are
  never replayed after restart.
- Handles remain stable across restart; the broker changes their current
  generation and `Reference` binding.
- A logical child never outlives its parent.

## Source map

| Area | Primary files |
| --- | --- |
| Public API | `broker.rb`, `handle.rb`, `context.rb`, `timer.rb`, `future.rb` |
| Connection and process lifecycle | `reference.rb`, `launcher.rb`, `runner.rb` |
| Worker-to-broker protocol | `protocol.rb`, `broker_client.rb`, `transport.rb` |
| Errors | `errors.rb` |
| Routing tests | `test/broker_routing_test.rb` |
| Hierarchy and shutdown tests | `test/broker_lifecycle_test.rb` |
| Child-spawn tests | `test/broker_children_test.rb` |
| Restart and failure tests | `test/broker_restart_test.rb` |
| Tell tests | `test/broker_tell_test.rb` |
| Timer tests | `test/broker_timer_test.rb` |
| Event/watch tests | `test/broker_events_test.rb` |

## Validation

Run the normal quality gate with:

```sh
bundle exec rake
```

The soak and Linux fault-matrix harnesses exercise longer-running and
platform-specific behavior. Their commands and accepted limitations are in
[linux-validation.md](linux-validation.md).

The chronological design, review, and validation record remains in
[actor-broker-handoff.md](actor-broker-handoff.md). It is useful when tracing
why a decision was made, but this document is the starting point for the
current architecture.