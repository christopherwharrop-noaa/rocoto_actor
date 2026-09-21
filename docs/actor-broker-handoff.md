# ActorBroker Implementation Handoff

## Purpose

This document preserves the design discussion and current implementation state for adding process-isolated actor handles and a parent-owned actor broker to RocotoActor. It is intended for a future Copilot, Claude Code, or other coding session.

## Repository and baseline

- Repository: `/home/admin/rocoto_actor`
- Runtime used for validation: Ruby 3.4.10 on Linux 6.12.76-linuxkit
- Existing actor model: one supervisor process group per actor, one Unix socket pair between the application and that actor, parent-side reader/writer/reaper threads, bounded outbound mailbox.
- The previously identified concurrent `Reference#stop` race was fixed in `lib/rocoto_actor/reference.rb` and covered by `test_concurrent_stop_waits_for_existing_shutdown`.
- Before the broker prototype, the suite passed with 35 runs and 70 assertions. The concurrent ask/stop and timeout/load stress probes passed.

## Core design decisions

### Process isolation remains primary

Actors must continue to run in separate operating-system processes. Do not move toward a shared-thread actor runtime. The bulkheading requirement is the reason for RocotoActor.

### Flat OS ownership, logical actor hierarchy

The application-side broker owns every actual actor process, `Reference`, socket, reader thread, writer thread, reaper, and lifecycle decision. The OS process tree should not be used as the actor hierarchy.

Logical hierarchy is broker metadata only:

```text
ActorBroker
  logical path: database
  logical path: workers/a
  logical path: workers/a/cache
```

Cross-branch communication is routed through the same broker:

```text
workers/a -> broker -> database
workers/a -> broker -> workers/b
```

### Actor handles are opaque logical capabilities

A handle passed through transport must contain only an opaque logical actor ID or path. It must not contain a `Reference`, mutex, thread, socket, file descriptor, or process object. The broker maps the logical ID to the current parent-owned `Reference`.

This indirection is intentionally stable across future actor restarts:

```text
logical actor ID: database
current binding: generation 1 -> socket A
restart:          generation 2 -> socket B
```

### No socket descriptor transfer

Each actor gets exactly one socket pair with the parent broker at spawn time. Actor-to-actor calls send broker protocol messages over the requesting actor's existing socket. The broker performs the target `Reference#ask` and sends the result back over the source socket. Do not use `SCM_RIGHTS` or pass sockets between actors.

### Delivery semantics

The default should remain at-most-once. A failure can mean that a request was never received, was executing, completed before its response was lost, or completed and the actor died. Do not automatically retry non-idempotent database operations.

Future request IDs/idempotency keys may be added so applications can implement safe deduplication, especially for SQLite writes. Exactly-once effects are not provided by transport alone.

### Synchronous calls are dangerous

The current prototype exposes `ActorHandle#call`, but a one-message-at-a-time actor can deadlock:

```text
A synchronously waits for B
B synchronously waits for A
```

The long-term preferred API is asynchronous `handle.ask`, returning a future that can be integrated with the actor loop. `call` should either be documented as a convenience with deadlock risk or replaced with a nonblocking continuation/message pattern.

## Current prototype files

- `lib/rocoto_actor/broker.rb`: initial `ActorBroker` registry and route implementation.
- `lib/rocoto_actor/handle.rb`: initial `ActorHandle` with local parent-side `ask` and worker-side synchronous `call`.
- `lib/rocoto_actor.rb`: loads the new broker and handle files.
- `lib/rocoto_actor/transport.rb`: currently encodes handles as `['actor_handle', id]` and decodes them using the current transport thread's socket.
- `lib/rocoto_actor/reference.rb`: accepts broker requests from an actor and queues broker responses on the actor's existing writer.
- `test/support/process_actor.rb`: contains `ForwardingActor` used by the broker test.
- `test/broker_test.rb`: initial end-to-end broker tests.
- `README.md`: preliminary shared-actor documentation.

## Current protocol sketch

Actor A sends over A's existing socket:

```ruby
{
  op: :broker_request,
  request_id: integer,
  handle_id: opaque_id,
  message: supported_value
}
```

The parent broker looks up `handle_id`, calls the target reference, and queues this response to A:

```ruby
{
  op: :broker_response,
  request_id: integer,
  ok: true,
  result: supported_value
}
```

Or:

```ruby
{
  op: :broker_response,
  request_id: integer,
  ok: false,
  error_class: string,
  message: string,
  backtrace: array
}
```

The existing transport is a request/reply stream, so broker responses and ordinary actor replies need correlation and must not be confused. The current prototype uses a response `op` and serializes worker-side calls under a per-handle mutex.

## Current known status

Implementation progress:

- [x] Preserve process-based actors and parent-owned socket pairs.
- [x] Add opaque transport-serializable actor handles.
- [x] Add the first parent-side broker routing path.
- [x] Add a forwarding-actor end-to-end smoke test.
- [x] Define initial broker error semantics: errors preserve provenance through nested `RemoteError` wrappers; do not reconstruct arbitrary exception classes.
- [x] Propagate per-call broker timeouts and cover a hanging target.
- [x] Run the broker suite and full suite after the current changes.
- [x] Add an initial per-call timeout field and propagate it to the target future.
- [ ] Bound broker routing work with a finite dispatcher/request capacity.
- [ ] Add logical parent/child lifecycle metadata.
- [ ] Add broker-owned child spawning.
- [ ] Add restart policies and generations.

The broker-focused command was run:

```sh
bundle exec ruby -Itest test/broker_test.rb
```

Latest validation:

```text
broker: 6 runs, 15 assertions, 0 failures, 0 errors
full suite: 41 runs, 85 assertions, 0 failures, 0 errors
```

The target actor's `ArgumentError` becomes a `RemoteError` in the target reference, then the broker serializes that error and the forwarding actor raises another `RemoteError`. The outer caller therefore sees `remote_class == "RocotoActor::RemoteError"`. This is the documented initial behavior: preserve nested provenance rather than reconstruct arbitrary exception classes. A broker call timeout is reported to the outer application as `RemoteError` with `remote_class == "RocotoActor::AskTimeoutError"`.

The prototype has not yet had a full-suite validation after the broker edits. Remove generated `Gemfile.lock` if it is untracked and was created only by local validation.

## Immediate implementation plan

### 1. Stabilize the protocol

- Define protocol constants or helpers for `broker_request` and `broker_response`.
- Decide whether broker errors preserve an error chain (`RemoteError` wrapping `RemoteError`) or carry original remote class/message/backtrace fields.
- Add request timeout/deadline fields to broker requests. The prototype now propagates an optional call timeout to the target future; route threads are still unbounded and require a bounded dispatcher.
- Make request IDs unique per source actor, or include source actor ID/generation in correlation identity.
- Validate all broker request fields and reject malformed/unknown handles with typed errors.

### 2. Make routing bounded

- Do not create an unbounded Ruby `Thread` for every broker request.
- Add a broker mailbox/request limit and bounded worker strategy, or use a dedicated broker dispatcher with explicit capacity.
- Ensure a blocked target actor does not exhaust broker threads or prevent unrelated actor sockets from being serviced.
- Ensure broker responses are included in an appropriate byte/count budget.
- Decide behavior when the source actor stops while a target request is pending; discard the response and cancel/forget the route if possible.

### 3. Add logical lifecycle metadata

Track at least:

```ruby
id
path
generation
parent_id
children
state # starting, running, stopping, stopped, failed, restarting
reference
```

Required invariants:

- The broker owns every reference.
- A logical child cannot outlive its parent unless explicit re-parenting is added.
- Stopping a parent recursively stops its descendants.
- Stopping a child revokes its handle or transitions it to a typed stopped state.
- Actor failure transitions the logical node and applies the configured subtree policy.
- Unknown/stale handles never reach an unrelated recycled process.

### 4. Add broker-owned child spawning

Do not let workers call `RocotoActor.spawn` directly. Add a broker protocol request such as:

```ruby
{
  op: :broker_spawn,
  request_id: integer,
  parent_id: source_actor_id,
  actor_class: class_name,
  source: source_path,
  arguments: supported_values,
  options: constrained_spawn_options
}
```

The broker should create the child, record parent/child metadata, and return an opaque handle. Child spawn must be bounded and must not block unrelated broker traffic.

Pass an `ActorContext` or system capability to workers if desired, but keep it as an opaque broker-facing value. Do not expose the parent socket or a direct `Reference`.

### 5. Define shutdown and restart policies

Even before restart support, failure handling needs explicit policies:

- queued requests on actor failure: fail, do not replay by default
- in-flight request on actor failure: fail as ambiguous
- pending broker calls from a stopped source: fail and discard late response
- parent stop: stop descendants recursively, then parent or vice versa; choose and test one order
- broker stop: reject new spawns/routes and wait for all process groups
- restart: stable logical ID, increment generation, replace reference/socket internally
- new requests during restart: wait, fail with `ActorRestartingError`, or queue according to policy
- restart loops: backoff and maximum attempts

### 6. Prefer async handles

Add a worker-safe asynchronous API, likely `ActorHandle#ask`, that returns a broker future without blocking inside `receive`. This requires the actor loop to support pending broker calls or a continuation/event mechanism. Do not rely on synchronous `call` for database services in general.

## Tests to add

Normal suite:

- handle encoding contains only an opaque ID and never socket/thread/mutex state
- worker routes a successful request to a shared target
- target remote error has defined nested/flattened semantics
- unknown handle returns a typed error
- stopped target returns a typed error
- source actor stopping with a pending broker call does not leak a route thread
- target actor stopping with a pending broker call resolves the caller
- concurrent requests from multiple actors preserve target actor serialization
- broker stop is idempotent and recursively stops all registered actors
- new spawns/routes are rejected after broker stop
- malformed broker requests fail closed
- broker mailbox/request limits remain bounded
- synchronous call deadlock behavior is documented or prevented

Optional Linux/constrained job:

- run the normal suite under `ulimit -n`, cgroup memory, process, and PID limits
- separately test spawn failures from unavailable descriptors/process slots
- keep hard outer timeouts and cleanup traps

Do not make genuine Linux `D`-state fixtures part of normal CI. They require disposable, representative storage/network failure environments and are unsuitable for a portable test suite.

## Public API direction

Possible target API:

```ruby
system = RocotoActor::ActorBroker.new

database = system.spawn(DatabaseActor, "app.db")
worker = system.spawn(WorkerActor, database)

future = worker.ask(:write)
result = future.value(timeout: 5)

system.stop
```

Future child API:

```ruby
# Conceptual worker-side API; not implemented yet.
child = context.spawn(ChildActor, configuration)
child.ask(message)
```

Avoid calling this full component `ActorSystem` until it has general lifecycle, dispatch, registry, and supervision behavior. `ActorBroker` accurately describes the current focused role.

## Completion criteria for this feature

- Full existing suite passes.
- Broker tests pass without unbounded threads or blocked shutdown.
- Every accepted broker request reaches exactly one terminal caller-visible outcome.
- Parent and target actor failures are distinguishable from successful results.
- Logical parent/child lifecycle invariants are tested.
- Broker handles remain stable across any future restart implementation.
- Documentation states at-most-once behavior and retry/idempotency requirements.
- No socket descriptors cross actor boundaries.
