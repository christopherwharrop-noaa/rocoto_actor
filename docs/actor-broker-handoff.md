# ActorBroker Implementation Handoff

## Purpose

This document preserves the design discussion and current implementation state for adding process-isolated actor handles and a parent-owned actor broker to RocotoActor. It is intended for a future Copilot, Claude Code, or other coding session.

## Repository and baseline

- Repository: `/home/admin/rocoto_actor`
- Runtime used for validation: Ruby 3.4.10 on Linux (linuxkit)
- Existing actor model: one watchdog process group per actor, one Unix socket pair between the application and that actor, parent-side reader/writer/reaper threads, bounded outbound mailbox.
- The previously identified concurrent `Reference#stop` race was fixed in `lib/rocoto_actor/reference.rb` and covered by `test_concurrent_stop_waits_for_existing_shutdown`.
- Before the broker prototype, the suite passed with 35 runs and 70 assertions. The concurrent ask/stop and timeout/load stress probes passed.

## Core design decisions

### Process isolation remains primary

Actors must continue to run in separate operating-system processes. Do not move toward a shared-thread actor runtime. The bulkheading requirement is the reason for RocotoActor.

### Scale: a handful of actors, never thousands

Process-based actors are expensive to create and run; that cost is accepted because separate processes are the only way to bulkhead Ruby code given the GVL. The practical population is a handful of actors per application. Do not design for thousands: retained terminal nodes, per-actor threads (reader, writer, reaper), and per-restart process creation are all fine at this scale. Thread-based actors were considered and deliberately left out; a hung thread-based actor could hang the application, which defeats the purpose of the library.

### Idempotency keys and message ids are application design

Deduplication has to live in the receiver, recorded atomically with the effect (same transaction) when the effect is in a transactional store, and pushed to the downstream system's own idempotency mechanism when it is not. Message ids for correlating a `tell` reply with its request are likewise part of the application's protocol. The library carries no key or id slot in the envelope and never retries.

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

`ActorHandle#call` is documented as a convenience with deadlock risk. A one-message-at-a-time actor can deadlock:

```text
A synchronously waits for B
B synchronously waits for A
```

The long-term preferred API is asynchronous `handle.ask`, returning a future that can be integrated with the actor loop. `call` should either be documented as a convenience with deadlock risk or replaced with a nonblocking continuation/message pattern.

## Current prototype files

- `lib/rocoto_actor/broker.rb`: `ActorBroker` registry and lifecycle nodes, bounded non-blocking routing, the service thread (route expiries and failure cleanup), and the lifecycle pool for actor-initiated spawn/stop.
- `lib/rocoto_actor/handle.rb`: `ActorHandle`; in the application it delegates to the broker, in an actor it sends requests through `BrokerClient`.
- `lib/rocoto_actor/broker_client.rb`: worker-side per-socket request channel with unique request IDs and deferred frames.
- `lib/rocoto_actor/context.rb`: `ActorContext`, exposed as `RocotoActor.context` inside an actor (`spawn`, `handle`).
- `lib/rocoto_actor.rb`: loads the library and holds worker-side accessors (`worker_process?`, `context`, `broker_client`); no public spawn.
- `lib/rocoto_actor/launcher.rb`: internal (`private_constant`) process launcher (`launch` returns `[reference, boot_future]`), startup error mapping, and process-group helpers used by `ActorBroker` and `Reference`.
- `lib/rocoto_actor/runner.rb`: worker entry point; marks the worker process, builds the context from the boot message, marks the broker client ready after `:ready`, and runs the actor loop (`run_actor` drains deferred frames and flattens `RemoteError` provenance in `error_response`).
- `lib/rocoto_actor/future.rb`: `on_resolve` callbacks and `expire` used by the broker to answer without a waiting thread.
- `lib/rocoto_actor/transport.rb`: encodes handles as `['actor_handle', id]` and decodes them using the current transport thread's socket.
- `lib/rocoto_actor/reference.rb`: accepts broker requests from an actor and queues broker responses on the actor's existing writer, with an `on_done` callback per response for broker accounting.
- `test/support/process_actor.rb`: contains `ForwardingActor` used by the broker test.
- `test/support/supervisor_actor.rb`: `SupervisorActor`, `InitSpawnActor`, and `FailingInitSpawnActor` exercising `RocotoActor.context`.
- `test/support/tell_actor.rb`: `CollectorActor` and `CoordinatorActor` exercising `tell`, `context.sender`, and failure in told messages.
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

The existing transport is a request/reply stream, so broker responses and ordinary actor replies need correlation and must not be confused. The worker side uses the response `op`, serializes calls under a per-handle mutex, and defers any non-response frame read during a call (`RocotoActor.defer_frame`) so the actor loop processes it next in arrival order. The `timeout` field must be sent inside one explicit hash: `Transport.write(io, timeout:)` is the write deadline, and the prototype's keyword form silently dropped it.

Error provenance is flattened rather than nested: `error_response` and the broker both forward `remote_class`, `remote_message`, and `remote_backtrace` of a `RemoteError`, so a caller two hops away still sees `remote_class == "ArgumentError"`.

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
- [x] Bound broker routing work with a finite dispatcher/request capacity (`max_routes`, `max_routes_per_actor`, `route_timeout`; no thread per route; one timer thread per broker).
- [x] Flatten `RemoteError` provenance across hops; typed errors for unknown handle, stopped target, invalid timeout, and broker capacity.
- [x] Fix asks arriving during a brokered `call` being dropped by the waiting handle.
- [x] Add logical parent/child lifecycle metadata (`ActorBroker::Node`: id, name, path, generation, parent_id, children, state, reference; `spawn(parent:, name:)`; recursive stop; failure propagation; `ActorFailedError`).
- [x] Add broker-owned child spawning (`RocotoActor.context.spawn`, `:broker_spawn`/`:broker_stop`, bounded lifecycle pool, per-socket `BrokerClient` with unique request IDs).
- [x] Add `tell` (one-way messages, broker-acked enqueue, sender handle in the envelope, `context.sender`, failure on unhandled exception with `handle.last_failure`).
- [x] Add restart policies and generations (`restart: :never | :on_failure`, `max_restarts`, `restart_window`, `restart_backoff`; `:restarting` state; `ActorRestartingError`; `handle.generation`).

The broker-focused command was run:

```sh
bundle exec ruby -Itest test/broker_test.rb
```

Latest validation (2026-09-22):

```text
broker: 66 runs, 280 assertions, 0 failures, 0 errors
full suite: 102 runs, 353 assertions, 0 failures, 0 errors
```

The outer caller sees the innermost remote class: a target `ArgumentError` arrives as `RemoteError` with `remote_class == "ArgumentError"` and `remote_message == "requested failure"`; a broker deadline arrives as `remote_class == "RocotoActor::AskTimeoutError"`; a stopped target as `"RocotoActor::ActorStoppedError"`; an over-capacity broker as `"RocotoActor::BrokerBusyError"`; an unknown handle as `"RocotoActor::Error"` with message `unknown actor handle`.

Remove generated `Gemfile.lock` if it is untracked and was created only by local validation.

### Routing design (implemented)

- `ActorBroker#route` runs on the source actor's reader thread and never waits on the target: it registers `Future#on_resolve` on the target future and returns.
- The response is written on the source `Reference` writer thread via `send_broker_response(..., on_done:)`; `on_done` fires when the frame is written or discarded (source stopped), releasing the per-source response slot.
- One `rocoto-actor-broker-timer` thread per broker expires target futures at their deadline (`Future#expire`), so a hanging target costs no thread.
- `max_routes` (global in-flight routes) rejects with `BrokerBusyError`; `max_routes_per_actor` (unwritten responses per source) blocks that source's reader only, which stops reading that actor's socket until responses drain.
- A source that stops with routes pending has its responses discarded by `discard_control_outbox`, which releases the route slots; the target's work is not cancelled.
- `ActorHandle#call` applies no local read timeout: the broker always answers, and a partial frame read abandoned on timeout would desynchronize the stream.

### Lifecycle design (implemented)

- `ActorBroker::Node` holds `id, name, path, generation (always 1 until restart exists), parent_id, children (all child ids, including terminal ones), state, reference`. All node state is guarded by the broker mutex. Nodes are never removed, so a stale handle answers with a typed error and can never resolve to a recycled process.
- States: `:starting`, `:running`, `:stopping`, `:stopped`, `:failed`. A node is registered as `:starting` immediately after the process is launched and before its boot completes, so the actor can issue broker requests (including `context.spawn`) from `initialize`. `check_placement` is run before launch (cheap rejection) and again under the mutex at registration, so a parent stopped during the child's startup makes the child fail with `ActorStoppedError` and the child process is force-stopped.
- Local handles no longer hold a `Reference`; every `ask`/`stop`/`alive?`/`state`/`path`/`parent`/`children` goes through the broker (`ActorHandle.new(id, broker:)`), which checks node state first. `ActorHandle#==` compares ids.
- Stop order: descendants first, deepest first (post-order), then the node; one deadline shared across the subtree (`stop_subtrees`), falling back to `force: true` once the deadline passes. `broker.stop` applies the same to every root.
- Failure: `Reference#on_exit` fires on the reaper thread. `actor_exited` marks the node `:stopped` if it was `:stopping`, else `:failed`, marks live descendants `:stopping`, and enqueues their force-stop on the broker service thread (the former timer thread now runs expiries and lifecycle tasks). The reaper thread never waits on another actor.
- Names: unique among live siblings; may be reused after the sibling stops or fails. `children(handle)` returns only live children.
- `ActorFailedError < ActorStoppedError` distinguishes a crash from an orderly stop for both local callers and brokered callers (`remote_class`).

### Child spawning design (implemented)

- Worker side: `RocotoActor.context` (`ActorContext`, `lib/rocoto_actor/context.rb`) is created by the runner from the boot message's `context: { actor_id: }`, which `ActorBroker` passes through a new `RocotoActor.spawn(context:)` keyword. `context.spawn(Class|"Name", *args, name:, source:, start_timeout:, mailbox_size:, mailbox_bytes:)` resolves the source path in the worker and sends `{ op: :broker_spawn, actor_class:, source:, arguments:, name:, options: }`. `context.handle` is the actor's own handle.
- The process primitive is `RocotoActor::Launcher.spawn` (`lib/rocoto_actor/launcher.rb`), a `private_constant` called only by `ActorBroker#spawn_node` for both `broker.spawn` and `context.spawn`. There is no public `RocotoActor.spawn`; `ActorBroker` is the only way to create actors. The launcher accepts a class name string plus `source:`, so the broker never needs the child class loaded. `test/rocoto_actor_test.rb` reaches it through `RocotoActor.const_get(:Launcher)` to test `Reference` directly.
- `BrokerClient` (`lib/rocoto_actor/broker_client.rb`) is one per socket in the worker; all handles and the context share its mutex and request counter, which closes the request-ID collision item. It holds the deferred non-response frames that `run_actor` drains. Requests are valid from the first frame because the parent's reader thread services the socket from the moment the process is launched.
- Broker side: `Reference#read_replies` forwards every op in `ActorBroker::REQUEST_OPS` to `ActorBroker#dispatch`, which takes the per-source response slot then routes `:broker_request` inline and queues `:broker_spawn`/`:broker_stop` on a lifecycle pool (`max_lifecycle_workers`, lazily started; `max_pending_lifecycle_requests` queue bound rejecting with `BrokerBusyError`). Spawn blocks up to `start_timeout`, so it must not run on a reader thread or the service thread.
- The source node is found through `@node_ids_by_reference`; an unregistered source gets `Error("unknown source actor")`. `spawn_child` validates types and restricts `options` to `SPAWN_OPTIONS` with numeric values; `source` must be absolute.
- Worker-side `handle.stop(timeout:, force:)` sends `:broker_stop`; the broker only allows stopping descendants of the requester (`descendant?`), else `Error("... is not a descendant of ...")`.
- Handles decoded in the application bind to the owning broker (`Thread.current[:rocoto_actor_broker]`, set on the reference reader thread by `attach_broker`), so a handle returned from an actor's reply is a fully local handle. Decoding in a worker (`RocotoActor.worker_process?`) binds to the socket as before.
- `ActorBroker#stop` fails any queued lifecycle requests with `ActorStoppedError` and joins the lifecycle workers; a spawn in progress completes and is rejected at registration because the broker is stopped.

### Boot protocol (implemented)

- Boot is an ordinary correlated request. `Launcher.launch` forks, builds the `Reference` immediately, and calls `Reference#boot(arguments, context)`, which enqueues `{ op: :boot, id:, arguments:, context: }` (bypassing the mailbox bound) and returns `[reference, boot_future]`. The runner replies `{ id:, ok: true }` after `initialize` returns, or an `error_response` with the boot id on failure; a watchdog failure before the boot message is read sends a bare `op: :boot_error` frame, which the reader resolves against `@boot_id`. `Launcher.spawn` (launch + wait) remains for the low-level `Reference` tests only.
- `ActorBroker#launch_node` registers the node as `:starting` (with `booting = true`), attaches the broker, and returns `[node, boot]`. Exactly one caller settles it via `settle_boot(node, error)`: success moves `:starting` to `:running`; failure kills the process (`stop(force: true, timeout: 0)`), `unregister`s the node (removed from `@nodes` and the parent's children, since no handle can exist for it), force-stops any children it spawned while booting on the service thread, and maps the error with `Launcher.startup_error` (`AskTimeoutError` becomes `TransportTimeoutError` "startup timed out"; `ActorStoppedError` becomes `Error` "closed during startup"; `RemoteError` passes through with the constructor's original class).
- `broker.spawn(start_timeout:)` waits synchronously on the boot future. `spawn_child` (lifecycle pool) does not wait: it schedules an expiry for `start_timeout` on the service thread and responds from `boot.on_resolve`, so a chain of actors spawning in their constructors needs no thread per level (`test_nested_initialization_spawning_is_not_limited_by_the_lifecycle_pool`).
- `:starting` sources may issue broker requests and may be spawn parents (`Node#active?`); `:starting` targets accept routed messages, which queue until the actor loop starts. An actor that passes `context.handle` to a child during its own `initialize` and has that child call back synchronously deadlocks until a timeout, as documented for synchronous calls generally.
- `ActorBroker#roots` returns handles of live top-level actors.

### Tell design (implemented)

- Envelope: every delivered message is `{ op: :ask | :tell, message:, sender: }` (`id:` only for asks). `sender` is an `ActorHandle` encoded by id, or nil from the application; the runner sets `RocotoActor.context.sender` around `receive` (`Runner.deliver`) for both ops.
- Application side: `handle.tell` → `ActorBroker#tell` → `Reference#tell`, which enqueues without a pending future (`enqueue(reply: false)`) and raises `MailboxFullError`/`ActorStoppedError` if not enqueued. Ordering with asks from the same sender is preserved by the single outbox.
- Worker side: `handle.tell` sends `:broker_tell`; `ActorBroker#relay_tell` runs inline on the source's reader thread (no route slot; it never waits on the target), enqueues with `sender: sender_handle(source)`, and acks with `ok: true, result: nil` or a typed error. Routed asks now also carry the sender.
- Failure: `Runner.run_actor` rescues an exception from a told `receive`, writes `{ op: :actor_error, error_class:, message:, backtrace: }`, and exits; `Reference` stores it as `exit_error`, `ActorBroker#last_failure` returns `node.failure || reference.exit_error`, and `relaunch` copies the previous reference's `exit_error` into `node.failure` before swapping. The exit then follows the normal failure path (`actor_failed`, restart policy).
- Idempotency keys were deliberately left as an application pattern (dedup must live in the receiver, atomically with the effect); no envelope slot was added. Worker-side async `ask` was rejected: a future waited on inside `receive` either blocks or delivers results outside the message flow. `tell` plus reply-as-message is the async model.

### Restart design (implemented)

- Terminology: the small per-actor process started by `runner.rb` is the watchdog (`Runner.watch`; it was called the supervisor before 2026-09-22): a process-group owner with no policy. The supervisor in the actor-system sense is the broker. An external kill of the worker is noticed by the watchdog, which kills the group and exits; the broker's reaper then runs the same failure path as a crash. Deaths without an exception (signals, `exit!`, OOM) leave `last_failure` nil but are reported through `last_exit`: the watchdog keeps its socket copy, and after `waitpid` returns for the worker it writes `{ op: :actor_exit, exitstatus:, termsig: }` before killing the group and exiting (`Runner.report_exit`); `Reference#exit_status` holds it as `RocotoActor::ExitStatus`, `ActorBroker#last_exit` returns `node.exit || reference.exit_status`, and `relaunch` carries it across generations like `last_failure`. The watchdog holding the socket delays the parent's EOF by at most one `PARENT_CHECK_INTERVAL`. Ruby converts a fatal signal into `SignalException`; `Runner.run_actor` rescues it and `die_by_signal` resets the disposition with `Signal.trap(signo, "SYSTEM_DEFAULT")` (`"DEFAULT"` would restore Ruby's own handler, which raises again) and re-delivers the signal so the status shows `termsig` instead of exit 0. An unhandled exception in a told message exits with status 1 after the `:actor_error` frame. Nothing in the restart policy depends on the exit reason; it is diagnostics only.

- Policy per node (`Node#policy`): `restart` (`:never` default, `:on_failure`), `max_restarts` (3), `restart_window` (60 s), `restart_backoff` (0.1 s, doubling per consecutive restart). `Node#spec` keeps the class, arguments, launch options, and `start_timeout` needed to relaunch. Validated by `validate_policy` for both `broker.spawn` and `context.spawn` (`POLICY_OPTIONS` are accepted in `SPAWN_OPTIONS`).
- Failure decision is centralized in `actor_failed(node)`: marks live descendants `:stopping` and force-stops them on the service thread, then `restart_delay` (under the mutex) prunes restart timestamps outside the window, records the attempt, and returns the backoff or nil. With a delay the node becomes `:restarting` and a delayed service task (`enqueue_task(delay:)`) hands `relaunch(node)` to the lifecycle pool via `enqueue_lifecycle_job` (internal jobs bypass the request bound and are bounded by the number of nodes). Without one the node becomes `:failed`.
- `relaunch` launches under the same id (`context: { actor_id: }`), then under the mutex re-checks `:restarting` before installing the new reference and bumping `generation`; if the node was stopped meanwhile the new process is killed and discarded. `stop_subtrees` snapshots each node's reference at marking time for the same reason.
- Exit callbacks are bound to the reference they came from (`actor_exited(node, reference)`) so a late reaper from a previous generation cannot fail the current one; exits during any boot (`node.booting`) are owned by the boot settler (`settle_boot` for first boots, `settle_restart` for relaunches, whose failure feeds back into `actor_failed` and counts against the limit).
- Requests to a `:restarting` node fail fast with `ActorRestartingError` (`node_error`, hence also brokered routes). "Wait" and "queue" policies from the plan were not implemented; fail-fast is the only behavior. No request is ever replayed: `Reference` rejects queued and in-flight futures with `ActorStoppedError` when the process exits.
- `Reference#alive?` is false for a `:restarting` node between generations.

## Production-readiness review (2026-09-22)

A `/code-review high lib/` pass produced ten findings. Fixed, each with a regression test:

1. A relaunch whose `Launcher.launch` raised left the node `:restarting` forever; it now goes through `actor_failed` like any other failure.
2. `last_failure`/`last_exit` preferred the previous generation's values; they now prefer the current reference's, falling back to the carried-over ones.
3. `run_actor` only rescued `StandardError`, so `NotImplementedError`/`LoadError` in `receive` (and any protocol error) killed the actor with exit 0 and no report. Per-message rescues cover `ScriptError`; the method-level path reports anything else with `:actor_error` and exits 1; `SystemExit` exits with its status.
4. `Future#value`'s timeout branch did not broadcast, leaving other waiters on the same future asleep forever.
5. The reaper closed the socket under the reader mid-frame, losing the watchdog's `:actor_exit` frame. `actor_exited` now rejects pending futures, kills the group (which produces EOF for the reader), joins the reader for up to a second, then closes.
6. A process that died after replying ready but before `settle_boot` cleared `booting` was dropped (state `:running`, never restarted). `actor_exited` records `node.boot_exit` during a boot and the settlers call `actor_failed` when they see it.
9. `unregister` left a failed boot's children with a dangling `parent_id`: `descendant?` and `parent` now tolerate it, and `broker.stop` sweeps every node rather than only roots so such children are stopped.
10. `mailbox_size`/`mailbox_bytes` were validated only after the process was spawned; the launcher validates first.

Documented, not fixed:

7. While an actor is blocked in `BrokerClient#request` it reads and defers every incoming `:ask`/`:tell` without bound; the application-side mailbox limits bound only the unsent outbox, so a caller that floods an actor during its long `call` grows that actor's memory. Acceptable at the intended scale; a credit-based flow control or a bound that fails the call would be the fix if needed.
8. Terminal nodes are retained forever and root spawns scan all nodes. Node metadata retention is accepted per the scale decision above; the soak showed each terminal node also pinned its `Reference` (thread stacks, closed socket) and `spec` (constructor arguments), so `retire(node, state)` now copies `exit_error`/`exit_status` onto the node and releases the reference and spec at every terminal transition. `ask`/`tell` and the boot settlers read the reference under the mutex because a concurrent `retire` can null it.

A `/code-review ultra` pass (2026-09-22, standard diff review; the launch note asking for lock-ordering and settlement-race focus was recorded but not delivered to the reviewers) produced four findings, all fixed with tests:

1. Exponential backoff defeated the restart cap: timestamps older than `restart_window` were pruned, and once a doubling delay exceeded the window the count could never reach `max_restarts`, so a persistently crashing actor restarted forever (defaults were safe; `restart_backoff: 1, max_restarts: 10` was not). `restart_delay` now counts consecutive failures and resets only when the previous incarnation ran for `restart_window` seconds (`node.started_at`, set when an incarnation becomes `:running`); time spent in backoff never counts. README wording updated.
2. Internal relaunch jobs shared `@lifecycle_queue` and counted against `max_pending_lifecycle_requests`; the bound now counts only client requests.
3. `report_failure`/`report_boot_error` dropped the report when the error itself could not be serialized (invalid UTF-8 in a message); `write_report` falls back to reporting the `SerializationError`, matching the ask path.
4. A local named `roots` in `ActorBroker#stop` held every node; renamed `nodes`.

The lock-ordering and settlement-race review the note asked for has therefore only had the `/code-review high` pass; a follow-up review with that focus, or a targeted manual read of `actor_exited`/`actor_failed`/`settle_boot`/`settle_restart`/`relaunch`/`stop_subtrees` against `Reference#stop`/`actor_exited`, is still worthwhile.

Soak harness: `test/soak/soak.rb` (`SOAK_SECONDS=1800 bundle exec ruby -Ilib test/soak/soak.rb`) runs continuous ask/tell traffic with injected crashes, external kills, told-message exceptions, stops, and respawns; samples threads/fds/children/zombies/RSS/live heap slots and live `Reference`/`Future`/`ActorHandle` counts after `GC.start`; and fails on upward trends, a zombie persisting across consecutive samples, children surviving `broker.stop`, `broker.stop` returning false, or unexpected error classes. It logs any `Reference#stop` that returns false with its pid and elapsed time. It is not part of `rake test`.

Soak results (2026-09-22, this container): a 10-minute run (~900k operations, ~250 injected failures) showed threads and file descriptors flat during the run and back to baseline after `broker.stop`, no surviving children, and only expected error classes. Findings: a single-sample zombie (the normal exit-to-`waitpid` window; the check now requires persistence), `Reference`/`Future` counts growing with terminal nodes (fixed by `retire`), and one `broker.stop` returning false while every process was gone a second later, which did not reproduce in later runs; `StopDiagnostics` in the harness will name the reference if it recurs. After `retire`, a 3-minute run passed every check: live `Reference` count flat at 15–17 and zero after `broker.stop`, `Future` count flat, `heap_live_slots` flat, and RSS climbing only during the first minute (24→36 MB) before plateauing at ~37 MB, so the earlier steady RSS growth was terminal-node retention plus heap warm-up rather than a leak. A multi-hour run before production is still worthwhile for the `broker.stop` false return that did not reproduce.

## Immediate implementation plan

### 1. Stabilize the protocol (remaining items)

- Define protocol constants or helpers for `broker_request` and `broker_response`.
- Done: timeout field, flattened error provenance, typed errors for unknown handle and invalid timeout, request IDs unique per source socket (`BrokerClient`).

### 2. Make routing bounded (done)

See "Routing design (implemented)" above. Still open: a byte budget for control responses (they currently bypass the mailbox byte limit but are bounded in count by `max_routes_per_actor`).

### 3. Add logical lifecycle metadata (done)

See "Lifecycle design (implemented)" above. The original requirements are kept below for reference. Tracked fields:

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

### 4. Add broker-owned child spawning (done)

See "Child spawning design (implemented)" above. Original sketch:

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

### 5. Define shutdown and restart policies (done)

See "Restart design (implemented)" above. The original requirements are kept for reference:

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

- [x] handle encoding contains only an opaque ID and never socket/thread/mutex state
- [x] worker routes a successful request to a shared target
- [x] target remote error has defined nested/flattened semantics
- [x] unknown handle returns a typed error
- [x] stopped target returns a typed error
- [x] source actor stopping with a pending broker call does not leak a route (`test_stopped_source_releases_its_route`)
- [x] target actor stopping with a pending broker call resolves the caller promptly with `ActorStoppedError`
- [x] concurrent requests from multiple actors preserve target actor serialization (5 sources × 8 calls, unique sequence numbers)
- [x] broker stop is idempotent, rejects new actors, and stops every subtree
- [x] new routes are rejected after broker stop
- [x] logical hierarchy: path/parent/children, name validation and sibling uniqueness
- [x] stopping a parent stops descendants; stopping a child leaves the parent running
- [x] failed parent marks `:failed` and stops descendants; failed target reports `ActorFailedError` to brokered callers
- [x] subtree stop shares one deadline
- [x] actor spawns a child through its context; child reaches its parent through `context.handle`
- [x] stopping the parent stops children spawned by the actor; actor stops its own child; non-descendant stop refused
- [x] child boot failure reported to the actor; spawn during initialization rejected
- [x] malformed spawn requests and unknown sources fail closed; lifecycle requests are bounded
- [x] children spawned from `initialize`; nested constructor spawning with a pool of one; failed constructor unregisters the actor and its children, reported to both the application and a spawning actor
- [x] tell: application tell ordered with asks and without sender; actor fan-out with tell and reply via `context.sender`; routed ask carries sender; tell then call from one actor arrive in order; tell to stopped target rejected for application and actors; exception in told message fails the actor and is recorded in `last_failure`; exception in told message triggers restart policy
- [x] `ask` inside an actor raises with guidance
- [x] actor killed from outside (SIGKILL to the worker) is restarted under `:on_failure` and fails under the default policy; the per-actor watchdog process holds no policy
- [x] backoff longer than the window cannot defeat `max_restarts`; healthy uptime resets the count; relaunch jobs do not consume the lifecycle request budget; unserializable crash messages are still reported
- [x] exit reasons: `last_exit` reports termsig for SIGKILL and SIGTERM, exitstatus for `exit!` and told-message exceptions (with `last_failure`), nil while running and after stop, retained across restart
- [x] restart: same handle and path with new generation; restarted actor recreates children; requests during restart fail fast locally and via broker; restart limit leaves `:failed`; stop during backoff cancels relaunch; policy from `context.spawn`; policy validation; default policy does not restart
- [x] malformed broker requests fail closed (invalid timeout)
- [x] broker request limits remain bounded (`max_routes`, `max_routes_per_actor`, no thread per route)
- [x] synchronous call deadlock behavior is documented (README); asks arriving during a call are not dropped

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
