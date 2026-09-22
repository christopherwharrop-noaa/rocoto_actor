# RocotoActor

RocotoActor runs each actor in a separate operating-system process and communicates over a Unix domain socket pair. A blocked actor therefore cannot block the application's Ruby VM.

```ruby
# worker.rb
class Worker
  def receive(message)
    system(*message.fetch(:command))
  end
end
```

```ruby
# application.rb
require "rocoto_actor"
require_relative "worker"

broker = RocotoActor::ActorBroker.new
worker = broker.spawn(Worker)
future = worker.ask(command: ["long-running-command"])

# Continue serving other work, then collect the result when needed.
result = future.value(timeout: 30)
broker.stop
```

`RocotoActor::ActorBroker` is the only way to create actors. It owns every actor process, socket, and lifecycle thread in the application process and hands out `RocotoActor::ActorHandle` values; there is no lower-level spawn API.

## Constructor arguments

Arguments after the actor class are passed to the actor's `initialize` method in the new process:

```ruby
# report_worker.rb
class ReportWorker
  def initialize(output_directory, format)
    @output_directory = output_directory
    @format = format
  end

  def receive(report)
    # Generate the report using @output_directory and @format.
  end
end
```

```ruby
# application.rb
require "rocoto_actor"
require_relative "report_worker"

worker = broker.spawn(ReportWorker, "/var/reports", :pdf)
```

Constructor arguments must use supported transport values. When automatic source discovery is unavailable, pass `source:` separately from the constructor arguments:

```ruby
worker = broker.spawn(
  ReportWorker,
  "/var/reports",
  :pdf,
  source: "/path/to/report_worker.rb"
)
```

Constructor arguments, messages, and return values are serialized as JSON. Supported Ruby values are `nil`, booleans, strings, integers, finite floats, symbols, arrays, and hashes composed of those values. Other objects, invalid UTF-8, oversized frames, cycles, and excessive nesting raise `RocotoActor::SerializationError`; define an application-specific conversion to and from supported values instead of passing domain objects directly. The tagged JSON representation preserves symbols and hash key types without invoking Ruby deserialization hooks.

Actors process one message at a time. `ask` serializes and places a message in an in-process outbound mailbox, then returns a `RocotoActor::Future` without waiting for socket capacity. The mailbox is limited to 1,000 messages and 16 MiB by default. Use `broker.spawn(Worker, mailbox_size: 100, mailbox_bytes: 4 * 1024 * 1024)` to change either limit. `ask` raises `RocotoActor::MailboxFullError` rather than blocking when either limit is reached.

`Future#value` blocks only the calling thread and accepts an optional timeout. A timeout is terminal for that future: subsequent calls raise the same `RocotoActor::AskTimeoutError`, and a later actor response is discarded. Timing out does not cancel queued or executing work. Unexpected actor or transport failure rejects all unresolved futures with `RocotoActor::ActorStoppedError`.

`stop` rejects new messages, drains messages already sent to the actor, and waits up to five seconds for the whole actor process group to exit. Set a different failsafe with `stop(timeout: 30)`. If graceful draining exceeds the deadline, the process group is sent `KILL` and unresolved futures fail with `RocotoActor::ActorStoppedError`. Use `stop(force: true)` to skip draining. The method returns `true` when the process group is confirmed gone and `false` when the deadline expires. Signals cannot terminate a process while it remains in uninterruptible `D` state.

Actors run in fresh Ruby processes. The actor class must be named and defined in a dedicated file that can be loaded independently without starting the application or performing other process-wide side effects. `spawn` normally locates the defining file automatically, or it can be specified with `broker.spawn(Worker, source: "/path/to/worker.rb")`. The startup exchange has a five-second deadline by default; use `start_timeout:` to change it.

Only the actor's Unix socket is passed into the new process, avoiding inherited Ruby locks, threads, connections, and unrelated file descriptors. Standard input, output, and error are connected to `/dev/null`; actors should return diagnostics through the protocol or use an explicitly configured logging destination.

Each actor has a small watchdog process that remains a normal child of the application and owns a dedicated process group containing the actor worker. The watchdog detects worker or application death and terminates the group, including subprocesses launched by the actor; it holds no restart policy, which belongs to the broker. Actors are not daemonized and do not call `setsid`. A subprocess that deliberately creates another session or process group escapes this containment.

## Shared actors

Handles are opaque and can be passed to other actors as constructor arguments or in messages; only the handle ID crosses an actor boundary.

```ruby
broker = RocotoActor::ActorBroker.new
database = broker.spawn(DatabaseActor, "app.db")
worker = broker.spawn(WorkerActor, database)

result = worker.ask(:write).value(timeout: 5)
broker.stop
```

An actor can call a shared handle with `handle.call(message, timeout:)` or send it a one-way message with `handle.tell(message)` (see "Tell"). Calls are routed through the broker and do not transfer socket descriptors between actors. Brokered handles are stable logical identities, but brokered calls are at-most-once and an actor failure can leave the operation outcome ambiguous. Do not retry non-idempotent operations without an application-level request ID and deduplication policy.

`call` blocks the calling actor until the broker answers, so two actors that synchronously call each other deadlock until their timeouts expire. Messages sent to the calling actor while it waits are processed afterwards in arrival order.

The broker owns every brokered deadline. A call with no `timeout:` uses the broker's `route_timeout:` (30 seconds by default), and the broker answers every accepted request exactly once with a result, a `RocotoActor::AskTimeoutError`, or another error. Errors that cross actor boundaries keep their original class, message, and backtrace: the caller sees a `RocotoActor::RemoteError` whose `remote_class` names the class raised in the target actor, or `RocotoActor::ActorStoppedError` when the target is gone, or `RocotoActor::Error` with message `unknown actor handle` for a handle the broker does not own.

### Logical hierarchy

Every actor process is a direct child of the application; parent/child structure is broker metadata. Pass `parent:` and `name:` to place an actor under another:

```ruby
workers = broker.spawn(SupervisorActor, name: "workers")
worker_a = broker.spawn(WorkerActor, name: "a", parent: workers)

worker_a.path      # => "workers/a"
worker_a.parent    # => workers
workers.children   # => [worker_a]
worker_a.state     # => :running
```

Names must be unique among a parent's live children and default to the handle ID. `handle.state` is one of `:starting`, `:running`, `:restarting`, `:stopping`, `:stopped`, or `:failed`. A child never outlives its parent: `handle.stop` stops the actor's live descendants first, deepest first, then the actor, under a single shared deadline, and `broker.stop` does the same for every subtree. When an actor process exits without being asked to stop, its node becomes `:failed` and its descendants are force-stopped from the broker's service thread. Messages to a stopped node raise `RocotoActor::ActorStoppedError`; messages to a failed node raise `RocotoActor::ActorFailedError`, a subclass, so callers can distinguish a crash from an orderly shutdown. Brokered callers in other actors see the same class in `RemoteError#remote_class`. A stopped or failed handle stays known to the broker and never resolves to another process.

### Tell

`ask` is a request/reply protocol for the application edge. Between actors, prefer `tell`: it delivers a message that expects no reply, and replies come back as ordinary messages that the sender's `receive` handles like any other. Nothing blocks, and no future has to be waited on inside an actor.

```ruby
# coordinator_actor.rb
require_relative "worker_actor"

class CoordinatorActor
  def initialize(size)
    @workers = Array.new(size) { |i| RocotoActor.context.spawn(WorkerActor, name: "worker-#{i}") }
    @results = []
  end

  def receive(message)
    case message
    when :start then @workers.each { |worker| worker.tell(:work) }
    when :results then @results
    when Hash then @results << message[:result] if message[:op] == :done
    end
  end
end

# worker_actor.rb
class WorkerActor
  def receive(message)
    RocotoActor.context.sender.tell(op: :done, result: perform(message))
  end
end
```

`handle.tell(message)` works in the application and inside actors. It returns `nil` once the message is in the target's mailbox and never waits for it to be processed; it raises `RocotoActor::MailboxFullError`, `RocotoActor::ActorStoppedError`, `RocotoActor::ActorRestartingError`, or `RocotoActor::ActorFailedError` (as `RemoteError#remote_class` inside an actor) when the message could not be enqueued. Delivery is at-most-once: an accepted message can still be lost if the target dies before processing it. Messages from one sender to one target arrive in the order they were sent, whether told or asked.

During `receive`, `RocotoActor.context.sender` is the handle of the actor that sent the current message (told or called), or `nil` when it came from the application; `sender.tell` is the reply path. The return value of `receive` is discarded for a told message.

Because no reply can carry an exception, an unhandled exception while processing a told message ends the actor: the process exits, the broker records the error as `handle.last_failure` (a `RemoteError`), and the restart policy decides what happens next. Whatever ends an actor's process on its own, the watchdog reports how: `handle.last_exit` is a `RocotoActor::ExitStatus` with `exitstatus` or `termsig` (`signaled?`, and `to_s` such as `killed by signal 9 (KILL)`), retained across a restart. It is `nil` while the actor runs and after an orderly `stop`. A signal delivered to the worker ends it by that signal, so an OOM kill or an external `TERM` is reported as such rather than masked as a normal exit. With the default policy the actor stays `:failed`; with `restart: :on_failure` it is relaunched and later tells are processed by the new incarnation. Messages that were in its mailbox are not replayed.

### Restart policies

By default a crashed actor stays `:failed`; nothing is restarted or retried on the application's behalf, because a brokered call is at-most-once and the crash may have happened after a non-idempotent effect. Opt in per actor:

```ruby
database = broker.spawn(DatabaseActor, "app.db", name: "database",
                        restart: :on_failure, max_restarts: 3, restart_window: 60, restart_backoff: 0.1)
```

With `restart: :on_failure`, an actor whose process exits without being stopped is relaunched with the same handle, path, and name, and `handle.generation` increases. Its live descendants are force-stopped first; a restarted actor recreates its children in `initialize` like any fresh instance. Relaunch waits `restart_backoff` seconds, doubling for each consecutive restart within `restart_window`; more than `max_restarts` failures within the window leave the actor `:failed`, and a relaunch whose `initialize` fails counts as another failure. While an actor is `:restarting`, messages fail immediately with `RocotoActor::ActorRestartingError` (brokered callers see it in `remote_class`), and requests that were queued or in flight when the process died are rejected with `RocotoActor::ActorStoppedError` and never replayed. Stopping a restarting actor cancels the relaunch. `context.spawn` accepts the same four options.

### Spawning children from an actor

Inside an actor, `RocotoActor.context` is an opaque capability to the owning broker. `context.spawn` asks the broker to create a logical child; the child process is owned by the application like any other, and the actor receives only a handle. `context.handle` is the actor's own handle, which can be passed to children so they can reach their parent.

```ruby
# supervisor_actor.rb
require_relative "worker_actor"

class SupervisorActor
  def initialize(pool_size)
    @workers = Array.new(pool_size) do |index|
      RocotoActor.context.spawn(WorkerActor, RocotoActor.context.handle, name: "worker-#{index}")
    end
  end

  def receive(message)
    @workers.map { |worker| worker.call(message, timeout: 30) }
  end
end
```

Inside an actor a handle supports `call` and `stop` only. `ask` returns a `RocotoActor::Future` and is an application-side API; calling it, or `state`, `children`, and the other broker queries, inside an actor raises `RocotoActor::Error` naming the method and the alternative. `context.spawn` accepts `name:`, `source:`, `start_timeout:`, `mailbox_size:`, and `mailbox_bytes:`; it blocks until the child is ready or fails, and a boot failure is raised as `RocotoActor::RemoteError` with the child's error class. Children may be spawned from `initialize` as well as from `receive`; an actor is registered with the broker in a `:starting` state while its constructor runs, and nested initialization (children that spawn grandchildren in their own constructors) does not tie up broker threads. If `initialize` raises, the actor is discarded along with any children it already spawned, and the spawner sees the constructor's error. `broker.roots` lists the live top-level actors. An actor can stop handles of its own descendants and nothing else. Spawn and stop requests run on a small pool of broker threads (`max_lifecycle_workers:`, default 2) with a bounded queue (`max_pending_lifecycle_requests:`, default 100); a request beyond the queue fails with `RocotoActor::BrokerBusyError`.

Routing is bounded and does not create a thread per request. `RocotoActor::ActorBroker.new(max_routes: 1_000, max_routes_per_actor: 100)` limits requests awaiting a target across the broker and unwritten responses owed to one actor. A request beyond `max_routes` fails with `RocotoActor::BrokerBusyError`; an actor at `max_routes_per_actor` is not read from until its responses drain, without affecting other actors.

If the application exits, the watchdog detects it within 100 milliseconds and terminates the actor group independently of the worker's state. A process blocked in uninterruptible kernel sleep remains until the kernel operation returns, but the application does not wait for it.

## Security boundary

RocotoActor is a reliability bulkhead, not a sandbox for hostile code. Actor workers run under the application's user identity and inherit its environment, working directory, resource limits, and filesystem and network permissions. Only run trusted actor code. Use operating-system controls such as containers, service accounts, namespaces, sandbox profiles, or restricted environment variables when actors require a security boundary.

## Development

```sh
bundle install
bundle exec rake test
```