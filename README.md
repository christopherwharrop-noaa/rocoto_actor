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

worker = RocotoActor.spawn(Worker)
future = worker.ask(command: ["long-running-command"])

# Continue serving other work, then collect the result when needed.
result = future.value(timeout: 30)
worker.stop
```

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

worker = RocotoActor.spawn(ReportWorker, "/var/reports", :pdf)
```

Constructor arguments must use supported transport values. When automatic source discovery is unavailable, pass `source:` separately from the constructor arguments:

```ruby
worker = RocotoActor.spawn(
  ReportWorker,
  "/var/reports",
  :pdf,
  source: "/path/to/report_worker.rb"
)
```

Constructor arguments, messages, and return values are serialized as JSON. Supported Ruby values are `nil`, booleans, strings, integers, finite floats, symbols, arrays, and hashes composed of those values. Other objects, invalid UTF-8, oversized frames, cycles, and excessive nesting raise `RocotoActor::SerializationError`; define an application-specific conversion to and from supported values instead of passing domain objects directly. The tagged JSON representation preserves symbols and hash key types without invoking Ruby deserialization hooks.

Actors process one message at a time. `ask` serializes and places a message in an in-process outbound mailbox, then returns a `RocotoActor::Future` without waiting for socket capacity. The mailbox is limited to 1,000 messages and 16 MiB by default. Use `RocotoActor.spawn(Worker, mailbox_size: 100, mailbox_bytes: 4 * 1024 * 1024)` to change either limit. `ask` raises `RocotoActor::MailboxFullError` rather than blocking when either limit is reached.

`Future#value` blocks only the calling thread and accepts an optional timeout. A timeout is terminal for that future: subsequent calls raise the same `RocotoActor::AskTimeoutError`, and a later actor response is discarded. Timing out does not cancel queued or executing work. Unexpected actor or transport failure rejects all unresolved futures with `RocotoActor::ActorStoppedError`.

`stop` rejects new messages, drains messages already sent to the actor, and waits up to five seconds for the whole actor process group to exit. Set a different failsafe with `stop(timeout: 30)`. If graceful draining exceeds the deadline, the process group is sent `KILL` and unresolved futures fail with `RocotoActor::ActorStoppedError`. Use `stop(force: true)` to skip draining. The method returns `true` when the process group is confirmed gone and `false` when the deadline expires. Signals cannot terminate a process while it remains in uninterruptible `D` state.

Actors run in fresh Ruby processes. The actor class must be named and defined in a dedicated file that can be loaded independently without starting the application or performing other process-wide side effects. `spawn` normally locates the defining file automatically, or it can be specified with `RocotoActor.spawn(Worker, source: "/path/to/worker.rb")`. The startup exchange has a five-second deadline by default; use `start_timeout:` to change it.

Only the actor's Unix socket is passed into the new process, avoiding inherited Ruby locks, threads, connections, and unrelated file descriptors. Standard input, output, and error are connected to `/dev/null`; actors should return diagnostics through the protocol or use an explicitly configured logging destination.

Each actor has a small supervisor that remains a normal child of the application and owns a dedicated process group containing the actor worker. The supervisor detects worker or application death and terminates the group, including subprocesses launched by the actor. Actors are not daemonized and do not call `setsid`. A subprocess that deliberately creates another session or process group escapes this containment.

If the application exits, the supervisor detects it within 100 milliseconds and terminates the actor group independently of the worker's state. A process blocked in uninterruptible kernel sleep remains until the kernel operation returns, but the application does not wait for it.

## Security boundary

RocotoActor is a reliability bulkhead, not a sandbox for hostile code. Actor workers run under the application's user identity and inherit its environment, working directory, resource limits, and filesystem and network permissions. Only run trusted actor code. Use operating-system controls such as containers, service accounts, namespaces, sandbox profiles, or restricted environment variables when actors require a security boundary.

## Development

```sh
bundle install
bundle exec rake test
```