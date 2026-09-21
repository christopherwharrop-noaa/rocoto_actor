# Linux Validation Handoff

## Objective

Perform a fresh adversarial review and Linux-specific validation of RocotoActor. Treat the implementation and existing tests as untrusted. Report findings before changing production code.

The library exists to keep an application responsive when an actor or a subprocess launched by an actor blocks indefinitely, including Linux uninterruptible sleep (`D` state). It is a reliability bulkhead, not a security sandbox.

## Current architecture

- `RocotoActor.spawn` creates a Unix socket pair and starts a fresh Ruby supervisor with `Process.spawn`.
- The supervisor is a normal child of the application and a process-group leader. It forks one actor worker before starting threads.
- The actor worker loads the actor's dedicated source file, constructs it from JSON-serialized arguments, and processes one message at a time.
- Only the actor socket is passed across `exec`; standard input, output, and error are mapped to `/dev/null`.
- The parent has a bounded outbound mailbox and separate reader, writer, and reaper threads.
- Mailbox limits apply to both message count and encoded bytes.
- `ask` returns a future without waiting for socket capacity. A full mailbox raises `MailboxFullError`.
- A future timeout is terminal and does not cancel queued or executing actor work.
- Graceful stop drains accepted messages. Force stop and graceful timeout send `KILL` to the actor process group.
- `stop` returns `true` only when the whole process group is observed gone before its deadline; otherwise it returns `false`.
- The supervisor polls for application death every 100 milliseconds and kills its process group if the application or actor worker exits.

Primary implementation files:

- `lib/rocoto_actor.rb`: spawning, startup protocol, transport deadline, process-group helpers, actor loop
- `lib/rocoto_actor/runner.rb`: supervisor and actor worker lifecycle
- `lib/rocoto_actor/reference.rb`: mailbox, reader/writer/reaper threads, stopping, process-group status
- `lib/rocoto_actor/future.rb`: result, error, and terminal timeout state
- `lib/rocoto_actor/transport.rb`: framed tagged-JSON codec

## Accepted limitations

- `SIGTERM` and `SIGKILL` do not take effect while a process remains in `D` state. The parent application must nevertheless stay responsive and `stop` must return `false` at its deadline.
- A subprocess can escape process-group containment by deliberately creating a new session or process group.
- Actors run with the application's UID, environment, working directory, resource limits, and filesystem/network access.
- A timed-out future does not cancel work. Late responses are discarded.
- Actor source files must be independently loadable and free of application-startup side effects.

## Evidence collected on macOS

The latest complete run passed:

```text
30 runs, 66 assertions, 0 failures, 0 errors, 0 skips
```

Additional bounded probes passed:

- 50 concurrent ask/stop iterations with 10,000 total request attempts, no blocked threads or changing future outcomes.
- 20 repetitions of worker death while a descendant inherited the actor socket.
- 20 repetitions of application parent death while an actor-owned background child was running.
- No leaked test-created processes after those runs.
- Gem build included all library files, including the runner.

Do not treat this macOS evidence as proof of Linux behavior.

## Required Linux review

Start by recording:

```sh
uname -a
ruby -v
bundle -v
git rev-parse HEAD
git status --short
```

Install dependencies and run the baseline:

```sh
bundle install
bundle exec rake test
gem build rocoto_actor.gemspec
rm -f rocoto_actor-*.gem
```

Then perform a fresh code review without using the list of previously fixed bugs as a checklist. Model these state machines explicitly:

1. Request: created, serialized, admitted, queued, writing, delivered, replied, timed out, rejected.
2. Reference: running, draining, terminating, supervisor exited, process group gone.
3. Processes: application, supervisor, worker, actor descendants, escaped descendants.
4. Transport: empty, partial header, partial body, full frame, EOF, malformed frame, blocked reader/writer.

Report findings with severity, file and line, triggering interleaving, impact, and a proposed correction. Run a bounded reproducer before labeling timing-dependent findings as confirmed.

## Linux fault matrix

Exercise at least these cases with hard outer time limits and cleanup traps:

### Concurrency

- Many threads call `ask` while another calls graceful `stop`.
- Many threads call `ask` while another calls force stop.
- Multiple concurrent graceful and forceful stops.
- Replies race exactly with `Future#value` timeout.
- Actor exits while requests are queued, writing, executing, and replying.
- Mailbox count and byte limits are reached concurrently.
- Repeat randomized lifecycle stress for thousands of actor instances or until resource limits make that impractical.

For every accepted request, verify its future reaches exactly one permanent terminal state. Verify no caller remains blocked after the test deadline.

### Process lifecycle

- Kill the worker with `TERM`, `KILL`, and ordinary exit while a descendant retains the socket.
- Kill the supervisor while the worker is idle and while it is busy.
- Kill the application with normal exit, `TERM`, and `KILL` while the actor is idle, busy, and has descendants.
- Launch descendants that ignore `TERM`; verify group `KILL` removes them.
- Confirm escaped sessions/process groups are the only documented descendant escape.
- Verify actors and supervisors are reaped and do not accumulate as zombies.
- Verify `alive?` and `stop` do not confuse unrelated recycled PIDs or process groups under PID churn.
- Verify startup timeout remains bounded when source loading or initialization hangs and ignores `TERM`.

Inspect process state with commands such as:

```sh
ps -eo pid,ppid,pgid,sid,state,wchan:32,etime,cmd
cat /proc/PID/status
cat /proc/PID/wchan
cat /proc/PID/stack
```

Reading `/proc/PID/stack` may require elevated permissions. Do not route passwords or credentials through the agent.

### Transport and resources

- Fill the Unix socket buffer while the actor is not reading; `ask` must remain bounded and force stop must remain callable.
- Inject partial headers, partial payloads, EOF, malformed JSON, unknown tags, oversized declared frames, invalid UTF-8, deep nesting, cycles, huge integers, and unsupported values.
- Verify malformed responses reject pending futures or stop the actor without blocking the application.
- Measure parent memory while filling the mailbox near both limits. Confirm retained memory is bounded to an acceptable multiple of `mailbox_bytes`.
- Test file-descriptor exhaustion, process/thread exhaustion, and failure to create the socket, supervisor, worker, reader, writer, or reaper.
- Verify no descriptors other than the intended socket and `/dev/null` standard streams cross `exec`.

### Linux `D` state

Use a disposable environment and a safe, reproducible blocked-I/O fixture if one is available. Do not intentionally disrupt host storage or production mounts.

Verify while the worker is in `D` state:

- The parent application continues executing and serving unrelated work.
- Other actors remain responsive.
- `Future#value(timeout:)` reaches a permanent `AskTimeoutError` state.
- `stop(timeout:)` returns `false` near its deadline rather than hanging.
- Reader, writer, reaper, and supervisor behavior does not block application shutdown.
- Mailbox limits still prevent parent memory growth.
- Once the kernel wait releases, pending `KILL` takes effect and the process group disappears.

An Ubuntu container under Docker Desktop uses the Docker Linux VM kernel. It is suitable for Linux process, `/proc`, signal, and socket behavior, but may not reproduce the storage-driver, block-device, or NFS failure modes of the deployment host. Repeat the `D`-state scenario on a disposable environment representative of production if those subsystems are central to the threat model.

### Security

- Confirm the socket is not filesystem-addressable and only intended descriptors cross `exec`.
- Review environment-variable and source-path handling for injection or accidental secret exposure.
- Fuzz the framed JSON decoder and verify memory, nesting, and frame limits are enforced before dangerous allocation.
- Verify symbol decoding cannot cause unacceptable memory growth on every supported Ruby version.
- Confirm malformed remote error fields cannot instantiate arbitrary classes or execute deserialization hooks.
- Document that same-UID actor code can access application-readable resources and signal application processes; this library does not defend against malicious actor code.

## Supported Ruby versions

The gem currently declares Ruby `>= 3.1`. At minimum, run the full suite on Ruby 3.1 and the newest supported Ruby. Include another maintained intermediate version if practical. Differences in `Process.spawn`, `fork`, JSON, `Thread`, and signal behavior are especially relevant.

## Completion criteria

- Findings are reported before production fixes are made.
- Every confirmed serious finding has a deterministic or bounded regression test.
- Full tests pass on the tested Linux/Ruby matrix.
- Stress runs finish without blocked Ruby threads, zombies, or leaked actor descendants.
- Parent responsiveness is demonstrated during a real or representative blocked-I/O scenario.
- Any remaining limitation is explicit in `README.md` and accepted before release.

## Suggested prompt for the container session

```text
Read README.md and docs/linux-validation.md. Perform the fresh Linux-specific
adversarial review described there. Treat the implementation as untrusted.
Run bounded tests and fault injection, but report findings before changing
production code. Do not create a dangerous D-state fixture on shared or
production storage.
```