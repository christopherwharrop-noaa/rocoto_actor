# frozen_string_literal: true

require_relative "support/broker_test_case"
require_relative "support/supervisor_actor"

# Behaviour near the user's process limit (RLIMIT_NPROC): refusing to spawn
# before the limit is hit, and never letting thread-creation failures escape
# from broker threads or from stop.
class BrokerLimitsTest < BrokerTestCase
  PROCESS_BUDGET = RocotoActor.const_get(:ProcessBudget)
  LIFECYCLE_EXECUTOR = RocotoActor.const_get(:LifecycleExecutor)
  DEADLINE_SCHEDULER = RocotoActor.const_get(:DeadlineScheduler)
  EVENT_DISPATCHER = RocotoActor.const_get(:EventDispatcher)
  NEAR_LIMIT = { limit: 100, in_use: 70, margin: 32 }.freeze # 70 + 7 + 32 > 100

  def test_spawn_is_refused_when_one_more_actor_would_not_fit
    supervisor = @broker.spawn(SupervisorActor, name: "sup")

    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      error = assert_raises(RocotoActor::ResourceLimitError) { @broker.spawn(ExampleActor, "x", name: "x") }
      assert_match(/70 of the user's 100/, error.message)

      remote = assert_raises(RocotoActor::RemoteError) do
        supervisor.ask(op: :spawn, name: "child", arguments: ["c"]).value(timeout: 5)
      end
      assert_equal "RocotoActor::ResourceLimitError", remote.remote_class
    end

    assert_empty supervisor.children
    assert_equal "database: fine", @database.ask("fine").value(timeout: 1)
  end

  def test_preflight_is_skipped_when_disabled_or_unmeasurable
    unchecked = RocotoActor::ActorBroker.new(process_margin: nil)
    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_equal "x: ok", unchecked.spawn(ExampleActor, "x").ask("ok").value(timeout: 2)
    end
    PROCESS_BUDGET.stub(:snapshot, nil) do
      assert_equal "y: ok", @broker.spawn(ExampleActor, "y").ask("ok").value(timeout: 2)
    end
  ensure
    unchecked&.stop(timeout: 2, force: true)
  end

  def test_describe_reports_process_headroom
    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_equal NEAR_LIMIT, @broker.describe[:process_limit]
    end
    assert_nil RocotoActor::ActorBroker.new(process_margin: nil).describe[:process_limit]
  end

  def test_relaunch_refused_by_the_limit_is_reported_and_counts_as_a_failure
    reported = Queue.new
    broker = RocotoActor::ActorBroker.new(error_handler: ->(error, context) { reported << [error.class, context] })
    actor = broker.spawn(ExampleActor, "x", name: "x", restart: :on_failure, max_restarts: 1, restart_backoff: 0.01)

    PROCESS_BUDGET.stub(:snapshot, NEAR_LIMIT) do
      assert_raises(RocotoActor::ActorStoppedError) { actor.ask(:crash).value(timeout: 2) }
      wait_until { actor.state == :failed }
    end

    assert_equal [RocotoActor::ResourceLimitError, "relaunch of x"], reported.pop(timeout: 2)
    assert_equal 1, actor.generation
  ensure
    broker&.stop(timeout: 2, force: true)
  end

  def test_lifecycle_executor_fails_a_request_it_cannot_start_a_thread_for
    reported = []
    executor = LIFECYCLE_EXECUTOR.new(max_workers: 1, max_pending_requests: 10,
                                      error_handler: ->(error, context) { reported << [error.class, context] },
                                      request_error: ->(*) {}) { |*| nil }

    rejection = Thread.stub(:new, ->(*) { raise ThreadError, "simulated" }) do
      executor.enqueue_request(FakeSource.new, { request_id: 1 }, -> {})
    end

    assert_instance_of RocotoActor::ResourceLimitError, rejection
    assert_equal [[ThreadError, "starting a lifecycle thread"]], reported
    assert_equal 0, executor.pending_requests
    refute Thread.stub(:new, ->(*) { raise ThreadError, "simulated" }) { executor.enqueue_job { nil } }
  ensure
    executor&.stop
  end

  def test_scheduler_and_dispatcher_report_instead_of_raising_when_threads_are_unavailable
    reported = []
    handler = ->(error, context) { reported << [error.class, context] }
    scheduler = DEADLINE_SCHEDULER.new(error_handler: handler)
    dispatcher = EVENT_DISPATCHER.new(error_handler: handler) { |*| nil }

    Thread.stub(:new, ->(*) { raise ThreadError, "simulated" }) do
      scheduler.enqueue { nil }
      dispatcher.emit("id", :stopped, {}, [])
    end

    assert_equal [[ThreadError, "starting the scheduler thread"], [ThreadError, "starting the event thread"]], reported
  ensure
    scheduler&.stop
    dispatcher&.stop
  end
end
