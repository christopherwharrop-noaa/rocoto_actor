# frozen_string_literal: true

require "minitest/autorun"
require_relative "../lib/rocoto_actor"
require_relative "support/example_actor"
require_relative "support/process_actor"

class ActorBrokerTest < Minitest::Test
  def setup
    @broker = RocotoActor::ActorBroker.new
    @database = @broker.spawn(ExampleActor, "database")
    @worker = @broker.spawn(ForwardingActor, @database)
  end

  def teardown
    @broker&.stop(timeout: 1)
  end

  def test_worker_can_call_shared_actor_through_serialized_handle
    assert_equal "database: write", @worker.ask("write").value(timeout: 1)
    assert_equal "database: read", @database.ask("read").value(timeout: 1)
  end

  def test_worker_receives_target_actor_errors
    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask(:fail).value(timeout: 1)
    end

    assert_equal "RocotoActor::RemoteError", error.remote_class
  end

  def test_handle_has_an_opaque_serialized_form
    payload = RocotoActor::Transport.dump(@database)

    assert_operator payload.bytesize, :<, 200
    refute_includes payload, "Mutex"
    refute_includes payload, "Thread"
  end

  def test_stopped_target_returns_actor_stopped_error
    @database.stop(force: true)

    assert_raises(RocotoActor::RemoteError) do
      @worker.ask("after stop").value(timeout: 1)
    end
  end

  def test_brokered_call_timeout_rejects_a_hanging_target
    error = assert_raises(RocotoActor::RemoteError) do
      @worker.ask(message: :hang, timeout: 0.05).value(timeout: 1)
    end

    assert_equal "RocotoActor::AskTimeoutError", error.remote_class
  end

  def test_broker_stop_is_idempotent_and_rejects_new_actors
    assert @broker.stop(force: true)
    assert @broker.stop(force: true)

    assert_raises(RocotoActor::ActorStoppedError) do
      @broker.spawn(ExampleActor, "too late")
    end
  end
end