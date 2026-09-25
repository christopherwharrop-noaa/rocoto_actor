# frozen_string_literal: true

module RocotoActor
  # Internal ordered delivery of application lifecycle events and watcher
  # notifications on one thread. Actor state, including who watches whom, is
  # owned by ActorBroker; this class owns only the queue and its thread.
  class EventDispatcher
    def initialize(error_handler:, &deliver)
      @error_handler = error_handler
      @deliver = deliver
      @events = Queue.new
      @thread = nil
    end

    def emit(node_id, event, detail, watcher_ids, notify_application: true)
      return if @events.closed?

      @events << [node_id, event, detail, watcher_ids, notify_application]
      return if @thread&.alive?

      @thread = Thread.new do
        Thread.current.report_on_exception = false
        while (queued = @events.pop)
          begin
            @deliver.call(*queued)
          rescue StandardError => error
            report_error(error, "event delivery")
          end
        end
      end
      @thread.name = "rocoto-actor-broker-events" if @thread.respond_to?(:name=)
    rescue ThreadError => error
      # The events stay queued; the next emit tries to start the thread again.
      report_error(error, "starting the event thread")
    end

    def stop
      @events.close
      @thread
    end

    private

    def report_error(error, context)
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end
  end
  private_constant :EventDispatcher
end
