# frozen_string_literal: true

module RocotoActor
  # Internal ordered delivery of application lifecycle events and watcher
  # notifications. Actor state is owned by ActorBroker; this class owns only
  # watcher IDs, the delivery queue, and its event thread.
  class EventDispatcher
    def initialize(error_handler:, &deliver)
      @error_handler = error_handler
      @deliver = deliver
      @watchers = {}
      @events = Queue.new
      @thread = nil
    end

    def watch(watched_id, watcher_id)
      (@watchers[watched_id] ||= {})[watcher_id] = true
    end

    def remove_watch(watched_id, watcher_id) # rubocop:disable Naming/PredicateMethod
      !@watchers[watched_id]&.delete(watcher_id).nil?
    end

    def watcher_ids(node_id)
      @watchers.fetch(node_id, {}).keys
    end

    def emit(node_id, event, detail, terminal:, watcher_ids: nil, notify_application: true)
      ids = watcher_ids || self.watcher_ids(node_id)
      @watchers.delete(node_id) if terminal
      enqueue(node_id, event, detail, ids, notify_application)
    end

    def purge(node_id)
      @watchers.each_value { |watchers| watchers.delete(node_id) }
    end

    def stop
      @events.close
      @thread
    end

    private

    def enqueue(node_id, event, detail, watcher_ids, notify_application)
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
    end

    def report_error(error, context)
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end
  end
  private_constant :EventDispatcher
end
