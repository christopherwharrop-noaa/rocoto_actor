# frozen_string_literal: true

module RocotoActor
  # Internal bounded executor for lifecycle operations that may block on
  # process creation or shutdown. It owns workers and queueing only; the
  # broker owns actor state and request semantics.
  class LifecycleExecutor
    Request = Struct.new(:source, :request, :release_response)

    def initialize(max_workers:, max_pending_requests:, error_handler:, request_error:, &request_handler)
      @max_workers = max_workers
      @max_pending_requests = max_pending_requests
      @error_handler = error_handler
      @request_error = request_error
      @request_handler = request_handler
      @queue = []
      @workers = []
      @idle_workers = 0
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @stopped = false
    end

    def enqueue_request(source, request, release_response)
      @mutex.synchronize do
        return ActorStoppedError.new("actor broker is stopped") if @stopped

        waiting = @queue.count { |job| job.is_a?(Request) }
        if waiting >= @max_pending_requests
          return BrokerBusyError.new("actor broker has #{@max_pending_requests} lifecycle requests waiting")
        end

        @queue << Request.new(source, request, release_response)
        start_worker_locked
        @condition.signal
        nil
      end
    end

    def enqueue_job(&block)
      @mutex.synchronize do
        return false if @stopped

        @queue << block
        start_worker_locked
        @condition.signal
        true
      end
    end

    def stop
      workers, requests = @mutex.synchronize do
        @stopped = true
        @condition.broadcast
        queued = @queue
        @queue = []
        [@workers.dup, queued.grep(Request)]
      end
      workers.each(&:join)
      requests
    end

    private

    def start_worker_locked
      return unless @idle_workers.zero? && @workers.size < @max_workers

      worker = Thread.new { run_worker }
      worker.report_on_exception = false
      worker.name = "rocoto-actor-broker-lifecycle" if worker.respond_to?(:name=)
      @workers << worker
    end

    def run_worker
      loop do
        job = @mutex.synchronize do
          @idle_workers += 1
          @condition.wait(@mutex) while @queue.empty? && !@stopped
          @idle_workers -= 1
          @stopped ? nil : @queue.shift
        end
        return unless job

        begin
          if job.is_a?(Request)
            @request_handler.call(job.source, job.request, job.release_response)
          else
            job.call
          end
        rescue Exception => error # rubocop:disable Lint/RescueException
          report_error(error, "lifecycle job")
          @request_error.call(job.source, job.request, error, job.release_response) if job.is_a?(Request)
          raise unless error.is_a?(StandardError)
        end
      end
    ensure
      @mutex.synchronize { @workers.delete(Thread.current) }
    end

    def report_error(error, context)
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end
  end
  private_constant :LifecycleExecutor
end
