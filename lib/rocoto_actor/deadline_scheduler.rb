# frozen_string_literal: true

module RocotoActor
  # Internal deadline executor for route expirations and delayed broker tasks.
  # It owns timing and a thread, but knows nothing about actor state.
  class DeadlineScheduler
    def initialize(error_handler:)
      @error_handler = error_handler
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @expirations = {}
      @tasks = []
      @stopped = false
      @thread = nil
    end

    def schedule_expiration(key, timeout, &block)
      deadline = monotonic_time + timeout
      @mutex.synchronize do
        return if @stopped

        @expirations[key] = [deadline, timeout, block]
        start_thread_locked
        @condition.signal
      end
    end

    def cancel_expiration(key)
      @mutex.synchronize { @expirations.delete(key) }
    end

    def enqueue(delay: 0, &block)
      @mutex.synchronize do
        return if @stopped

        @tasks << [monotonic_time + delay, block]
        start_thread_locked
        @condition.signal
      end
    end

    def stop
      @mutex.synchronize do
        @stopped = true
        @condition.broadcast
        return @thread
      end
    end

    private

    # Caller holds @mutex. Queued work waits for the thread; if it cannot be
    # created now (RLIMIT_NPROC), the failure is reported and the next enqueue
    # tries again.
    def start_thread_locked
      return if @thread&.alive?

      @thread = Thread.new { run }
      @thread.report_on_exception = false
      @thread.name = "rocoto-actor-broker" if @thread.respond_to?(:name=)
    rescue ThreadError => error
      @error_handler.call(error, "starting the scheduler thread")
    end

    def run
      loop do
        expirations, tasks = @mutex.synchronize do
          loop do
            break if @stopped

            now = monotonic_time
            due_expirations = @expirations.select { |_key, (deadline, _timeout, _block)| deadline <= now }
            ready, @tasks = @tasks.partition { |run_at, _block| run_at <= now }
            unless due_expirations.empty? && ready.empty?
              due_expirations.each_key { |key| @expirations.delete(key) }
              break [due_expirations.values, ready.map(&:last)]
            end

            next_deadline = (@expirations.values.map(&:first) + @tasks.map(&:first)).min
            @condition.wait(@mutex, next_deadline && (next_deadline - now))
          end
        end
        return unless expirations

        expirations.each { |_deadline, timeout, block| guarded("route expiration") { block.call(timeout) } }
        tasks.each { |task| guarded("service task") { task.call } }
      end
    end

    def guarded(context)
      yield
    rescue StandardError => error
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
  private_constant :DeadlineScheduler
end
