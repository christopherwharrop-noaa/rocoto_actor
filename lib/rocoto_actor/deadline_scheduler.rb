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

    # Returns true when the expiration is armed on a live thread, :stopped when
    # the scheduler has been stopped, or false when its thread cannot be
    # created. In the last two cases the caller must fail the work itself
    # rather than let it wait without a deadline.
    def schedule_expiration(key, timeout, &block)
      deadline = monotonic_time + timeout
      @mutex.synchronize do
        return :stopped if @stopped

        @expirations[key] = [deadline, timeout, block]
        armed = start_thread_locked
        @expirations.delete(key) unless armed
        @condition.signal
        armed
      end
    end

    def cancel_expiration(key)
      @mutex.synchronize { @expirations.delete(key) }
    end

    # Returns true when the task will run, :stopped when the scheduler has been
    # stopped, or false when its thread cannot be created.
    def enqueue(delay: 0, &block)
      @mutex.synchronize do
        return :stopped if @stopped

        entry = [monotonic_time + delay, block]
        @tasks << entry
        armed = start_thread_locked
        @tasks.delete(entry) unless armed
        @condition.signal
        armed
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

    # Caller holds @mutex. Returns true when a live thread will run the queued
    # work. A thread that cannot be created (RLIMIT_NPROC) is reported, never
    # raised into the caller.
    def start_thread_locked
      return true if @thread&.alive?

      @thread = Thread.new { run }
      @thread.report_on_exception = false
      @thread.name = "rocoto-actor-broker" if @thread.respond_to?(:name=)
      true
    rescue ThreadError => error
      report_error(error, "starting the scheduler thread")
      false
    end

    def report_error(error, context)
      @error_handler.call(error, context)
    rescue Exception # rubocop:disable Lint/RescueException
      nil
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
