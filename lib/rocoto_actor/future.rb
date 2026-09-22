# frozen_string_literal: true

module RocotoActor
  class Future
    def initialize(&on_timeout)
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @on_timeout = on_timeout
      @resolved = false
      @callbacks = []
    end

    def value(timeout: nil)
      deadline = timeout && Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      loop do
        timed_out = @mutex.synchronize do
          if @resolved
            raise @error if @error

            return @result
          end

          remaining = deadline && deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          if remaining && remaining <= 0
            @error = AskTimeoutError.new("actor did not reply within #{timeout} seconds")
            @resolved = true
            true
          else
            @condition.wait(@mutex, remaining)
            false
          end
        end
        next unless timed_out

        @on_timeout&.call
        run_callbacks
        raise @error
      end
    end

    def ready?
      @mutex.synchronize { @resolved }
    end

    # Registers a block called once with (result, error) when the future resolves.
    # The block runs on the resolving thread, or immediately if already resolved.
    def on_resolve(&block)
      resolved = @mutex.synchronize do
        @callbacks << block unless @resolved
        @resolved
      end
      block.call(@result, @error) if resolved
      self
    end

    # Resolves the future with AskTimeoutError, as if value(timeout:) had expired.
    def expire(timeout)
      expired = @mutex.synchronize do
        next false if @resolved

        @error = AskTimeoutError.new("actor did not reply within #{timeout} seconds")
        @resolved = true
        @condition.broadcast
        true
      end
      return false unless expired

      @on_timeout&.call
      run_callbacks
      true
    end

    def fulfill(result)
      resolve(result, nil)
    end

    def reject(error)
      resolve(nil, error)
    end

    private

    def resolve(result, error)
      @mutex.synchronize do
        return if @resolved

        @result = result
        @error = error
        @resolved = true
        @condition.broadcast
      end
      run_callbacks
    end

    def run_callbacks
      callbacks = @mutex.synchronize do
        values = @callbacks
        @callbacks = []
        values
      end
      callbacks.each { |callback| callback.call(@result, @error) }
    end
  end
end