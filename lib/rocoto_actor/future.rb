# frozen_string_literal: true

module RocotoActor
  class Future
    def initialize(&on_timeout)
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @on_timeout = on_timeout
      @resolved = false
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
        raise @error
      end
    end

    def ready?
      @mutex.synchronize { @resolved }
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
    end
  end
end