# frozen_string_literal: true

module RocotoActor
  class Reference
    DEFAULT_STOP_TIMEOUT = 5

    attr_reader :pid

    def initialize(socket, pid, mailbox_size:, mailbox_bytes:)
      raise ArgumentError, "mailbox_size must be positive" unless mailbox_size.positive?
      raise ArgumentError, "mailbox_bytes must be positive" unless mailbox_bytes.positive?

      @socket = socket
      @pid = pid
      @mailbox_size = mailbox_size
      @mailbox_bytes = mailbox_bytes
      @next_id = 0
      @pending = {}
      @pending_mutex = Mutex.new
      @outbox = []
      @control_outbox = []
      @outbox_bytes = 0
      @writing_bytes = 0
      @outbox_condition = ConditionVariable.new
      @writer_stopped = false
      @stopped = false
      @termination_started = false
      @process_exited = false
      @group_exited = false
      @exit_condition = ConditionVariable.new
      @reaper = nil
      @reaper_mutex = Mutex.new
      @broker = nil
      start_reaper
      @reader = Thread.new { read_replies }
      @reader.name = "rocoto-actor-reader-#{pid}" if @reader.respond_to?(:name=)
      @writer = Thread.new { write_requests }
      @writer.name = "rocoto-actor-writer-#{pid}" if @writer.respond_to?(:name=)
    end

    def attach_broker(broker)
      @pending_mutex.synchronize { @broker = broker }
    end

    def send_broker_response(request_id, result: nil, error: nil, error_class: nil, message: nil, backtrace: nil)
      response = if error
                   {
                     op: :broker_response,
                     request_id: request_id,
                     ok: false,
                     error_class: error_class || error.class.name,
                     message: message || error.message,
                     backtrace: backtrace || error.backtrace || []
                   }
                 else
                   { op: :broker_response, request_id: request_id, ok: true, result: result }
                 end
      payload = Transport.dump(response)
      @pending_mutex.synchronize do
        return if @writer_stopped

        @control_outbox << payload
        @outbox_condition.signal
      end
    end

    def ask(message)
      @pending_mutex.synchronize do
        raise ActorStoppedError, "actor is stopped" if @stopped

        @next_id += 1
        id = @next_id
        future = Future.new { remove_pending(id) }
        payload = Transport.dump(op: :ask, id: id, message: message)
        raise MailboxFullError, "actor mailbox is full" unless mailbox_has_space?(payload.bytesize)

        @pending[id] = future
        @outbox << payload
        @outbox_bytes += payload.bytesize
        @outbox_condition.signal
        future
      end
    end

    def stop(timeout: DEFAULT_STOP_TIMEOUT, force: false)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      if force
        force_stop
        return wait_for_exit(deadline)
      end

      shutdown = @pending_mutex.synchronize do
        next if @stopped

        @stopped = true
        @next_id += 1
        id = @next_id
        future = Future.new { remove_pending(id) }
        payload = Transport.dump(op: :stop, id: id)
        wait_for_mailbox_space(deadline, payload.bytesize)
        @pending[id] = future
        @outbox << payload
        @outbox_bytes += payload.bytesize
        @outbox_condition.signal
        future
      end
      return wait_for_exit(deadline) unless shutdown

      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise AskTimeoutError if remaining <= 0

      shutdown.value(timeout: remaining)
      close_and_reap
      wait_for_exit(deadline)
    rescue ActorStoppedError, AskTimeoutError, IOError, SystemCallError
      force_stop
      wait_for_exit(deadline)
    end

    def alive?
      @pending_mutex.synchronize do
        return false if @group_exited

        if @process_exited && !RocotoActor.process_group_alive?(@pid)
          @group_exited = true
          return false
        end
        true
      end
    end

    private

    def force_stop
      pending = @pending_mutex.synchronize do
        return if @termination_started

        @termination_started = true
        @stopped = true
        @writer_stopped = true
        @outbox.clear
        @outbox_bytes = 0
        @outbox_condition.broadcast
        values = @pending.values
        @pending.clear
        values
      end
      pending.each { |future| future.reject(ActorStoppedError.new("actor stopped")) }
      @socket.close unless @socket.closed?
      RocotoActor.signal_process_group(@pid, "KILL")
      nil
    rescue Errno::ESRCH, IOError
      nil
    ensure
      start_reaper
    end

    def write_requests
      loop do
        payload, stop_writer = @pending_mutex.synchronize do
          @outbox_condition.wait(@pending_mutex) while @outbox.empty? && @control_outbox.empty? && !@writer_stopped
          if @writer_stopped
            [nil, true]
          elsif !@control_outbox.empty?
            [@control_outbox.shift, false]
          else
            next_payload = @outbox.shift
            @outbox_bytes -= next_payload.bytesize
            @writing_bytes = next_payload.bytesize
            [next_payload, false].tap { @outbox_condition.broadcast }
          end
        end
        break if stop_writer

        Transport.write_payload(@socket, payload)
        @pending_mutex.synchronize do
          @writing_bytes = 0
          @outbox_condition.broadcast
        end
      end
    rescue IOError, SystemCallError => error
      fail_pending(ActorStoppedError.new(error.message))
      force_stop
    end

    def read_replies
      while (reply = Transport.read(@socket))
        if reply[:op] == :broker_request
          broker = @pending_mutex.synchronize { @broker }
          if broker
            broker.route(self, reply)
          else
            send_broker_response(reply[:request_id], error: Error.new("actor broker is unavailable"))
          end
          next
        end

        future = remove_pending(reply.fetch(:id))
        next unless future

        if reply[:ok]
          future.fulfill(reply[:result])
        else
          future.reject(RemoteError.new(reply[:error_class], reply[:message], reply[:backtrace]))
        end
      end
    rescue EOFError, IOError, SystemCallError, Error => error
      fail_pending(ActorStoppedError.new(error.message))
    ensure
      force_stop
    end

    def remove_pending(id)
      @pending_mutex.synchronize { @pending.delete(id) }
    end

    def fail_pending(error)
      pending = @pending_mutex.synchronize do
        values = @pending.values
        @pending.clear
        values
      end
      pending.each { |future| future.reject(error) }
    end

    def close_and_reap
      @pending_mutex.synchronize do
        @writer_stopped = true
        @outbox_condition.broadcast
      end
      @socket.close unless @socket.closed?
    rescue IOError
      nil
    ensure
      start_reaper
    end

    def wait_for_mailbox_space(deadline, payload_bytes)
      until mailbox_has_space?(payload_bytes)
        raise ActorStoppedError, "actor is stopped" if @termination_started

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        raise AskTimeoutError if remaining <= 0

        @outbox_condition.wait(@pending_mutex, remaining)
      end
      raise ActorStoppedError, "actor is stopped" if @termination_started
    end

    def mailbox_has_space?(payload_bytes)
      @outbox.length < @mailbox_size &&
        @outbox_bytes + @writing_bytes + payload_bytes <= @mailbox_bytes
    end

    def start_reaper
      @reaper_mutex.synchronize do
        return if @reaper || !@pid

        @reaper = Thread.new do
          Process.waitpid(@pid)
          actor_exited
        rescue Errno::ECHILD
          actor_exited
        end
        @reaper.name = "rocoto-actor-reaper-#{@pid}" if @reaper.respond_to?(:name=)
      end
    end

    def actor_exited
      pending = @pending_mutex.synchronize do
        return if @process_exited

        @process_exited = true
        @stopped = true
        @termination_started = true
        @writer_stopped = true
        @control_outbox.clear
        @outbox.clear
        @outbox_bytes = 0
        @outbox_condition.broadcast
        @exit_condition.broadcast
        values = @pending.values
        @pending.clear
        values
      end
      @socket.close unless @socket.closed?
      RocotoActor.signal_process_group(@pid, "KILL")
      pending.each { |future| future.reject(ActorStoppedError.new("actor process exited")) }
    rescue IOError
      nil
    end

    def wait_for_exit(deadline)
      @pending_mutex.synchronize do
        until @process_exited
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          return false if remaining <= 0

          @exit_condition.wait(@pending_mutex, remaining)
        end
      end
      while RocotoActor.process_group_alive?(@pid)
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.01
      end
      @pending_mutex.synchronize { @group_exited = true }
      true
    end
  end
end