# frozen_string_literal: true

require "securerandom"

module RocotoActor
  class ActorBroker
    def initialize
      @mutex = Mutex.new
      @references = {}
      @stopped = false
    end

    def spawn(actor_class, *arguments, **options)
      @mutex.synchronize do
        raise ActorStoppedError, "actor broker is stopped" if @stopped
      end

      reference = RocotoActor.spawn(actor_class, *arguments, **options)
      id = SecureRandom.hex(16)
      @mutex.synchronize { @references[id] = reference }
      reference.attach_broker(self)
      ActorHandle.new(id, reference: reference)
    rescue Exception
      reference&.stop(force: true)
      raise
    end

    def stop(timeout: Reference::DEFAULT_STOP_TIMEOUT, force: false)
      references = @mutex.synchronize do
        @stopped = true
        @references.values.dup
      end
      references.each { |reference| reference.stop(timeout: timeout, force: force) }
    end

    def route(source, request)
      reference = @mutex.synchronize { @references[request[:handle_id]] }
      unless reference
        source.send_broker_response(request[:request_id], error: Error.new("unknown actor handle"))
        return
      end

      Thread.new do
        begin
          result = reference.ask(request[:message]).value(timeout: request[:timeout])
          source.send_broker_response(request[:request_id], result: result)
        rescue RemoteError => error
          source.send_broker_response(
            request[:request_id],
            error: error,
            error_class: error.remote_class,
            message: error.message,
            backtrace: error.remote_backtrace
          )
        rescue StandardError => error
          source.send_broker_response(request[:request_id], error: error)
        end
      end
    end
  end
end
