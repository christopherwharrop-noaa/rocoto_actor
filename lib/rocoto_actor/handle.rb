# frozen_string_literal: true

module RocotoActor
  class ActorHandle
    attr_reader :id

    def initialize(id, reference: nil, socket: nil)
      @id = id
      @reference = reference
      @socket = socket
      @request_mutex = Mutex.new if socket
      @next_request_id = 0
    end

    def ask(message)
      raise Error, "actor handle is not local" unless @reference

      @reference.ask(message)
    end

    def call(message, timeout: nil)
      return ask(message).value(timeout: timeout) if @reference

      @request_mutex.synchronize do
        @next_request_id += 1
        request_id = @next_request_id
        Transport.write(
          @socket,
          op: :broker_request,
          request_id: request_id,
          handle_id: @id,
          message: message,
          timeout: timeout
        )

        loop do
          response = Transport.read(@socket, timeout: timeout)
          raise ActorStoppedError, "broker connection closed" unless response
          next unless response[:op] == :broker_response && response[:request_id] == request_id

          return response[:result] if response[:ok]

          raise RemoteError.new(response[:error_class], response[:message], response[:backtrace])
        end
      end
    rescue TransportTimeoutError => error
      raise AskTimeoutError, error.message
    end

    def stop(**options)
      raise Error, "actor handle is not local" unless @reference

      @reference.stop(**options)
    end

    def alive?
      raise Error, "actor handle is not local" unless @reference

      @reference.alive?
    end

    def local_reference
      @reference
    end
  end
end
