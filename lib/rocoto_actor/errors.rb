# frozen_string_literal: true

module RocotoActor
  class Error < StandardError; end
  class ActorStoppedError < Error; end
  class AskTimeoutError < Error; end
  class MailboxFullError < Error; end
  class SerializationError < Error; end
  class TransportTimeoutError < Error; end

  class RemoteError < Error
    attr_reader :remote_class, :remote_backtrace

    def initialize(remote_class, message, remote_backtrace = [])
      @remote_class = remote_class
      @remote_backtrace = remote_backtrace
      super("#{remote_class}: #{message}")
      set_backtrace(remote_backtrace)
    end
  end
end