# frozen_string_literal: true

require "socket"
require "rbconfig"

require_relative "rocoto_actor/version"
require_relative "rocoto_actor/errors"
require_relative "rocoto_actor/handle"
require_relative "rocoto_actor/transport"
require_relative "rocoto_actor/future"
require_relative "rocoto_actor/reference"
require_relative "rocoto_actor/broker"

module RocotoActor
  PARENT_CHECK_INTERVAL = 0.1
  START_TIMEOUT = 5
  DEFAULT_MAILBOX_SIZE = 1_000
  DEFAULT_MAILBOX_BYTES = 16 * 1024 * 1024
  RUNNER_PATH = File.expand_path("rocoto_actor/runner.rb", __dir__)
  CHILD_SOCKET_FD = 3

  module_function

  def spawn(actor_class, *arguments, source: nil, start_timeout: START_TIMEOUT,
            mailbox_size: DEFAULT_MAILBOX_SIZE, mailbox_bytes: DEFAULT_MAILBOX_BYTES)
    actor_name = actor_class.name
    raise ArgumentError, "actor class must have a name" if actor_name.nil? || actor_name.empty?

    source ||= Object.const_source_location(actor_name)&.first
    raise ArgumentError, "cannot locate source for #{actor_name}; pass source:" unless source

    parent_socket, child_socket = UNIXSocket.pair
    parent_pid = Process.pid
    environment = {
      "ROCOTO_ACTOR_FD" => CHILD_SOCKET_FD.to_s,
      "ROCOTO_ACTOR_PARENT_PID" => parent_pid.to_s,
      "ROCOTO_ACTOR_CLASS" => actor_name,
      "ROCOTO_ACTOR_SOURCE" => File.expand_path(source)
    }
    pid = Process.spawn(
      environment,
      RbConfig.ruby,
      RUNNER_PATH,
      CHILD_SOCKET_FD => child_socket,
      in: File::NULL,
      out: File::NULL,
      err: File::NULL,
      pgroup: true,
      close_others: true
    )
    child_socket.close
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + start_timeout
    Transport.write(parent_socket, { op: :boot, arguments: arguments }, timeout: start_timeout)
    wait_until_ready(parent_socket, pid, deadline: deadline)
    Reference.new(parent_socket, pid, mailbox_size: mailbox_size, mailbox_bytes: mailbox_bytes)
  rescue Exception # rubocop:disable Lint/RescueException
    parent_socket&.close
    child_socket&.close
    terminate_failed_spawn(pid)
    raise
  end

  def wait_until_ready(socket, pid, deadline:)
    remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
    raise TransportTimeoutError, "actor #{pid} startup timed out" if remaining <= 0

    response = Transport.read(socket, timeout: remaining)
    return if response&.fetch(:op) == :ready

    if response&.fetch(:op, nil) == :boot_error
      raise RemoteError.new(response[:error_class], response[:message], response[:backtrace])
    end

    raise Error, "actor #{pid} closed during startup"
  end
  private_class_method :wait_until_ready

  def terminate_failed_spawn(pid)
    return unless pid
    return if Process.waitpid(pid, Process::WNOHANG)

    terminate_process_group(pid)
  rescue Errno::ECHILD
    nil
  end
  private_class_method :terminate_failed_spawn

  def terminate_process_group(pid)
    signal_process_group(pid, "KILL")
    reaper = Thread.new do
      Process.waitpid(pid)
    rescue Errno::ECHILD
      nil
    end
    reaper
  end

  def signal_process_group(pid, signal)
    Process.kill(signal, -pid)
  rescue Errno::ESRCH, Errno::EPERM
    nil
  end

  def process_group_alive?(pid)
    Process.kill(0, -pid)
    true
  rescue Errno::ESRCH
    false
  rescue Errno::EPERM
    true
  end

  def run_actor(socket, actor)
    loop do
      request = Transport.read(socket)
      break unless request

      if request.fetch(:op) == :stop
        Transport.write(socket, id: request.fetch(:id), ok: true, result: nil)
        break
      end
      next unless request[:op] == :ask

      response = begin
        { id: request.fetch(:id), ok: true, result: actor.receive(request[:message]) }
      rescue StandardError => error
        error_response(request.fetch(:id), error)
      end
      begin
        Transport.write(socket, response)
      rescue SerializationError => error
        Transport.write(socket, error_response(request.fetch(:id), error))
      end
    end
  rescue EOFError, IOError, SystemCallError
    nil
  ensure
    socket.close unless socket.closed?
    exit! 0
  end

  def error_response(id, error)
    {
      id: id,
      ok: false,
      error_class: error.class.name,
      message: error.message,
      backtrace: error.backtrace || []
    }
  end
  private_class_method :error_response
end