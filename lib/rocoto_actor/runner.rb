# frozen_string_literal: true

require_relative "../rocoto_actor"

module RocotoActor
  module Runner
    module_function

    def run
      socket = UNIXSocket.for_fd(Integer(ENV.fetch("ROCOTO_ACTOR_FD")))
      parent_pid = Integer(ENV.fetch("ROCOTO_ACTOR_PARENT_PID"))
      worker_pid = fork { run_worker(socket) }
      socket.close
      supervise(worker_pid, parent_pid)
    rescue Exception => error # rubocop:disable Lint/RescueException
      report_boot_error(socket, error)
      socket&.close
      exit! 1
    end

    def run_worker(socket)
      require ENV.fetch("ROCOTO_ACTOR_SOURCE")
      actor_class = constantize(ENV.fetch("ROCOTO_ACTOR_CLASS"))
      boot = Transport.read(socket)
      raise Error, "expected actor boot message" unless boot&.fetch(:op) == :boot

      actor = actor_class.new(*boot.fetch(:arguments))
      Transport.write(socket, op: :ready)
      RocotoActor.run_actor(socket, actor)
    rescue Exception => error # rubocop:disable Lint/RescueException
      report_boot_error(socket, error)
      socket&.close
      exit! 1
    end
    private_class_method :run_worker

    def supervise(worker_pid, parent_pid)
      loop do
        terminate_group unless Process.ppid == parent_pid
        break if Process.waitpid(worker_pid, Process::WNOHANG)

        sleep RocotoActor::PARENT_CHECK_INTERVAL
      end
      terminate_group
    rescue Errno::ECHILD
      terminate_group
    end
    private_class_method :supervise

    def terminate_group
      Process.kill("KILL", -Process.getpgrp)
      exit! 0
    end
    private_class_method :terminate_group

    def constantize(name)
      name.split("::").reject(&:empty?).reduce(Object) do |namespace, constant_name|
        namespace.const_get(constant_name, false)
      end
    end
    private_class_method :constantize

    def report_boot_error(socket, error)
      return unless socket && !socket.closed?

      Transport.write(
        socket,
        op: :boot_error,
        error_class: error.class.name,
        message: error.message,
        backtrace: error.backtrace || []
      )
    rescue IOError, SystemCallError
      nil
    end
    private_class_method :report_boot_error
  end
end

RocotoActor::Runner.run