# frozen_string_literal: true

module RocotoActor
  # Broker-owned record for one logical actor in the family tree. All methods
  # are called while ActorBroker's mutex is held.
  class ActorNode
    STATES = %i[starting running restarting stopping stopped failed].freeze
    TERMINAL_STATES = %i[stopped failed].freeze
    ACTIVE_STATES = %i[starting running restarting].freeze

    attr_reader :id, :name, :path, :generation, :parent_id, :children, :state, :reference,
                :booting, :spec, :policy, :restarts, :failure, :exit, :boot_exit, :started_at

    def initialize(id:, name:, path:, parent_id:, reference:, spec:, policy:)
      @id = id
      @name = name
      @path = path
      @generation = 1
      @parent_id = parent_id
      @children = []
      @state = :starting
      @reference = reference
      @booting = true
      @spec = spec
      @policy = policy
      @restarts = 0
      @failure = nil
      @exit = nil
      @boot_exit = nil
      @started_at = nil
    end

    def terminal?
      TERMINAL_STATES.include?(@state)
    end

    def active?
      ACTIVE_STATES.include?(@state)
    end

    def boot_succeeded(now)
      @booting = false
      return unless @state == :starting

      @state = :running
      @started_at = now
    end

    def boot_failed
      @booting = false
    end

    def record_boot_exit(reference)
      @boot_exit = reference
    end

    def begin_stopping
      @state = :stopping
    end

    def begin_restarting
      @state = :restarting
    end

    def record_restart_attempt(now, window)
      @restarts = 0 if @started_at && now - @started_at > window
      @started_at = nil
      return false if @restarts >= @policy[:max_restarts]

      @restarts += 1
      @restarts
    end

    def install_restarted_reference(reference) # rubocop:disable Naming/PredicateMethod
      return false unless @state == :restarting

      record_reference(@reference)
      @reference = reference
      @generation += 1
      @booting = true
      true
    end

    def restart_succeeded(now)
      @booting = false
      return unless @state == :restarting

      @state = :running
      @started_at = now
    end

    def retire(state)
      @state = state
      record_reference(@reference)
      reference = @reference
      @reference = nil
      @spec = nil
      @restarts = 0
      reference
    end

    private

    def record_reference(reference)
      return unless reference

      @failure = reference.exit_error || @failure
      @exit = reference.exit_status || @exit
    end
  end
  private_constant :ActorNode
end
