# frozen_string_literal: true

require "minitest/autorun"
require "stringio"
require "socket"
require_relative "../lib/rocoto_actor"

class TransportTest < Minitest::Test
  TRANSPORT = RocotoActor.const_get(:Transport) # internal; exercised directly here
  DECODE_BINDINGS = RocotoActor.const_get(:DecodeBindings)

  class FakeBroker
    attr_reader :asks

    def initialize
      @asks = []
    end

    def ask(id, message)
      @asks << [id, message]
      :future
    end
  end

  def test_json_codec_round_trips_supported_values
    message = {
      operation: :ask,
      "payload" => [nil, true, false, "text", 42, 1.5, { nested: :value }]
    }
    io = StringIO.new

    TRANSPORT.write(io, message)
    io.rewind

    assert_equal message, TRANSPORT.read(io)
  end

  def test_decode_bindings_bind_handles_to_the_broker
    broker = FakeBroker.new
    bindings = DECODE_BINDINGS.new
    bindings.attach_broker(broker)
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), { nested: RocotoActor::ActorHandle.new("child") }])
    io.rewind

    actor, nested = TRANSPORT.read(io, bindings: bindings)

    assert_equal :future, actor.ask(:work)
    assert_equal :future, nested[:nested].ask(:child_work)
    assert_equal [["actor", :work], ["child", :child_work]], broker.asks
  end

  def test_decode_bindings_bind_worker_capabilities_to_the_socket
    reader, writer = UNIXSocket.pair
    bindings = DECODE_BINDINGS.new(socket: reader)
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), RocotoActor::Timer.new("timer")])
    io.rewind

    handle, timer = TRANSPORT.read(io, bindings: bindings)

    assert_same RocotoActor.broker_client(reader), handle.instance_variable_get(:@client)
    assert_same RocotoActor.broker_client(reader), timer.instance_variable_get(:@client)
  ensure
    reader&.close
    writer&.close
  end

  def test_unbound_decode_bindings_produce_unbound_capabilities
    io = StringIO.new
    TRANSPORT.write(io, [RocotoActor::ActorHandle.new("actor"), RocotoActor::Timer.new("timer")])
    io.rewind

    handle, timer = TRANSPORT.read(io)

    assert_raises(RocotoActor::Error) { handle.ask(:work) }
    assert_raises(RocotoActor::Error) { timer.cancel }
  end

  def test_decode_bindings_attach_to_only_one_broker
    bindings = DECODE_BINDINGS.new
    broker = FakeBroker.new

    bindings.attach_broker(broker)

    bindings.attach_broker(broker)
    assert_raises(RocotoActor::Error) { bindings.attach_broker(FakeBroker.new) }
    assert_raises(RocotoActor::Error) { DECODE_BINDINGS.new(socket: StringIO.new).attach_broker(broker) }
  end

  def test_json_codec_rejects_arbitrary_objects_without_writing
    io = StringIO.new

    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(io, Object.new)
    end

    assert_match(/unsupported value type: Object/, error.message)
    assert_empty io.string
  end

  def test_json_codec_rejects_non_finite_numbers
    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, Float::INFINITY)
    end

    assert_match(/non-finite/, error.message)
  end

  def test_json_codec_normalizes_invalid_utf8_errors
    error = assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, "\xFF".b)
    end

    assert_match(/UTF-8/, error.message)
  end

  def test_json_codec_rejects_cycles
    value = []
    value << value

    assert_raises(RocotoActor::SerializationError) do
      TRANSPORT.write(StringIO.new, value)
    end
  end

  def test_read_timeout_covers_partial_frames
    reader, writer = UNIXSocket.pair
    writer.write("\x00\x00")

    assert_raises(RocotoActor::TransportTimeoutError) do
      TRANSPORT.read(reader, timeout: 0.05)
    end
  ensure
    reader&.close
    writer&.close
  end

  def test_read_with_timeout_returns_nil_at_frame_boundary_eof
    reader, writer = UNIXSocket.pair
    writer.close

    assert_nil TRANSPORT.read(reader, timeout: 0.05)
  ensure
    reader&.close
    writer&.close
  end

  def test_read_with_timeout_rejects_partial_frame_at_eof
    reader, writer = UNIXSocket.pair
    writer.write([4].pack("N") << "x")
    writer.close

    assert_raises(EOFError) do
      TRANSPORT.read(reader, timeout: 0.05)
    end
  ensure
    reader&.close
    writer&.close
  end
end
