# frozen_string_literal: true

require "minitest/autorun"
require "stringio"
require "socket"
require_relative "../lib/rocoto_actor"

class TransportTest < Minitest::Test
  def test_json_codec_round_trips_supported_values
    message = {
      operation: :ask,
      "payload" => [nil, true, false, "text", 42, 1.5, { nested: :value }]
    }
    io = StringIO.new

    RocotoActor::Transport.write(io, message)
    io.rewind

    assert_equal message, RocotoActor::Transport.read(io)
  end

  def test_json_codec_rejects_arbitrary_objects_without_writing
    io = StringIO.new

    error = assert_raises(RocotoActor::SerializationError) do
      RocotoActor::Transport.write(io, Object.new)
    end

    assert_match(/unsupported value type: Object/, error.message)
    assert_empty io.string
  end

  def test_json_codec_rejects_non_finite_numbers
    error = assert_raises(RocotoActor::SerializationError) do
      RocotoActor::Transport.write(StringIO.new, Float::INFINITY)
    end

    assert_match(/non-finite/, error.message)
  end

  def test_json_codec_normalizes_invalid_utf8_errors
    error = assert_raises(RocotoActor::SerializationError) do
      RocotoActor::Transport.write(StringIO.new, "\xFF".b)
    end

    assert_match(/UTF-8/, error.message)
  end

  def test_json_codec_rejects_cycles
    value = []
    value << value

    assert_raises(RocotoActor::SerializationError) do
      RocotoActor::Transport.write(StringIO.new, value)
    end
  end

  def test_read_timeout_covers_partial_frames
    reader, writer = UNIXSocket.pair
    writer.write("\x00\x00")

    assert_raises(RocotoActor::TransportTimeoutError) do
      RocotoActor::Transport.read(reader, timeout: 0.05)
    end
  ensure
    reader&.close
    writer&.close
  end
end