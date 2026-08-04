defmodule Bedrock.DataPlane.Log.Shale.Writer do
  @moduledoc """
  A struct that represents a writer for a segment.
  """

  alias Bedrock.DataPlane.Transaction

  defstruct [:fd, :write_offset, :bytes_remaining, :sync_fun, dirty?: false]

  @wal_eof_version <<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>
  @eof_marker <<@wal_eof_version::binary, 0::unsigned-big-32, 0::unsigned-big-32>>
  @empty_segment_header <<"BED0">> <> @eof_marker

  @type durability :: :immediate | :deferred

  @typedoc """
  A `Writer` is a handle to a segment that can be used to write transactions
  to the segment. It is a stateful object that keeps track of the current
  write offset and the number of bytes remaining in the segment.
  """
  @type t :: %__MODULE__{
          fd: File.file_descriptor(),
          write_offset: pos_integer(),
          bytes_remaining: pos_integer(),
          sync_fun: (File.file_descriptor() -> :ok | {:error, term()}),
          dirty?: boolean()
        }

  @spec open(path_to_file :: String.t(), opts :: keyword()) :: {:ok, t()} | {:error, File.posix()}
  def open(path_to_file, opts \\ []) do
    sync_fun = Keyword.get(opts, :sync_fun, &:file.sync/1)

    with {:ok, stat} <- File.stat(path_to_file),
         {:ok, fd} <- File.open(path_to_file, [:write, :read, :raw, :binary]) do
      # Write header - close fd on failure to avoid leak
      case :file.pwrite(fd, 0, @empty_segment_header) do
        :ok ->
          {:ok,
           %__MODULE__{
             fd: fd,
             write_offset: 4,
             bytes_remaining: stat.size - 4 - 16,
             sync_fun: sync_fun,
             dirty?: true
           }}

        {:error, reason} ->
          File.close(fd)
          {:error, reason}
      end
    end
  end

  @spec close(writer :: t() | nil) :: :ok | {:error, File.posix()}
  def close(nil), do: :ok
  def close(%__MODULE__{} = writer), do: :file.close(writer.fd)

  @doc """
  Synchronizes a dirty writer and returns its clean state.

  Closing a writer is deliberately not a durability barrier. Callers using
  deferred appends must call this function before closing or publishing the
  segment.
  """
  @spec sync(t()) :: {:ok, t()} | {:error, term()}
  def sync(%__MODULE__{dirty?: false} = writer), do: {:ok, writer}

  def sync(%__MODULE__{} = writer) do
    case writer.sync_fun.(writer.fd) do
      :ok -> {:ok, %{writer | dirty?: false}}
      {:error, _reason} = error -> error
    end
  end

  @spec append(t(), Transaction.encoded(), Bedrock.version()) ::
          {:ok, t()} | {:error, :segment_full} | {:error, term()}
  def append(%__MODULE__{} = writer, transaction, commit_version) do
    append(writer, transaction, commit_version, :immediate)
  end

  @spec append(t(), Transaction.encoded(), Bedrock.version(), durability()) ::
          {:ok, t()} | {:error, :segment_full} | {:error, term()}
  def append(%__MODULE__{} = writer, transaction, _commit_version, durability)
      when durability in [:immediate, :deferred] and writer.bytes_remaining < 16 + byte_size(transaction),
      do: {:error, :segment_full}

  def append(%__MODULE__{} = writer, transaction, commit_version, durability)
      when durability in [:immediate, :deferred] do
    # Wrap transaction in log format: [version, size, payload, crc32]
    payload_size = byte_size(transaction)
    crc32 = :erlang.crc32(transaction)

    log_entry = <<
      commit_version::binary-size(8),
      payload_size::unsigned-big-32,
      transaction::binary,
      crc32::unsigned-big-32
    >>

    writer.fd
    |> :file.pwrite(writer.write_offset, [log_entry, @eof_marker])
    |> case do
      :ok ->
        size_of_entry = byte_size(log_entry)

        dirty_writer = %{
          writer
          | write_offset: writer.write_offset + size_of_entry,
            bytes_remaining: writer.bytes_remaining - size_of_entry,
            dirty?: true
        }

        maybe_sync(dirty_writer, durability)

      {:error, _reason} = error ->
        error
    end
  end

  def append(%__MODULE__{}, _transaction, _commit_version, durability), do: {:error, {:invalid_durability, durability}}

  defp maybe_sync(writer, :immediate), do: sync(writer)
  defp maybe_sync(writer, :deferred), do: {:ok, writer}
end
