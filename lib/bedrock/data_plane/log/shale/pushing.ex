defmodule Bedrock.DataPlane.Log.Shale.Pushing do
  @moduledoc false
  import Bedrock.DataPlane.Log.Telemetry

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction

  @spec push(
          t :: State.t(),
          expected_version :: Bedrock.version(),
          encoded_transaction :: Transaction.encoded(),
          ack_fn :: (:ok | {:error, term()} -> :ok)
        ) :: {:ok | :wait, State.t()} | {:error, :tx_out_of_order} | {:error, :tx_too_large}
  def push(%{mode: mode}, _, _, _) when mode in [:locked, :recovering] do
    {:error, :not_ready}
  end

  def push(_, _, encoded_transaction, _ack_fn) when byte_size(encoded_transaction) > 10_000_000 do
    {:error, :tx_too_large}
  end

  def push(t, expected_version, encoded_transaction, ack_fn) when expected_version == t.last_version do
    case write_encoded_transaction(t, encoded_transaction, :immediate, nil) do
      {:ok, t} ->
        trace_push_transaction(encoded_transaction)
        :ok = ack_fn.(:ok)
        do_pending_pushes(t)

      {:error, reason, _failed_t} ->
        :ok = ack_fn.({:error, reason})
        {:error, reason}
    end
  end

  def push(t, expected_version, encoded_transaction, ack_fn) when expected_version > t.last_version do
    {:wait, Map.update!(t, :pending_pushes, &Map.put(&1, expected_version, {encoded_transaction, ack_fn}))}
  end

  def push(t, expected_version, _, _) do
    trace_push_out_of_order(expected_version, t.last_version)
    {:error, :tx_out_of_order}
  end

  @doc false
  @spec push_recovery(State.t(), Bedrock.version(), Transaction.encoded()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def push_recovery(%{mode: :recovering} = t, expected_version, encoded_transaction)
      when expected_version == t.last_version and byte_size(encoded_transaction) <= 10_000_000 do
    case write_encoded_transaction(t, encoded_transaction, :deferred, nil) do
      {:ok, t} ->
        trace_push_transaction(encoded_transaction)
        {:ok, t}

      {:error, _reason, _failed_t} = error ->
        error
    end
  end

  def push_recovery(%{mode: :recovering} = t, _expected_version, encoded_transaction)
      when byte_size(encoded_transaction) > 10_000_000, do: {:error, :tx_too_large, t}

  def push_recovery(%{mode: :recovering} = t, _expected_version, _encoded_transaction),
    do: {:error, :tx_out_of_order, t}

  def push_recovery(t, _expected_version, _encoded_transaction), do: {:error, :not_recovering, t}

  @spec do_pending_pushes(State.t()) ::
          {:ok | :wait, State.t()} | {:error, :tx_out_of_order} | {:error, :tx_too_large}
  def do_pending_pushes(t) do
    next_expected_version = t.last_version

    case Map.pop(t.pending_pushes, next_expected_version) do
      {nil, _} ->
        {:ok, t}

      {{encoded_transaction, ack_fn}, pending_pushes} ->
        t_with_updated_pending = %{t | pending_pushes: pending_pushes}

        case write_encoded_transaction(t_with_updated_pending, encoded_transaction) do
          {:ok, new_t} ->
            trace_push_transaction(encoded_transaction)
            :ok = ack_fn.(:ok)
            do_pending_pushes(new_t)

          {:error, reason} ->
            :ok = ack_fn.({:error, reason})
            {:error, reason}
        end
    end
  end

  @spec write_encoded_transaction(State.t(), Transaction.encoded()) ::
          {:ok, State.t()} | {:error, term()}
  def write_encoded_transaction(t, encoded_transaction) do
    case write_encoded_transaction(t, encoded_transaction, :immediate, nil) do
      {:ok, t} -> {:ok, t}
      {:error, reason, _failed_t} -> {:error, reason}
    end
  end

  defp write_encoded_transaction(t, encoded_transaction, durability, sync_fun) when is_nil(t.writer) do
    version =
      case Transaction.commit_version(encoded_transaction) do
        {:ok, version} ->
          version

        {:error, reason} ->
          raise "Failed to extract version: #{inspect(reason)}"
      end

    case Segment.allocate_from_recycler(t.segment_recycler, t.path, version) do
      {:ok, new_segment} ->
        allocated_t = %{
          t
          | writer: nil,
            active_segment: new_segment,
            segments: if(t.active_segment, do: [t.active_segment | t.segments], else: t.segments)
        }

        case open_writer(new_segment.path, sync_fun) do
          {:ok, new_writer} ->
            write_encoded_transaction(
              %{allocated_t | writer: new_writer},
              encoded_transaction,
              durability,
              sync_fun
            )

          {:error, reason} ->
            {:error, {:writer_open_failed, reason}, allocated_t}
        end

      {:error, reason} ->
        {:error, reason, t}
    end
  end

  defp write_encoded_transaction(t, encoded_transaction, durability, _sync_fun) do
    case Transaction.commit_version(encoded_transaction) do
      {:ok, version} ->
        case Writer.append(t.writer, encoded_transaction, version, durability) do
          {:ok, writer} ->
            # Update the active segment's transaction cache to keep it coherent with disk
            updated_active_segment = update_segment_transaction_cache(t.active_segment, encoded_transaction)
            {:ok, %{t | writer: writer, last_version: version, active_segment: updated_active_segment}}

          {:error, :segment_full} ->
            next_sync_fun = t.writer.sync_fun

            case sync_and_close_full_segment(t.writer) do
              :ok ->
                write_encoded_transaction(
                  %{t | writer: nil},
                  encoded_transaction,
                  durability,
                  next_sync_fun
                )

              {:error, reason} ->
                {:error, reason, t}
            end

          {:error, reason} ->
            {:error, reason, t}
        end

      {:error, reason} ->
        {:error, {:version_extraction_failed, reason}, t}
    end
  end

  defp open_writer(path, nil), do: Writer.open(path)
  defp open_writer(path, sync_fun), do: Writer.open(path, sync_fun: sync_fun)

  defp sync_and_close_full_segment(writer) do
    case Writer.sync(writer) do
      {:ok, writer} ->
        case Writer.close(writer) do
          :ok -> :ok
          {:error, reason} -> {:error, {:segment_close_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:segment_sync_failed, reason}}
    end
  end

  @spec update_segment_transaction_cache(Segment.t(), Transaction.encoded()) :: Segment.t()
  defp update_segment_transaction_cache(segment, encoded_transaction) do
    case segment.transactions do
      nil ->
        # If transactions not loaded, initialize with the new transaction
        %{segment | transactions: [encoded_transaction]}

      existing_transactions ->
        # Prepend new transaction to maintain newest-first order
        %{segment | transactions: [encoded_transaction | existing_transactions]}
    end
  end
end
