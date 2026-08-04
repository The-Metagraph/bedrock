defmodule Bedrock.DataPlane.Log.Shale.Recovery do
  @moduledoc """
  Recovery logic for Shale log servers.

  Supports multi-source recovery for the consistent hashing model. When multiple
  source logs are provided, transactions are pulled from available sources to
  establish the version range. Since all logs receive the same version sequence
  (with personalized content), pulling from any survivor establishes the correct
  version boundaries.

  For future optimization, true multi-source coalescing could merge transaction
  streams and filter by shard index, but for now we use the simpler approach
  of pulling from available sources.
  """
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push_recovery: 3]

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction

  @spec recover_from(
          State.t(),
          source_logs :: [Log.ref()],
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | {:error, :lock_required}
          | {:error, term(), State.t()}
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
          | {:error, :no_source_logs_available}
  def recover_from(t, _, _, _) when t.mode != :locked, do: {:error, :lock_required}

  def recover_from(t, source_logs, first_version, last_version) do
    source_logs = List.wrap(source_logs)

    if self() in source_logs do
      recover_from_self(t, first_version, last_version)
    else
      recover_from_sources(t, source_logs, first_version, last_version)
    end
  end

  defp recover_from_sources(t, source_logs, first_version, last_version) do
    sync_fun = writer_sync_fun(t)
    recovering = t |> Map.put(:mode, :recovering) |> abort_all_waiting_pullers()

    with {:ok, t} <- reset_recovery_target(recovering, first_version, sync_fun),
         {:ok, t} <-
           pull_transactions_from_sources_with_state(
             t,
             source_logs,
             first_version,
             last_version,
             sync_fun
           ),
         :ok <- validate_final_version(t, last_version),
         {:ok, t} <- sync_recovery_writer(t) do
      {oldest, last} =
        if first_version == last_version do
          {t.oldest_version, t.last_version}
        else
          {first_version, last_version}
        end

      {:ok, %{t | mode: :running, oldest_version: oldest, last_version: last}}
    else
      {:error, reason, failed_t} ->
        failed_recovery(reason, failed_t)
    end
  end

  defp recover_from_self(t, first_version, last_version) do
    t =
      t
      |> abort_all_waiting_pullers()
      |> close_writer()
      |> ensure_active_segment(first_version)
      |> open_writer()

    {:ok,
     %{
       t
       | mode: :running,
         oldest_version: min_version(t.oldest_version, first_version),
         last_version: max_version(t.last_version, last_version)
     }}
  end

  defp min_version(left, right) when left <= right, do: left
  defp min_version(_left, right), do: right

  defp max_version(left, right) when left >= right, do: left
  defp max_version(_left, right), do: right

  @spec pull_transactions_from_sources(
          t :: State.t(),
          source_logs :: [Log.ref()],
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
          | {:error, :no_source_logs_available}

  # No source logs - this is initial recovery (brand new cluster)
  def pull_transactions_from_sources(t, [], first_version, last_version) when first_version == last_version do
    {:ok, %{t | oldest_version: first_version, last_version: first_version}}
  end

  def pull_transactions_from_sources(_t, [], _first_version, _last_version) do
    # No source logs available and we have transactions to recover
    {:error, :no_source_logs_available}
  end

  # Single source log - use original behavior
  def pull_transactions_from_sources(t, [source_log], first_version, last_version) do
    pull_transactions(t, source_log, first_version, last_version)
  end

  # Multiple source logs - try each in order until one succeeds
  # All logs have the same version sequence, so any survivor works
  def pull_transactions_from_sources(t, source_logs, first_version, last_version) do
    case pull_transactions_from_sources_with_state(
           t,
           source_logs,
           first_version,
           last_version,
           writer_sync_fun(t)
         ) do
      {:ok, t} -> {:ok, t}
      {:error, reason, _t} -> {:error, reason}
    end
  end

  defp pull_transactions_from_sources_with_state(t, [], first_version, last_version, _sync_fun)
       when first_version == last_version do
    {:ok, %{t | oldest_version: first_version, last_version: first_version}}
  end

  defp pull_transactions_from_sources_with_state(t, [], _first_version, _last_version, _sync_fun) do
    {:error, :no_source_logs_available, t}
  end

  defp pull_transactions_from_sources_with_state(t, source_logs, first_version, last_version, sync_fun) do
    try_pull_from_sources(t, source_logs, first_version, last_version, sync_fun, [])
  end

  defp try_pull_from_sources(t, [], _first_version, _last_version, _sync_fun, errors) do
    # All sources failed, return the last error
    case errors do
      [reason | _] -> {:error, reason, t}
      _ -> {:error, :no_source_logs_available, t}
    end
  end

  defp try_pull_from_sources(t, [source_log | rest], first_version, last_version, sync_fun, errors) do
    case pull_transactions_with_state(t, source_log, first_version, last_version) do
      {:ok, t} ->
        {:ok, t}

      {:error, reason, failed_t} ->
        retry_from_next_source(
          failed_t,
          rest,
          first_version,
          last_version,
          sync_fun,
          [reason | errors]
        )
    end
  end

  defp retry_from_next_source(t, [], first_version, last_version, sync_fun, errors) do
    try_pull_from_sources(t, [], first_version, last_version, sync_fun, errors)
  end

  defp retry_from_next_source(t, remaining_sources, first_version, last_version, sync_fun, errors) do
    case reset_recovery_target(t, first_version, sync_fun) do
      {:ok, clean_t} ->
        try_pull_from_sources(
          clean_t,
          remaining_sources,
          first_version,
          last_version,
          sync_fun,
          errors
        )

      {:error, reason, failed_t} ->
        {:error, {:source_retry_reset_failed, reason}, failed_t}
    end
  end

  @spec pull_transactions(
          t :: State.t(),
          log_ref :: Log.ref(),
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version()
        ) ::
          {:ok, State.t()}
          | Log.pull_errors()
          | {:error, {:source_log_unavailable, log_ref :: Log.ref()}}
  def pull_transactions(t, _, first_version, last_version) when first_version == last_version do
    {:ok, %{t | oldest_version: first_version, last_version: first_version}}
  end

  def pull_transactions(t, log_ref, first_version, last_version) do
    case pull_transactions_with_state(t, log_ref, first_version, last_version) do
      {:ok, t} -> {:ok, t}
      {:error, reason, _t} -> {:error, reason}
    end
  end

  defp pull_transactions_with_state(t, _, first_version, last_version) when first_version == last_version do
    {:ok, %{t | oldest_version: first_version, last_version: first_version}}
  end

  defp pull_transactions_with_state(t, log_ref, first_version, last_version) do
    case Log.pull(log_ref, first_version, recovery: true, last_version: last_version) do
      {:ok, []} ->
        {:ok, t}

      {:ok, transactions} ->
        transactions
        |> Enum.reduce_while({first_version, t}, fn bytes, acc ->
          process_transaction_bytes(bytes, acc)
        end)
        |> case do
          {:error, reason, failed_t} -> {:error, reason, failed_t}
          {next_first, t} -> pull_transactions_with_state(t, log_ref, next_first, last_version)
        end

      {:error, :unavailable} ->
        {:error, {:source_log_unavailable, log_ref}, t}

      {:error, reason} ->
        {:error, {:log_pull_failed, reason, log_ref}, t}
    end
  end

  @spec process_transaction_bytes(Transaction.encoded(), {Bedrock.version(), State.t()}) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term(), State.t()}}
  defp process_transaction_bytes(bytes, {last_version, t}) do
    case Transaction.commit_version(bytes) do
      {:ok, version} when is_binary(version) ->
        handle_valid_transaction_bytes(bytes, version, last_version, t)

      {:ok, nil} ->
        {:halt, {:error, :missing_transaction_id, t}}

      {:error, :invalid_format} ->
        {:halt, {:error, :invalid_transaction, t}}

      {:error, reason} ->
        {:halt, {:error, reason, t}}
    end
  end

  @spec handle_valid_transaction_bytes(
          Transaction.encoded(),
          Bedrock.version(),
          Bedrock.version(),
          State.t()
        ) ::
          {:cont, {Bedrock.version(), State.t()}} | {:halt, {:error, term(), State.t()}}
  defp handle_valid_transaction_bytes(bytes, version, last_version, t) do
    with {:ok, _transaction} <- Transaction.decode(bytes),
         {:ok, t} <- push_recovery(t, last_version, bytes) do
      {:cont, {version, t}}
    else
      {:error, :invalid_format} -> {:halt, {:error, :invalid_transaction, t}}
      {:error, reason, failed_t} -> {:halt, {:error, reason, failed_t}}
      {:error, reason} -> {:halt, {:error, reason, t}}
    end
  end

  defp writer_sync_fun(%{writer: %Writer{sync_fun: sync_fun}}), do: sync_fun
  defp writer_sync_fun(_t), do: nil

  defp reset_recovery_target(t, first_version, sync_fun) do
    with {:ok, t} <- close_writer_result(t),
         {:ok, t} <- discard_all_segments_result(t),
         {:ok, t} <- ensure_active_segment_result(t, first_version),
         {:ok, t} <- open_writer_result(t, sync_fun) do
      push_sentinel_result(t, first_version)
    end
  end

  defp close_writer_result(%{writer: nil} = t), do: {:ok, t}

  defp close_writer_result(%{writer: writer} = t) do
    case Writer.close(writer) do
      :ok -> {:ok, %{t | writer: nil}}
      {:error, reason} -> {:error, {:writer_close_failed, reason}, t}
    end
  end

  defp discard_all_segments_result(t) do
    segments = if t.active_segment, do: [t.active_segment | t.segments], else: t.segments
    discard_segments_result(segments, %{t | active_segment: nil, segments: []})
  end

  defp discard_segments_result([], t), do: {:ok, t}

  defp discard_segments_result([segment | rest], t) do
    case SegmentRecycler.check_in(t.segment_recycler, segment.path) do
      :ok ->
        discard_segments_result(rest, t)

      {:error, reason} ->
        {:error, {:segment_recycle_failed, reason}, %{t | segments: [segment | rest]}}
    end
  end

  defp ensure_active_segment_result(t, version) do
    case Segment.allocate_from_recycler(t.segment_recycler, t.path, version) do
      {:ok, new_segment} ->
        {:ok, %{t | active_segment: new_segment, last_version: version}}

      {:error, reason} ->
        {:error, {:segment_allocation_failed, reason}, t}
    end
  end

  defp open_writer_result(t, sync_fun) do
    opts = if sync_fun, do: [sync_fun: sync_fun], else: []

    case Writer.open(t.active_segment.path, opts) do
      {:ok, writer} -> {:ok, %{t | writer: writer}}
      {:error, reason} -> {:error, {:writer_open_failed, reason}, t}
    end
  end

  defp push_sentinel_result(t, version) do
    sentinel_transaction = %{mutations: []}
    encoded_sentinel = Transaction.encode(sentinel_transaction)

    with {:ok, sentinel} <- Transaction.add_commit_version(encoded_sentinel, version),
         {:ok, t} <- push_recovery(t, version, sentinel) do
      {:ok, t}
    else
      {:error, reason, failed_t} -> {:error, {:sentinel_push_failed, reason}, failed_t}
      {:error, reason} -> {:error, {:sentinel_push_failed, reason}, t}
    end
  end

  defp validate_final_version(%{last_version: version}, version), do: :ok

  defp validate_final_version(%{last_version: actual} = t, expected),
    do: {:error, {:final_version_mismatch, expected, actual}, t}

  defp sync_recovery_writer(%{writer: writer} = t) do
    case Writer.sync(writer) do
      {:ok, writer} -> {:ok, %{t | writer: writer}}
      {:error, reason} -> {:error, {:final_sync_failed, reason}, t}
    end
  end

  defp failed_recovery(reason, t) do
    case cleanup_failed_recovery(t) do
      {:ok, locked_t} ->
        {:error, reason, locked_t}

      {:error, cleanup_reason, failed_t} ->
        {:error, {:recovery_cleanup_failed, reason, cleanup_reason}, failed_t}
    end
  end

  defp cleanup_failed_recovery(t) do
    with {:ok, t} <- close_writer_result(t),
         {:ok, t} <- discard_all_segments_result(t) do
      {:ok,
       %{
         t
         | mode: :locked,
           writer: nil,
           active_segment: nil,
           segments: [],
           oldest_version: nil,
           last_version: nil
       }}
    end
  end

  @spec abort_all_waiting_pullers(State.t()) :: State.t()
  def abort_all_waiting_pullers(%{waiting_pullers: waiting_pullers} = t) do
    Enum.reduce(waiting_pullers, %{t | waiting_pullers: %{}}, fn {_version, puller_list}, t ->
      Enum.each(puller_list, fn {_timestamp, reply_to_fn, _opts} ->
        reply_to_fn.({:ok, []})
      end)

      t
    end)
  end

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: nil} = t), do: t

  @spec close_writer(State.t()) :: State.t()
  def close_writer(%{writer: writer} = t) do
    :ok = Writer.close(writer)
    %{t | writer: nil}
  end

  @spec ensure_active_segment(State.t(), Bedrock.version()) :: State.t()
  def ensure_active_segment(%{active_segment: nil} = t, version) do
    case Segment.allocate_from_recycler(t.segment_recycler, t.path, version) do
      {:ok, new_segment} -> %{t | active_segment: new_segment, last_version: version}
      {:error, :allocation_failed} -> raise "Failed to allocate new segment"
    end
  end

  def ensure_active_segment(t, _version), do: t

  @spec open_writer(State.t()) :: State.t()
  def open_writer(t) do
    case Writer.open(t.active_segment.path) do
      {:ok, new_writer} ->
        %{t | writer: new_writer}

      {:error, _} ->
        raise "Failed to open writer"
    end
  end
end
