defmodule Bedrock.Internal.TransactionBuilder.PointReads do
  @moduledoc """
  Point read operations for the Transaction Builder.

  This module handles single-point read operations, including regular key fetches
  and key selector resolution. All operations ensure proper transaction semantics
  with repeatable reads and conflict tracking.
  """

  import Bedrock.Internal.TransactionBuilder.ReadVersions, only: [ensure_read_version: 2]

  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Internal.TransactionBuilder.LayoutIndex
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.Key
  alias Bedrock.KeySelector

  @type storage_get_key_fn() :: (pid(), binary(), Bedrock.version(), keyword() ->
                                   {:ok, binary()} | {:error, atom()})

  @type storage_get_key_selector_fn() :: (pid(), KeySelector.t(), Bedrock.version(), keyword() ->
                                            {:ok, binary()} | {:error, atom()})

  @doc """
  Get a regular key within the transaction context.

  Expects pre-encoded keys and returns raw values.
  """
  @spec get_key(
          State.t(),
          key :: Bedrock.key(),
          opts :: [storage_get_key_fn: storage_get_key_fn(), snapshot: boolean()]
        ) ::
          {State.t(),
           {:ok, {Bedrock.key(), Bedrock.value()}}
           | {:error, :not_found}
           | {:failure,
              %{
                (:timeout
                 | :unavailable
                 | :version_too_old
                 | :version_too_new
                 | :no_servers_to_race
                 | :layout_lookup_failed) => [pid()]
              }}}
  def get_key(t, key, opts \\ []) do
    case Tx.repeatable_read(t.tx, key) do
      nil ->
        get_key_from_storage(t, key, opts)

      :clear ->
        {t, {:error, :not_found}}

      value ->
        {t, {:ok, {key, value}}}
    end
  end

  @doc """
  Resolve a bounded exact-key set while preserving read-your-writes,
  repeatable-read, point-conflict, and storage-racing semantics.
  """
  @spec get_many(State.t(), [Bedrock.key()], keyword()) ::
          {State.t(), {:ok, %{Bedrock.key() => Bedrock.value() | nil}} | {:failure, map()}}
  def get_many(t, keys, opts \\ []) when is_list(keys) do
    {known, unresolved} = partition_repeatable_reads(t.tx, keys)

    case unresolved do
      [] ->
        {t, {:ok, known}}

      unresolved ->
        get_many_from_storage(t, unresolved, known, opts)
    end
  end

  defp partition_repeatable_reads(tx, keys) do
    keys
    |> Enum.reduce({%{}, []}, fn key, {known, unresolved} ->
      case Tx.repeatable_read(tx, key) do
        nil -> {known, [key | unresolved]}
        :clear -> {Map.put(known, key, nil), unresolved}
        value -> {Map.put(known, key, value), unresolved}
      end
    end)
    |> then(fn {known, unresolved} -> {known, Enum.reverse(unresolved)} end)
  end

  defp get_many_from_storage(t, keys, known, opts) do
    with {:ok, t} <- ensure_read_version(t, opts),
         {:ok, groups} <- group_keys_by_layout(t, keys) do
      t = record_many_read_intents(t, keys, opts)

      Enum.reduce_while(groups, {t, {:ok, known}}, fn {_range, group_keys}, {state, {:ok, values}} ->
        case fetch_key_group(state, group_keys, opts) do
          {state, {:ok, fetched}} ->
            {:cont, {merge_many_storage_reads(state, fetched, opts), {:ok, Map.merge(values, fetched)}}}

          {state, {:failure, failures}} ->
            {:halt, {state, {:failure, failures}}}
        end
      end)
    else
      {:failure, failures} -> {t, {:failure, failures}}
      {:error, :layout_lookup_failed} -> {t, {:failure, %{layout_lookup_failed: []}}}
    end
  end

  defp group_keys_by_layout(t, keys) do
    {:ok,
     keys
     |> Enum.group_by(fn key ->
       {range, _servers} = LayoutIndex.lookup_key!(t.layout_index, key)
       range
     end)
     |> Enum.sort_by(&elem(&1, 0))}
  rescue
    RuntimeError -> {:error, :layout_lookup_failed}
  end

  defp record_many_read_intents(t, keys, opts) do
    if Keyword.get(opts, :snapshot, false) do
      t
    else
      tx = Enum.reduce(keys, t.tx, &Tx.add_read_conflict_key(&2, &1))
      %{t | tx: tx}
    end
  end

  defp fetch_key_group(t, keys, opts) do
    storage_get_many_fn = Keyword.get(opts, :storage_get_many_fn, &Materializer.get_many/4)

    operation = fn storage, version, timeout ->
      case storage_get_many_fn.(storage, keys, version, timeout: timeout) do
        {:error, :unsupported} -> fallback_get_many(storage, keys, version, timeout)
        result -> result
      end
    end

    case StorageRacing.race_storage_servers(t, hd(keys), operation) do
      {t, {:ok, {values, _range}}} when is_map(values) -> {t, {:ok, values}}
      {t, {:failure, failures}} -> {t, {:failure, failures}}
    end
  end

  defp fallback_get_many(storage, keys, version, timeout) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, values} ->
      case Materializer.get(storage, key, version, timeout: timeout) do
        {:ok, value} -> {:cont, {:ok, Map.put(values, key, value)}}
        {:error, :not_found} -> {:cont, {:ok, Map.put(values, key, nil)}}
        error -> {:halt, error}
      end
    end)
  end

  defp merge_many_storage_reads(t, values, opts) do
    if Keyword.get(opts, :snapshot, false) do
      t
    else
      tx =
        Enum.reduce(values, t.tx, fn
          {key, nil}, tx -> Tx.merge_storage_read(tx, key, :not_found)
          {key, value}, tx -> Tx.merge_storage_read(tx, key, value)
        end)

      %{t | tx: tx}
    end
  end

  defp get_key_from_storage(t, key, opts) do
    storage_get_key_fn = Keyword.get(opts, :storage_get_key_fn, &Materializer.get/4)

    case ensure_read_version(t, opts) do
      {:ok, t} ->
        t = record_read_intent(t, key, opts)

        execute_get_query(
          t,
          key,
          &(&1 |> storage_get_key_fn.(key, &2, timeout: &3) |> wrap_storage_get_result(key)),
          opts
        )

      {:failure, failures_by_reason} ->
        {t, {:failure, failures_by_reason}}
    end
  end

  # The conflict is registered when the read is issued, not when the result
  # returns: a read that comes back empty still constrains the transaction's
  # outcome, so the resolver must see it regardless of the result's shape.
  @spec record_read_intent(State.t(), Bedrock.key(), keyword()) :: State.t()
  defp record_read_intent(t, key, opts) do
    if Keyword.get(opts, :snapshot, false) do
      t
    else
      %{t | tx: Tx.add_read_conflict_key(t.tx, key)}
    end
  end

  defp wrap_storage_get_result({:ok, raw_value}, key), do: {:ok, {key, raw_value}}
  defp wrap_storage_get_result({:error, reason}, _key), do: {:error, reason}
  defp wrap_storage_get_result({:failure, reason, storage_id}, _key), do: {:failure, reason, storage_id}

  @doc """
  Get a KeySelector within the transaction context.
  """
  @spec get_key_selector(
          State.t(),
          KeySelector.t(),
          opts :: [storage_get_key_selector_fn: storage_get_key_selector_fn()]
        ) ::
          {State.t(),
           {:ok, Bedrock.key_value()}
           | {:error, :not_found}
           | {:failure,
              %{
                (:timeout
                 | :unavailable
                 | :version_too_old
                 | :version_too_new
                 | :no_servers_to_race
                 | :layout_lookup_failed) => [pid()]
              }}}
  def get_key_selector(t, %KeySelector{} = key_selector, opts \\ []) do
    storage_get_key_selector_fn = Keyword.get(opts, :storage_get_key_selector_fn, &Materializer.get/4)

    case ensure_read_version(t, opts) do
      {:ok, t} ->
        execute_selector_query(
          t,
          key_selector,
          &case storage_get_key_selector_fn.(&1, key_selector, &2, timeout: &3) do
            {:ok, nil} -> {:ok, nil}
            {:ok, {resolved_key, value}} -> {:ok, {resolved_key, value}}
            {:error, reason} -> {:error, reason}
            {:failure, reason, storage_id} -> {:failure, reason, storage_id}
          end,
          opts
        )

      {:failure, failures_by_reason} ->
        {t, {:failure, failures_by_reason}}
    end
  end

  # Private helper functions

  # A selector's conflict range can only be computed after resolution: any
  # mutation between the anchor key and the resolved key would change what the
  # selector resolves to, so the whole scanned span must reach the resolver.
  # When nothing resolves, the scanned shard range was read as empty and is
  # recorded whole.
  defp execute_selector_query(state, key_selector, operation_fn, opts) do
    snapshot = Keyword.get(opts, :snapshot, false)

    state
    |> StorageRacing.race_storage_servers(key_selector.key, operation_fn)
    |> case do
      {state, {:failure, failures_by_reason}} ->
        {state, {:failure, failures_by_reason}}

      {state, {:ok, {nil, {shard_start, shard_end}}}} ->
        state =
          if snapshot do
            state
          else
            %{state | tx: Tx.add_read_conflict_range(state.tx, shard_start, shard_end)}
          end

        {state, {:error, :not_found}}

      {state, {:ok, {{resolved_key, nil}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            {span_start, span_end} = selector_scan_span(key_selector.key, resolved_key)

            tx =
              state.tx
              |> Tx.merge_storage_read(resolved_key, :not_found)
              |> Tx.add_read_conflict_range(span_start, span_end)

            %{state | tx: tx}
          end

        {state, {:error, :not_found}}

      {state, {:ok, {{resolved_key, value}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            {span_start, span_end} = selector_scan_span(key_selector.key, resolved_key)

            tx =
              state.tx
              |> Tx.merge_storage_read(resolved_key, value)
              |> Tx.add_read_conflict_range(span_start, span_end)

            %{state | tx: tx}
          end

        {state, {:ok, {resolved_key, value}}}
    end
  end

  defp selector_scan_span(anchor, resolved_key) when anchor <= resolved_key, do: {anchor, Key.key_after(resolved_key)}

  defp selector_scan_span(anchor, resolved_key), do: {resolved_key, Key.key_after(anchor)}

  defp execute_get_query(state, racing_key, operation_fn, opts) do
    snapshot = Keyword.get(opts, :snapshot, false)

    state
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      {state, {:failure, failures_by_reason}} ->
        {state, {:failure, failures_by_reason}}

      {state, {:ok, {nil, _shard_range}}} ->
        {state, {:error, :not_found}}

      {state, {:ok, {{key, nil}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            %{state | tx: Tx.merge_storage_read(state.tx, key, :not_found)}
          end

        {state, {:error, :not_found}}

      {state, {:ok, {{key, value}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            %{state | tx: Tx.merge_storage_read(state.tx, key, value)}
          end

        {state, {:ok, {key, value}}}
    end
  end
end
