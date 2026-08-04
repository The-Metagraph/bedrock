defmodule Bedrock.DataPlane.Log.Shale.RecoveryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log.Shale.Recovery
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  # Helper functions for common test patterns
  defp version(n), do: Version.from_integer(n)

  setup context do
    tmp_dir = context.tmp_dir
    segment_size = Map.get(context, :segment_size, 1024 * 1024)

    {:ok, recycler} =
      start_supervised({SegmentRecycler, path: tmp_dir, min_available: 1, max_available: 1, segment_size: segment_size})

    state = %State{
      mode: :locked,
      path: tmp_dir,
      segment_recycler: recycler,
      active_segment: nil,
      segments: [],
      writer: nil,
      oldest_version: version(0),
      last_version: version(0)
    }

    {:ok, state: state, tmp_dir: tmp_dir}
  end

  describe "recover_from/4" do
    test "returns error when not in locked mode", %{state: state} do
      unlocked_state = %{state | mode: :running}

      assert {:error, :lock_required} =
               Recovery.recover_from(
                 unlocked_state,
                 [:source],
                 version(1),
                 version(2)
               )
    end

    test "successfully recovers with no transactions (empty source list)", %{state: state} do
      expected_version = version(1)

      assert {:ok, %{mode: :running, oldest_version: ^expected_version, last_version: ^expected_version}} =
               Recovery.recover_from(
                 state,
                 [],
                 expected_version,
                 expected_version
               )
    end

    test "successfully recovers with no transactions (source log returns empty)", %{state: state} do
      source_log = setup_mock_log([])
      expected_version = version(1)

      assert {:ok, %{mode: :running, oldest_version: ^expected_version, last_version: ^expected_version}} =
               Recovery.recover_from(
                 state,
                 [source_log],
                 expected_version,
                 expected_version
               )
    end

    test "correctly handles recovery when first_version equals last_version", %{state: state} do
      # This test verifies the fix for the issue where logs would report having
      # version ranges but had no segments loaded, causing :not_found errors
      v = version(5)

      assert {:ok, %{mode: :running, oldest_version: ^v, last_version: ^v, active_segment: segment, writer: writer}} =
               Recovery.recover_from(
                 state,
                 [],
                 v,
                 v
               )

      assert segment
      assert writer
    end

    test "self-source recovery preserves the retained log segment and unlocks it", %{state: state} do
      first_version = version(0)
      last_version = version(10)
      {:ok, running_state} = Recovery.recover_from(state, [], first_version, first_version)

      locked_state = %{running_state | mode: :locked}
      retained_segment_path = locked_state.active_segment.path

      assert {:ok,
              %{
                mode: :running,
                oldest_version: ^first_version,
                last_version: ^last_version,
                active_segment: %{path: ^retained_segment_path},
                writer: writer
              }} =
               Recovery.recover_from(
                 locked_state,
                 [self()],
                 first_version,
                 last_version
               )

      assert writer
    end

    test "successfully recovers with valid transactions", %{state: state} do
      first_version = version(1)
      last_version = version(2)

      transactions = [
        create_encoded_tx(first_version, %{"data" => "test1"}),
        create_encoded_tx(last_version, %{"data" => "test2"})
      ]

      source_log = setup_mock_log(transactions)

      assert {:ok, %{mode: :running, oldest_version: ^first_version, last_version: ^last_version}} =
               Recovery.recover_from(
                 state,
                 [source_log],
                 first_version,
                 last_version
               )
    end

    test "handles unavailable source log", %{state: state} do
      source_log = setup_failing_mock_log(:unavailable)

      assert {:error, {:source_log_unavailable, ^source_log}, failed_state} =
               Recovery.recover_from(
                 state,
                 [source_log],
                 version(1),
                 version(2)
               )

      assert failed_state.mode == :locked
      assert is_nil(failed_state.writer)
      assert is_nil(failed_state.active_segment)
    end

    test "tries multiple sources when first is unavailable", %{state: state} do
      unavailable_source = setup_failing_mock_log(:unavailable)
      available_source = setup_mock_log([])
      first_version = version(1)

      # First source fails, second succeeds
      assert {:ok, %{mode: :running}} =
               Recovery.recover_from(
                 state,
                 [unavailable_source, available_source],
                 first_version,
                 first_version
               )
    end

    test "returns error when all sources unavailable", %{state: state} do
      source1 = setup_failing_mock_log(:unavailable)
      source2 = setup_failing_mock_log(:unavailable)

      assert {:error, {:source_log_unavailable, _}, failed_state} =
               Recovery.recover_from(
                 state,
                 [source1, source2],
                 version(1),
                 version(2)
               )

      assert failed_state.mode == :locked
      assert failed_state.segments == []
    end

    @tag segment_size: 2 * 1024 * 1024
    test "replays 10,000 heartbeat-heavy transactions with one final sync", %{
      state: state,
      tmp_dir: tmp_dir
    } do
      {:ok, sync_count} = Agent.start_link(fn -> 0 end)
      state = install_sync_spy(state, tmp_dir, sync_count)

      transactions =
        Enum.map(1..10_000, fn n ->
          data = if rem(n, 1_000) == 0, do: %{"mutation-#{n}" => "value"}, else: %{}
          create_encoded_tx(version(n), data)
        end)

      source_log = setup_mock_log(transactions)

      assert {:ok, recovered} =
               Recovery.recover_from(
                 state,
                 [source_log],
                 version(1),
                 version(10_000)
               )

      assert recovered.mode == :running
      assert recovered.writer.dirty? == false
      assert Agent.get(sync_count, & &1) == 1
      assert length(recovered.segments) + 1 == 1
      assert recovered_transactions(recovered) == transactions
    end

    @tag segment_size: 1024
    test "synchronizes exactly once per dirty segment across forced rotations", %{
      state: state,
      tmp_dir: tmp_dir
    } do
      {:ok, sync_count} = Agent.start_link(fn -> 0 end)
      state = install_sync_spy(state, tmp_dir, sync_count)

      transactions =
        Enum.map(1..80, fn n ->
          create_encoded_tx(version(n), %{"mutation-#{n}" => String.duplicate("x", 128)})
        end)

      source_log = setup_mock_log(transactions)

      assert {:ok, recovered} =
               Recovery.recover_from(state, [source_log], version(1), version(80))

      segment_count = length(recovered.segments) + 1
      assert segment_count > 1
      assert Agent.get(sync_count, & &1) == segment_count
      assert recovered_transactions(recovered) == transactions
    end

    test "does not publish a target when its final durability barrier fails", %{
      state: state,
      tmp_dir: tmp_dir
    } do
      state = install_sync_fun(state, tmp_dir, fn _fd -> {:error, :eio} end)
      transaction = create_encoded_tx(version(1), %{"mutation" => "value"})
      source_log = setup_mock_log([transaction])

      assert {:error, {:final_sync_failed, :eio}, failed_state} =
               Recovery.recover_from(state, [source_log], version(1), version(1))

      assert failed_state.mode == :locked
      assert is_nil(failed_state.writer)
      assert is_nil(failed_state.active_segment)
      assert failed_state.segments == []
    end

    @tag segment_size: 512
    test "does not close or publish a full segment when its rotation barrier fails", %{
      state: state,
      tmp_dir: tmp_dir
    } do
      state = install_sync_fun(state, tmp_dir, fn _fd -> {:error, :eio} end)

      transactions =
        Enum.map(1..20, fn n ->
          create_encoded_tx(version(n), %{"mutation-#{n}" => String.duplicate("x", 128)})
        end)

      source_log = setup_mock_log(transactions)

      assert {:error, {:segment_sync_failed, :eio}, failed_state} =
               Recovery.recover_from(state, [source_log], version(1), version(20))

      assert failed_state.mode == :locked
      assert is_nil(failed_state.writer)
      assert is_nil(failed_state.active_segment)
      assert failed_state.segments == []
    end

    test "rejects an incomplete source instead of assigning its expected final version", %{state: state} do
      source_log = setup_mock_log([])

      assert {:error, {:final_version_mismatch, expected, actual}, failed_state} =
               Recovery.recover_from(state, [source_log], version(1), version(2))

      assert expected == version(2)
      assert actual == version(1)
      assert failed_state.mode == :locked
    end

    test "resets partial output before retrying a healthy source", %{
      state: state,
      tmp_dir: tmp_dir
    } do
      {:ok, sync_count} = Agent.start_link(fn -> 0 end)
      state = install_sync_spy(state, tmp_dir, sync_count)
      first = create_encoded_tx(version(1), %{"first" => "partial"})
      last = create_encoded_tx(version(2), %{"last" => "complete"})
      partial_source = setup_paged_mock_log([{:ok, [first]}, {:error, :unavailable}])
      healthy_source = setup_mock_log([first, last])

      assert {:ok, recovered} =
               Recovery.recover_from(
                 state,
                 [partial_source, healthy_source],
                 version(1),
                 version(2)
               )

      assert recovered_transactions(recovered) == [first, last]
      assert Agent.get(sync_count, & &1) == 1
    end
  end

  describe "pull_transactions/4" do
    test "sets versions correctly when first_version equals last_version", %{state: state} do
      # This test covers both empty transaction list and version consistency scenarios
      v = version(10)
      source_log = setup_mock_log([])

      assert {:ok, %{oldest_version: ^v, last_version: ^v}} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 v,
                 v
               )
    end

    test "handles invalid transaction data", %{state: state} do
      source_log = setup_mock_log(["invalid"])

      assert {:error, :invalid_transaction} =
               Recovery.pull_transactions(
                 state,
                 source_log,
                 version(1),
                 version(2)
               )
    end
  end

  defp create_encoded_tx(version, data) do
    mutations = Enum.map(data, fn {key, value} -> {:set, key, value} end)

    transaction = %{
      mutations: mutations,
      read_conflicts: [],
      write_conflicts: [],
      read_version: nil
    }

    encoded = Transaction.encode(transaction)
    {:ok, encoded_with_id} = Transaction.add_commit_version(encoded, version)
    encoded_with_id
  end

  defp setup_mock_log(transactions) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:ok, transactions}})
      after
        500 -> :timeout
      end
    end)
  end

  defp setup_failing_mock_log(error) do
    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
          send(from, {ref, {:error, error}})
      after
        500 -> :timeout
      end
    end)
  end

  defp setup_paged_mock_log(responses) do
    spawn_link(fn -> serve_pages(responses) end)
  end

  defp serve_pages([response | rest]) do
    receive do
      {:"$gen_call", {from, ref}, {:pull, _version, _opts}} ->
        send(from, {ref, response})
        serve_pages(rest)
    after
      500 -> :timeout
    end
  end

  defp serve_pages([]), do: :ok

  defp install_sync_spy(state, tmp_dir, counter) do
    install_sync_fun(state, tmp_dir, fn _fd ->
      Agent.update(counter, &(&1 + 1))
      :ok
    end)
  end

  defp install_sync_fun(state, tmp_dir, sync_fun) do
    {:ok, segment} = Segment.allocate_from_recycler(state.segment_recycler, tmp_dir, version(0))
    {:ok, writer} = Writer.open(segment.path, sync_fun: sync_fun)
    %{state | active_segment: segment, writer: writer}
  end

  defp recovered_transactions(state) do
    state.segments
    |> Enum.reverse([state.active_segment])
    |> Enum.flat_map(fn segment -> segment.transactions |> List.wrap() |> Enum.reverse() end)
    |> tl()
  end
end
