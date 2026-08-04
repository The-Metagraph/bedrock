defmodule Bedrock.DataPlane.Log.Shale.WriterTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Writer

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "test_segment.log")
    File.write!(path, :binary.copy(<<0>>, 1024))
    {:ok, writer} = Writer.open(path)
    %{path: path, writer: writer}
  end

  describe "open/1" do
    test "successfully opens a file and marks its new header dirty", %{path: path} do
      # Use pattern matching to assert all fields and that fd is present
      assert {:ok,
              %Writer{
                fd: fd,
                write_offset: 4,
                bytes_remaining: 1004,
                sync_fun: sync_fun,
                dirty?: true
              }} = Writer.open(path)

      assert fd
      assert is_function(sync_fun, 1)
    end

    test "uses custom sync function from options", %{path: path} do
      sync_fun = fn _fd -> :ok end

      assert {:ok, %Writer{sync_fun: writer_sync_fun}} =
               Writer.open(path, sync_fun: sync_fun)

      assert writer_sync_fun == sync_fun
    end
  end

  describe "close/1" do
    test "successfully closes the file descriptor", %{writer: writer} do
      assert :ok = Writer.close(writer)
    end

    test "returns :ok when writer is nil" do
      assert :ok = Writer.close(nil)
    end
  end

  describe "append/3 and append/4" do
    test "returns :segment_full error when there is not enough space", %{writer: writer} do
      large_transaction = :binary.copy(<<0>>, 1016)
      commit_version = <<1::unsigned-big-64>>
      assert {:error, :segment_full} = Writer.append(writer, large_transaction, commit_version)
    end

    test "immediate append synchronizes and updates the writer struct", %{path: path} do
      caller = self()

      sync_fun = fn _fd ->
        send(caller, :synced)
        :ok
      end

      assert {:ok, writer} = Writer.open(path, sync_fun: sync_fun)
      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      # Use pattern matching to assert the exact structure and values
      assert {:ok, %Writer{write_offset: 24, bytes_remaining: 984, dirty?: false}} =
               Writer.append(writer, transaction, commit_version)

      assert_receive :synced
    end

    test "returns error when writing to closed file descriptor", %{writer: writer} do
      # Close the file descriptor to cause pwrite to fail
      :ok = Writer.close(writer)

      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      # Writing to a closed file should return an error
      assert {:error, _reason} = Writer.append(writer, transaction, commit_version)
    end

    test "returns sync error and does not advance caller offsets", %{path: path} do
      sync_fun = fn _fd -> {:error, :eio} end
      assert {:ok, writer} = Writer.open(path, sync_fun: sync_fun)

      transaction = <<1, 2, 3, 4>>
      commit_version = <<1::unsigned-big-64>>

      assert {:error, :eio} = Writer.append(writer, transaction, commit_version)
      assert writer.write_offset == 4
      assert writer.bytes_remaining == 1004
      assert :ok = Writer.close(writer)
    end

    test "deferred append advances offsets without synchronizing", %{path: path} do
      caller = self()

      sync_fun = fn _fd ->
        send(caller, :synced)
        :ok
      end

      assert {:ok, writer} = Writer.open(path, sync_fun: sync_fun)
      transaction = <<1, 2, 3, 4>>

      assert {:ok, %Writer{write_offset: 24, bytes_remaining: 984, dirty?: true} = writer} =
               Writer.append(writer, transaction, <<1::unsigned-big-64>>, :deferred)

      refute_receive :synced, 20
      assert {:ok, %Writer{dirty?: false}} = Writer.sync(writer)
      assert_receive :synced
    end

    test "immediate and deferred appends produce identical WAL bytes", %{tmp_dir: tmp_dir} do
      immediate_path = Path.join(tmp_dir, "immediate.log")
      deferred_path = Path.join(tmp_dir, "deferred.log")
      File.write!(immediate_path, :binary.copy(<<0>>, 1024))
      File.write!(deferred_path, :binary.copy(<<0>>, 1024))
      transaction = <<1, 2, 3, 4>>
      version = <<1::unsigned-big-64>>

      assert {:ok, immediate} = Writer.open(immediate_path)
      assert {:ok, immediate} = Writer.append(immediate, transaction, version)
      assert :ok = Writer.close(immediate)

      assert {:ok, deferred} = Writer.open(deferred_path)
      assert {:ok, deferred} = Writer.append(deferred, transaction, version, :deferred)
      assert {:ok, deferred} = Writer.sync(deferred)
      assert :ok = Writer.close(deferred)

      assert File.read!(immediate_path) == File.read!(deferred_path)
    end

    test "rejects unsupported durability policies", %{writer: writer} do
      assert {:error, {:invalid_durability, :sometimes}} =
               Writer.append(writer, <<1>>, <<1::unsigned-big-64>>, :sometimes)
    end
  end

  describe "sync/1" do
    test "is a no-op for a clean writer", %{path: path} do
      caller = self()

      sync_fun = fn _fd ->
        send(caller, :synced)
        :ok
      end

      assert {:ok, writer} = Writer.open(path, sync_fun: sync_fun)
      assert {:ok, writer} = Writer.sync(writer)
      assert_receive :synced
      assert {:ok, ^writer} = Writer.sync(writer)
      refute_receive :synced, 20
    end

    test "preserves dirty state for retry when synchronization fails", %{path: path} do
      assert {:ok, writer} = Writer.open(path, sync_fun: fn _fd -> {:error, :eio} end)

      assert {:ok, %Writer{dirty?: true} = writer} =
               Writer.append(writer, <<1>>, <<1::unsigned-big-64>>, :deferred)

      assert {:error, :eio} = Writer.sync(writer)
      assert writer.dirty?
    end

    test "close does not implicitly synchronize a deferred writer", %{path: path} do
      caller = self()

      assert {:ok, writer} =
               Writer.open(path,
                 sync_fun: fn _fd ->
                   send(caller, :synced)
                   :ok
                 end
               )

      assert {:ok, writer} = Writer.append(writer, <<1>>, <<1::unsigned-big-64>>, :deferred)
      assert :ok = Writer.close(writer)
      refute_receive :synced, 20
    end
  end
end
