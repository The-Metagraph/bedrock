defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  test "builds an ordered tree when adjacent shards share a boundary" do
    boundary = <<0xFF>>
    keyspace_end = <<0xFF, 0xFF>>

    layout = %{
      shard_layout: %{
        boundary => {1, <<>>},
        keyspace_end => {0, boundary}
      },
      metadata_materializer: self(),
      shard_materializers: %{1 => self()}
    }

    index = LayoutIndex.build_index(layout)

    assert {{<<>>, ^boundary}, [pid]} = LayoutIndex.lookup_key!(index, <<0>>)
    assert pid == self()
    assert {{^boundary, ^keyspace_end}, [pid]} = LayoutIndex.lookup_key!(index, boundary)
    assert pid == self()
  end
end
