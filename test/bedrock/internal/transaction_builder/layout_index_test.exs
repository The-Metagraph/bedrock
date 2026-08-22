defmodule Bedrock.Internal.TransactionBuilder.LayoutIndexTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.LayoutIndex

  test "adjacent shard ranges share one boundary without duplicating the tree key" do
    user_materializer = spawn(fn -> Process.sleep(:infinity) end)
    system_materializer = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Process.exit(user_materializer, :kill)
      Process.exit(system_materializer, :kill)
    end)

    layout = %{
      metadata_materializer: system_materializer,
      shard_layout: %{
        <<0xFF>> => {1, <<>>},
        <<0xFF, 0xFF>> => {0, <<0xFF>>}
      },
      shard_materializers: %{0 => system_materializer, 1 => user_materializer}
    }

    index = LayoutIndex.build_index(layout)

    assert {{<<>>, <<0xFF>>}, [^user_materializer]} = LayoutIndex.lookup_key!(index, "user-key")

    assert {{<<0xFF>>, <<0xFF, 0xFF>>}, [^system_materializer]} =
             LayoutIndex.lookup_key!(index, <<0xFF, 0x01>>)

    assert [
             {{<<>>, <<0xFF>>}, [^user_materializer]},
             {{<<0xFF>>, <<0xFF, 0xFF>>}, [^system_materializer]}
           ] = LayoutIndex.lookup_range(index, <<>>, <<0xFF, 0xFF>>)
  end
end
