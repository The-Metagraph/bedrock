defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionPolicy do
  @moduledoc """
  Decides when a window advance should start a compaction.

  Olivine's index file is append-only: every window advance that evicts
  modified pages appends them, and nothing reclaims the superseded copies
  until a compaction rewrites the durable page map. Left alone, the index
  grows with the number of page rewrites rather than with the data.

  The trigger is relative to the last compaction's output, with an
  absolute floor so a small or freshly started worker does not compact
  on every advance: compact once

      index_bytes > max(floor_bytes, ratio * last_compacted_index_bytes)

  `last_compacted_index_bytes` starts at 0, so before the first
  compaction the floor alone decides.

  Both knobs come from the worker's manifest params, the same discipline
  as `SnapshotPolicy`: a knob is set only by an explicit positive integer
  (`compaction_index_ratio`, `compaction_index_floor_bytes`); a missing or
  malformed param keeps the default. `SnapshotPolicy`, which gates the
  upload after a compaction, is independent of this one.
  """

  @default_ratio 4
  @default_floor_bytes 64 * 1024 * 1024

  @type t :: %__MODULE__{ratio: pos_integer(), floor_bytes: pos_integer()}
  defstruct ratio: @default_ratio, floor_bytes: @default_floor_bytes

  @spec from_params(%{optional(String.t()) => term()}) :: t()
  def from_params(params) do
    %__MODULE__{
      ratio: knob(params["compaction_index_ratio"], @default_ratio),
      floor_bytes: knob(params["compaction_index_floor_bytes"], @default_floor_bytes)
    }
  end

  defp knob(value, _default) when is_integer(value) and value > 0, do: value
  defp knob(_value, default), do: default

  @doc "The index size above which a compaction is due."
  @spec bound(t(), non_neg_integer()) :: non_neg_integer()
  def bound(%__MODULE__{ratio: ratio, floor_bytes: floor}, last_compacted_index_bytes),
    do: max(floor, ratio * last_compacted_index_bytes)

  @spec due?(t(), index_bytes :: non_neg_integer(), last_compacted_index_bytes :: non_neg_integer()) :: boolean()
  def due?(%__MODULE__{} = policy, index_bytes, last_compacted_index_bytes),
    do: index_bytes > bound(policy, last_compacted_index_bytes)
end
