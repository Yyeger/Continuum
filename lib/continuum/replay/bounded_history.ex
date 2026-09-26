defmodule Continuum.Replay.BoundedHistory do
  @moduledoc false
  import Ecto.Query

  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{Event, Run, Snapshot}

  # The CASE expressions apply size limits in PostgreSQL before transferring
  # payloads to the replay process. Checking sizes after repo.get/all would
  # already have allocated an arbitrarily large history in the caller.
  def fetch_run(instance, id, limits) do
    max_bytes = limits.payload_bytes

    row =
      instance.repo.one(
        from(r in Run,
          where: r.id == ^id,
          select: %{
            id: r.id,
            workflow: r.workflow,
            version_hash: r.version_hash,
            namespace: r.namespace,
            state: r.state,
            input:
              fragment(
                "CASE WHEN octet_length(?) <= ? THEN ? ELSE NULL END",
                r.input,
                ^max_bytes,
                r.input
              ),
            result:
              fragment(
                "CASE WHEN octet_length(?) <= ? THEN ? ELSE NULL END",
                r.result,
                ^max_bytes,
                r.result
              ),
            too_large:
              fragment(
                "octet_length(?) > ? OR COALESCE(octet_length(?), 0) > ?",
                r.input,
                ^max_bytes,
                r.result,
                ^max_bytes
              )
          }
        )
      )

    case row do
      nil -> {:error, {:run_not_found, id}}
      %{too_large: true} -> {:error, :run_payload_too_large}
      row -> {:ok, row}
    end
  end

  def load(instance, id, entrypoint, opts) do
    limits = Keyword.fetch!(opts, :limits)

    with {:ok, snapshot, bytes} <- snapshot(instance, id, entrypoint, opts),
         {:ok, events} <- events(instance, id, snapshot, limits, bytes) do
      {:ok, {snapshot, events}}
    end
  end

  defp snapshot(instance, id, entrypoint, opts) do
    if Keyword.get(opts, :snapshot, true) do
      max_bytes = opts[:limits].history_bytes

      row =
        instance.repo.one(
          from(s in Snapshot,
            where: s.run_id == ^id,
            order_by: [desc: s.through_seq, desc: s.id],
            limit: 1,
            select: %{
              version_hash: s.version_hash,
              bytes: fragment("octet_length(?)", s.payload),
              payload:
                fragment(
                  "CASE WHEN octet_length(?) <= ? THEN ? ELSE NULL END",
                  s.payload,
                  ^max_bytes,
                  s.payload
                )
            }
          )
        )

      decode_snapshot(row, entrypoint, max_bytes)
    else
      {:ok, nil, 0}
    end
  end

  defp decode_snapshot(nil, _entrypoint, _max), do: {:ok, nil, 0}

  defp decode_snapshot(row, entrypoint, max) do
    cond do
      row.version_hash != entrypoint.__continuum_workflow__().version_hash ->
        {:ok, nil, 0}

      row.bytes > max ->
        {:error, :snapshot_too_large}

      true ->
        snapshot =
          row.payload
          |> Continuum.Snapshot.decode()
          |> Continuum.Replay.compatible_snapshot(entrypoint)

        {:ok, snapshot, if(snapshot, do: row.bytes, else: 0)}
    end
  end

  defp events(instance, id, snapshot, limits, snapshot_bytes) do
    after_seq = if snapshot, do: snapshot.through_seq, else: -1

    sql = """
    WITH bounded AS (
      SELECT seq, event_type, payload, octet_length(payload) AS bytes
      FROM continuum_events
      WHERE run_id = $1::text::uuid AND seq > $2
      ORDER BY seq LIMIT $3
    )
    SELECT seq, event_type,
      CASE WHEN bytes <= $4 AND sum(bytes) OVER () <= $5
        AND count(*) OVER () <= $6 THEN payload ELSE NULL END,
      bytes
    FROM bounded ORDER BY seq
    """

    params = [
      id,
      after_seq,
      limits.events + 1,
      limits.payload_bytes,
      limits.history_bytes - snapshot_bytes,
      limits.events
    ]

    with {:ok, %{rows: rows}} <- instance.repo.query(sql, params) do
      cond do
        length(rows) > limits.events ->
          {:error, :history_event_limit}

        Enum.any?(rows, fn [_, _, payload, _] -> is_nil(payload) end) ->
          {:error, :history_byte_limit}

        true ->
          {:ok,
           Enum.map(rows, fn [seq, type, payload, _] ->
             Postgres.decode_event(%Event{seq: seq, event_type: type, payload: payload})
           end)}
      end
    end
  end
end
