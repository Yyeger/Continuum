defmodule Continuum.Runtime.NamespacePrecondition do
  @moduledoc false

  alias Continuum.Runtime.Instance
  alias Continuum.Runtime.Journal.Postgres

  @spec resolve(Instance.t(), module(), binary(), keyword()) ::
          {:ok, binary()} | {:error, :not_found | {:invalid_namespace, term()}}
  def resolve(%Instance{} = instance, journal, run_id, opts) do
    resolved_run_id = resolve_chain_tip(instance, journal, run_id)

    case Keyword.fetch(opts, :namespace) do
      :error ->
        {:ok, resolved_run_id}

      {:ok, namespace} ->
        with {:ok, namespace} <- normalize(namespace),
             %{namespace: ^namespace} <- journal.get_run(instance, resolved_run_id) do
          {:ok, resolved_run_id}
        else
          nil -> {:error, :not_found}
          %{namespace: _other} -> {:error, :not_found}
          %{} -> {:error, :not_found}
          {:error, _reason} = error -> error
        end
    end
  end

  @spec normalize(term()) :: {:ok, binary()} | {:error, {:invalid_namespace, term()}}
  def normalize(namespace) when is_binary(namespace) and byte_size(namespace) > 0,
    do: {:ok, namespace}

  def normalize(namespace), do: {:error, {:invalid_namespace, namespace}}

  defp resolve_chain_tip(%Instance{repo: nil}, _journal, run_id), do: run_id

  defp resolve_chain_tip(instance, Postgres, run_id),
    do: Postgres.resolve_chain_tip(instance, run_id)

  defp resolve_chain_tip(_instance, _journal, run_id), do: run_id
end
