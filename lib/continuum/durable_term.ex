defmodule Continuum.DurableTerm do
  @moduledoc """
  Validates values before they cross a Continuum journal boundary.

  ETF can encode node-local identities, but decoding them during replay does
  not make them meaningful on another process or node. This validator rejects
  those identities recursively and reports their exact location.
  """

  alias Continuum.DurableTermError

  @type path_segment :: atom() | {:field, term()} | {:index, non_neg_integer()} | :tail
  @type path :: [path_segment()]

  @spec validate(term(), atom()) :: :ok | {:error, DurableTermError.t()}
  def validate(term, root \\ :value) when is_atom(root), do: do_validate(term, [root])

  @spec validate!(term(), atom()) :: term()
  def validate!(term, root \\ :value) do
    case validate(term, root) do
      :ok -> term
      {:error, error} -> raise error
    end
  end

  @doc """
  Decode a term Continuum wrote across a journal boundary.

  Decoding is `:safe`, so it refuses to create atoms from journal bytes. On a
  cold node, Continuum loads the modules declared by the node's loaded OTP
  applications and retries once; this makes atoms from deployed workflow,
  activity, and runtime code available without trusting database text. A final
  refusal means one of three things: the stored bytes are corrupt, they were
  written by code this node does not have, or workflow code journaled a
  dynamically constructed atom — which was never replay-durable, because the
  atom does not exist on a node that has not run the same input.

  Raises `Continuum.DurableTermError` rather than leaking `ArgumentError` from
  `:erlang.binary_to_term/2`.
  """
  @spec decode!(binary()) :: term()
  def decode!(binary) when is_binary(binary) do
    case safe_decode(binary) do
      {:ok, term} ->
        term

      :error ->
        # A cold node may not have loaded the module containing an otherwise
        # legitimate domain atom yet. Load atoms from deployed application
        # code, never from the journal bytes, then retry the safe decoder.
        load_deployed_code_atoms()

        case safe_decode(binary) do
          {:ok, term} -> term
          :error -> raise %DurableTermError{path: nil, kind: :undecodable}
        end
    end
  end

  @doc "Non-raising `decode!/1`."
  @spec decode(binary()) :: {:ok, term()} | {:error, DurableTermError.t()}
  def decode(binary) when is_binary(binary) do
    {:ok, decode!(binary)}
  rescue
    error in DurableTermError -> {:error, error}
  end

  @doc false
  @spec atom_from_binary!(binary(), atom()) :: atom()
  def atom_from_binary!(binary, root \\ :journal_atom) when is_binary(binary) and is_atom(root) do
    case existing_atom(binary) do
      {:ok, atom} ->
        atom

      :error ->
        load_deployed_code_atoms()

        case existing_atom(binary) do
          {:ok, atom} -> atom
          :error -> raise %DurableTermError{path: [root], kind: :unknown_atom, value: binary}
        end
    end
  end

  @doc false
  @spec module_from_binary!(binary(), atom()) :: module()
  def module_from_binary!(binary, root \\ :journal_module)
      when is_binary(binary) and is_atom(root) do
    module_name = if String.starts_with?(binary, "Elixir."), do: binary, else: "Elixir." <> binary
    atom_from_binary!(module_name, root)
  end

  @doc false
  def format_path([root | segments]) do
    Enum.reduce(segments, Atom.to_string(root), fn
      {:field, key}, path when is_atom(key) -> path <> "." <> Atom.to_string(key)
      {:field, key}, path -> path <> "[#{inspect(key)}]"
      {:index, index}, path -> path <> "[#{index}]"
      :tail, path -> path <> ".tail"
    end)
  end

  defp do_validate(term, path) when is_pid(term), do: invalid(path, :PID)
  defp do_validate(term, path) when is_reference(term), do: invalid(path, :reference)
  defp do_validate(term, path) when is_port(term), do: invalid(path, :port)
  defp do_validate(term, path) when is_function(term), do: invalid(path, :function)

  defp do_validate(term, _path)
       when is_atom(term) or is_number(term) or is_bitstring(term),
       do: :ok

  defp do_validate([], _path), do: :ok

  defp do_validate([head | tail], path) do
    with :ok <- do_validate(head, path ++ [{:index, 0}]) do
      validate_list_tail(tail, path, 1)
    end
  end

  defp do_validate(term, path) when is_tuple(term) do
    term
    |> Tuple.to_list()
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {value, index}, :ok ->
      case do_validate(value, path ++ [{:index, index}]) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp do_validate(term, path) when is_map(term) do
    term
    |> :maps.to_list()
    |> Enum.reduce_while(:ok, fn {key, value}, :ok ->
      with :ok <- do_validate(key, path ++ [{:field, :__key__}]),
           :ok <- do_validate(value, path ++ [{:field, key}]) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
  end

  defp validate_list_tail([], _path, _index), do: :ok

  defp validate_list_tail([head | tail], path, index) do
    with :ok <- do_validate(head, path ++ [{:index, index}]) do
      validate_list_tail(tail, path, index + 1)
    end
  end

  defp validate_list_tail(tail, path, _index), do: do_validate(tail, path ++ [:tail])

  defp invalid(path, kind), do: {:error, %DurableTermError{path: path, kind: kind}}

  defp safe_decode(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  defp existing_atom(binary) do
    {:ok, String.to_existing_atom(binary)}
  rescue
    ArgumentError -> :error
  end

  # Loading a BEAM module interns the atoms from its atom table. Application
  # manifests are trusted deployed code and provide a bounded source; unlike
  # decoding journal bytes without `:safe`, this cannot turn database contents
  # into new atoms. This fallback runs only after a safe decode misses.
  defp load_deployed_code_atoms do
    Application.loaded_applications()
    |> Enum.each(fn {application, _description, _version} ->
      application
      |> Application.spec(:modules)
      |> List.wrap()
      |> Enum.each(&Code.ensure_loaded/1)
    end)

    :ok
  rescue
    _error -> :ok
  catch
    :exit, _reason -> :ok
  end
end
