defmodule Continuum.AstCheck do
  @moduledoc """
  Compile-time AST scanner that rejects calls known to be non-deterministic
  inside workflow code.

  The scanner is invoked from `Continuum.Workflow` (and `Continuum.Pure`) at
  module compile time. Each forbidden call produces a `CompileError` with a
  remediation hint pointing at the deterministic equivalent.

  See the `:forbidden_calls/0` and `:trusted_stdlib/0` functions for the
  curated denylist and allowlist. Users can extend the allowlist via:

      config :continuum, trusted_modules: [Decimal, Money]
  """

  @typedoc "A `{module, function}` pair."
  @type call :: {module(), atom()}

  @typedoc "A violation found during AST scan."
  @type violation :: %{
          mfa: call(),
          line: pos_integer() | nil,
          file: String.t() | nil,
          hint: String.t()
        }

  @forbidden %{
    {DateTime, :utc_now} => "use Continuum.now/0",
    {DateTime, :now!} => "use Continuum.now/0",
    {DateTime, :now} => "use Continuum.now/0",
    {Date, :utc_today} => "use Continuum.today/0",
    {NaiveDateTime, :utc_now} => "use Continuum.now/0 then NaiveDateTime.from_iso8601!",
    {Time, :utc_now} => "use Continuum.now/0 |> DateTime.to_time/1",
    {:rand, :uniform} => "use Continuum.random/0",
    {:rand, :uniform_real} => "use Continuum.random/0",
    {:rand, :seed} => "rand seeding is non-deterministic on replay",
    {System, :os_time} => "use Continuum.now/0",
    {System, :system_time} => "use Continuum.now/0",
    {System, :monotonic_time} => "monotonic time is non-deterministic on replay",
    {System, :unique_integer} => "use Continuum.uuid4/0 instead",
    {System, :get_env} => "read env at workflow start; pass it as input",
    {System, :fetch_env!} => "read env at workflow start; pass it as input",
    {Node, :list} => "cluster topology is non-deterministic; wrap in an activity",
    {Node, :self} => "use Continuum.workflow_info/0",
    {Process, :send} => "send messages outside the workflow, or wrap in an activity",
    {Process, :send_after} => "use Continuum.timer/1",
    {Process, :sleep} => "use Continuum.timer/1",
    {Process, :spawn} => "spawn forbidden in workflows; use child workflows",
    {Continuum, :start} =>
      "start workflows outside workflow code; child workflows are planned post-v0.1",
    {Continuum, :signal} =>
      "signal/3 is a side effect; wrap it in Continuum.activity/2 or wait for Continuum.signal_child/2 (v0.3+)",
    {Continuum, :__generate_uuid4__} => "use Continuum.uuid4/0",
    {Continuum, :cancel} =>
      "cancel workflows outside workflow code or wrap cancellation in an activity",
    {Continuum, :await} =>
      "await/2 polls workflow state; use the workflow DSL's await signal(...) form",
    {IO, :puts} => "use Continuum.log/1 (journaled)",
    {IO, :inspect} => "use Continuum.log/1 (journaled)",
    {IO, :write} => "use Continuum.log/1 (journaled)",
    {File, :read} => "wrap in Continuum.activity/2",
    {File, :read!} => "wrap in Continuum.activity/2",
    {File, :write} => "wrap in Continuum.activity/2",
    {File, :write!} => "wrap in Continuum.activity/2",
    {:ets, :lookup} => "ETS bypasses the journal; wrap in an activity",
    {:ets, :insert} => "ETS bypasses the journal; wrap in an activity",
    {:persistent_term, :get} => "persistent_term bypasses the journal",
    {:persistent_term, :put} => "persistent_term bypasses the journal",
    {Kernel, :apply} => "dynamic dispatch forbidden in workflows",
    {Kernel, :spawn} => "spawn forbidden in workflows",
    {Kernel, :spawn_link} => "spawn forbidden in workflows",
    {Code, :eval_string} => "code evaluation is non-deterministic",
    {Code, :eval_quoted} => "code evaluation is non-deterministic",
    {:erlang, :now} => "use Continuum.now/0 (and :erlang.now is deprecated)",
    {:erlang, :spawn} => "spawn forbidden in workflows",
    {:erlang, :phash2} => "phash2 salt may change across OTP releases"
  }

  @trusted_stdlib MapSet.new([
                    Enum,
                    Stream,
                    Map,
                    String,
                    Integer,
                    Float,
                    Decimal,
                    Tuple,
                    List,
                    Keyword,
                    MapSet,
                    Range,
                    Base,
                    URI,
                    Regex,
                    Kernel,
                    Bitwise,
                    Access,
                    Function
                  ])

  @doc "The full denylist as a map of `{mod, fun} => hint`."
  def forbidden_calls, do: @forbidden

  @doc "Stdlib modules considered pure-by-construction."
  def trusted_stdlib, do: @trusted_stdlib

  @doc """
  Scan an AST. Returns `:ok` or `{:error, [violation]}`.

  Pass the originating `file` so diagnostics include it.
  """
  @spec scan(Macro.t(), String.t() | nil) :: :ok | {:error, [violation()]}
  def scan(ast, file \\ nil) do
    {_, violations} = Macro.prewalk(ast, [], &check_node(&1, &2, file))

    case Enum.reverse(violations) do
      [] -> :ok
      list -> {:error, list}
    end
  end

  @doc """
  Format a list of violations into a single human-readable string suitable
  for `CompileError`.
  """
  @spec format([violation()]) :: String.t()
  def format(violations) do
    violations
    |> Enum.map(&format_violation/1)
    |> Enum.join("\n\n")
  end

  # ---------------------------------------------------------------------------

  defp check_node({{:., _, [{:__aliases__, _, alias_parts}, fun]}, meta, args} = node, acc, file)
       when is_atom(fun) and is_list(args) do
    mod = Module.concat(alias_parts)
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  defp check_node({{:., _, [mod, fun]}, meta, args} = node, acc, file)
       when is_atom(mod) and is_atom(fun) and is_list(args) do
    {node, maybe_record(mod, fun, meta, file, acc)}
  end

  defp check_node(node, acc, _file), do: {node, acc}

  defp maybe_record(mod, fun, meta, file, acc) do
    case Map.fetch(@forbidden, {mod, fun}) do
      {:ok, hint} ->
        violation = %{
          mfa: {mod, fun},
          line: Keyword.get(meta, :line),
          file: file,
          hint: hint
        }

        [violation | acc]

      :error ->
        acc
    end
  end

  defp format_violation(%{mfa: {mod, fun}, line: line, file: file, hint: hint}) do
    location =
      case {file, line} do
        {nil, nil} -> ""
        {nil, l} -> "  line #{l}\n"
        {f, nil} -> "  #{f}\n"
        {f, l} -> "  #{f}:#{l}\n"
      end

    """
    == Determinism violation ==
    #{location}      #{Inspect.Atom.inspect(mod, %Inspect.Opts{})}.#{fun}

      #{hint}
    """
    |> String.trim_trailing()
  end
end
