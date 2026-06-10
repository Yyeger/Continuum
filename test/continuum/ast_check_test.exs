defmodule Continuum.AstCheckTest do
  use ExUnit.Case, async: true

  alias Continuum.AstCheck

  describe "scan/2" do
    test "passes through pure code" do
      ast =
        quote do
          fn items ->
            Enum.reduce(items, 0, fn x, acc -> x.price + acc end)
          end
        end

      assert :ok == AstCheck.scan(ast)
    end

    test "rejects DateTime.utc_now/0 with a remediation hint" do
      ast =
        quote do
          DateTime.utc_now()
        end

      assert {:error, [violation]} = AstCheck.scan(ast, "test/fake.ex")
      assert violation.mfa == {DateTime, :utc_now}
      assert violation.hint =~ "Continuum.now/0"
      assert violation.file == "test/fake.ex"
    end

    test "rejects :rand.uniform/0" do
      ast =
        quote do
          :rand.uniform()
        end

      assert {:error, [violation]} = AstCheck.scan(ast)
      assert violation.mfa == {:rand, :uniform}
      assert violation.hint =~ "Continuum.random/0"
    end

    test "rejects ETS access" do
      ast =
        quote do
          :ets.lookup(:my_table, :my_key)
        end

      assert {:error, [violation]} = AstCheck.scan(ast)
      assert violation.mfa == {:ets, :lookup}
    end

    test "rejects raw Process.send_after (and the self() inside it)" do
      ast =
        quote do
          Process.send_after(self(), :wake, 1000)
        end

      assert {:error, violations} = AstCheck.scan(ast)
      assert Enum.map(violations, & &1.mfa) == [{Process, :send_after}, {Kernel, :self}]

      assert Enum.find(violations, &(&1.mfa == {Process, :send_after})).hint =~
               "Continuum.timer/1"
    end

    test "rejects Continuum facade calls that would mutate runtime state" do
      ast =
        quote do
          Continuum.start(MyFlow, %{id: 1})
          Continuum.signal("run-id", :approved, :ok)
          Continuum.cancel("run-id")
          Continuum.await("run-id", 1_000)
        end

      assert {:error, violations} = AstCheck.scan(ast)

      mfas = Enum.map(violations, & &1.mfa)
      assert {Continuum, :start} in mfas
      assert {Continuum, :signal} in mfas
      assert {Continuum, :cancel} in mfas
      assert {Continuum, :await} in mfas

      signal_violation = Enum.find(violations, &(&1.mfa == {Continuum, :signal}))
      assert signal_violation.hint =~ "signal/3 is a side effect"
    end

    test "rejects cluster topology and remote call APIs" do
      ast =
        quote do
          :pg.get_members(:continuum, {:default, "run"})
          :rpc.call(:node, Mod, :fun, [])
          :erpc.call(:node, Mod, :fun, [])
        end

      assert {:error, violations} = AstCheck.scan(ast)

      mfas = Enum.map(violations, & &1.mfa)
      assert {:pg, :get_members} in mfas
      assert {:rpc, :call} in mfas
      assert {:erpc, :call} in mfas
    end

    test "rejects File.read! deep inside an expression" do
      ast =
        quote do
          fn ->
            data = File.read!("/etc/secrets")
            String.split(data, "\n")
          end
        end

      assert {:error, [violation]} = AstCheck.scan(ast)
      assert violation.mfa == {File, :read!}
    end

    test "collects multiple violations" do
      ast =
        quote do
          fn ->
            now = DateTime.utc_now()
            r = :rand.uniform()
            {now, r}
          end
        end

      assert {:error, violations} = AstCheck.scan(ast)
      assert length(violations) == 2

      mfas = Enum.map(violations, & &1.mfa)
      assert {DateTime, :utc_now} in mfas
      assert {:rand, :uniform} in mfas
    end

    test "rejects unqualified Kernel calls: apply, spawn, send, self, make_ref, node" do
      ast =
        quote do
          fn ->
            apply(DateTime, :utc_now, [])
            spawn(fn -> :ok end)
            spawn_link(fn -> :ok end)
            send(some_pid, :message)
            make_ref()
            node()
          end
        end

      assert {:error, violations} = AstCheck.scan(ast)
      mfas = Enum.map(violations, & &1.mfa)

      assert {Kernel, :apply} in mfas
      assert {Kernel, :spawn} in mfas
      assert {Kernel, :spawn_link} in mfas
      assert {Kernel, :send} in mfas
      assert {Kernel, :make_ref} in mfas
      assert {Kernel, :node} in mfas
    end

    test "does not flag local calls that merely share a banned name at another arity" do
      ast =
        quote do
          send(a, b, c)
        end

      assert :ok == AstCheck.scan(ast)
    end

    test "rejects receive blocks outright" do
      ast =
        quote do
          receive do
            :go -> :ok
          after
            5_000 -> :timeout
          end
        end

      assert {:error, [violation]} = AstCheck.scan(ast)
      assert violation.mfa == {Kernel.SpecialForms, :receive}
      assert violation.hint =~ "await signal"
    end

    test "resolves in-body imports: bare utc_now() after import DateTime is rejected" do
      ast =
        quote do
          import DateTime
          utc_now()
        end

      assert {:error, [violation]} = AstCheck.scan(ast)
      assert violation.mfa == {DateTime, :utc_now}
      assert violation.hint =~ "Continuum.now/0"
    end

    test "import only:/except: lists are honored when literal" do
      excluded =
        quote do
          import DateTime, only: [to_unix: 1]
          utc_now()
        end

      assert :ok == AstCheck.scan(excluded)

      included =
        quote do
          import DateTime, only: [utc_now: 0]
          utc_now()
        end

      assert {:error, [%{mfa: {DateTime, :utc_now}}]} = AstCheck.scan(included)

      excepted =
        quote do
          import DateTime, except: [utc_now: 0]
          utc_now()
        end

      assert :ok == AstCheck.scan(excepted)
    end

    test "format/1 produces a readable diagnostic" do
      {:error, violations} =
        AstCheck.scan(quote(do: DateTime.utc_now()), "test/foo.ex")

      output = AstCheck.format(violations)
      assert output =~ "Determinism violation"
      assert output =~ "Continuum.now/0"
    end
  end

  describe "denylist coverage" do
    test "every entry has a non-empty hint" do
      for {{mod, fun}, hint} <- AstCheck.forbidden_calls() do
        assert is_atom(mod), "module must be an atom: #{inspect({mod, fun})}"
        assert is_atom(fun), "function must be an atom: #{inspect({mod, fun})}"
        assert is_binary(hint) and hint != "", "missing hint for #{inspect({mod, fun})}"
      end
    end

    test "trusted stdlib modules are non-empty atoms" do
      for mod <- AstCheck.trusted_stdlib() do
        assert is_atom(mod)
      end
    end
  end
end
