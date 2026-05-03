defmodule Mix.Tasks.Continuum.Gen.WorkflowTest do
  use ExUnit.Case, async: false

  test "generates a workflow module" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.workflow", ["MyApp.OrderFlow", "--path", "lib"])

      path = "lib/my_app/order_flow.ex"
      assert File.exists?(path)
      assert File.read!(path) =~ "defmodule MyApp.OrderFlow"
      assert File.read!(path) =~ "use Continuum.Workflow"
    end)
  end

  defp in_tmp(fun) do
    root = Path.join(System.tmp_dir!(), "continuum-gen-workflow-#{System.unique_integer()}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      File.cd!(root, fun)
    after
      File.rm_rf(root)
    end
  end
end
