defmodule Mix.Tasks.Continuum.Gen.ActivityTest do
  use ExUnit.Case, async: false

  test "generates an activity module" do
    in_tmp(fn ->
      Mix.Task.rerun("continuum.gen.activity", ["MyApp.Activities.ValidateOrder", "--path", "lib"])

      path = "lib/my_app/activities/validate_order.ex"
      assert File.exists?(path)
      assert File.read!(path) =~ "defmodule MyApp.Activities.ValidateOrder"
      assert File.read!(path) =~ "use Continuum.Activity"
    end)
  end

  defp in_tmp(fun) do
    root = Path.join(System.tmp_dir!(), "continuum-gen-activity-#{System.unique_integer()}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      File.cd!(root, fun)
    after
      File.rm_rf(root)
    end
  end
end
