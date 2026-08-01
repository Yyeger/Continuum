defmodule Continuum.TestRuntimeIsolationTest do
  use ExUnit.Case, async: false

  test "global test runtime omits database-polling background workers" do
    refute Process.whereis(Continuum.Runtime.Recovery)
    refute Process.whereis(Continuum.Runtime.Dispatcher)
    refute Process.whereis(Continuum.Runtime.ActivityWorker.Dispatcher)
    refute Process.whereis(Continuum.Runtime.Snapshotter)
    refute Process.whereis(Continuum.Runtime.TimerWheel)
    refute Process.whereis(Continuum.Runtime.SignalRouter)
    refute Process.whereis(Continuum.VersionRegistry)
  end
end
