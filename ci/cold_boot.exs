repo = Continuum.Test.Repo
repo_config = Application.fetch_env!(:continuum, repo)

# Model the documented dependency startup order: Continuum's application boots
# before the host Repo is reachable, then the host starts its Repo followed by
# Continuum.children/1.
Application.put_env(:continuum, repo, Keyword.put(repo_config, :port, 1))

case Application.ensure_all_started(:continuum) do
  {:ok, _applications} -> :ok
  {:error, reason} -> raise "Continuum application cold boot failed: #{inspect(reason)}"
end

if Process.whereis(repo) do
  raise "Continuum application unexpectedly started the host Repo"
end

Application.put_env(:continuum, repo, repo_config)

case repo.start_link() do
  {:ok, _pid} -> :ok
  {:error, {:already_started, _pid}} -> :ok
  {:error, reason} -> raise "host Repo failed to start after cold boot: #{inspect(reason)}"
end

{:ok, runtime} =
  Supervisor.start_link(Continuum.children(),
    strategy: :one_for_one,
    name: Continuum.Test.ColdBootRuntimeSupervisor
  )

children = Supervisor.which_children(runtime)

unless Enum.any?(children, fn
         {{Continuum.Runtime.Lease.Heartbeater, Continuum}, pid, :worker, _modules}
         when is_pid(pid) ->
           true

         _child ->
           false
       end) do
  raise "documented host runtime did not start after the Repo"
end

IO.puts("documented host cold boot passed")
