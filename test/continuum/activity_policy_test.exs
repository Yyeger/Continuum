defmodule Continuum.ActivityPolicyTest do
  use Continuum.Test.DataCase, async: false
  use ExUnitProperties

  alias Continuum.Activity.Policy
  alias Continuum.Runtime.Journal.Postgres
  alias Continuum.Schema.{ActivityTask, Event, Run}

  defmodule InvalidKeyActivity do
    use Continuum.Activity

    def run(_value), do: :ok
    def idempotency_key(_args), do: :not_binary
  end

  defmodule InvalidKeyFlow do
    use Continuum.Workflow, version: 1

    def run(input), do: activity(InvalidKeyActivity.run(input))
  end

  defmodule OverrideFlow do
    use Continuum.Workflow, version: 1

    def run(input) do
      activity(InvalidKeyActivity.run(input),
        idempotency_key: "valid",
        retry: [max_attempts: input.max_attempts]
      )
    end
  end

  setup do
    Repo.delete_all(ActivityTask)
    Repo.delete_all(Event)
    Repo.delete_all(Run)
    :ok
  end

  test "normalizes retry, timeout, and idempotency settings" do
    policy =
      Policy.normalize!(
        retry: [
          max_attempts: 3,
          backoff: :exponential,
          base_ms: 50,
          max_backoff_ms: 200,
          max_retry_horizon_ms: 10_000
        ],
        timeout: {:seconds, 1},
        idempotency_key: "order:1"
      )

    assert policy == %Policy{
             max_attempts: 3,
             backoff: :exponential,
             base_ms: 50,
             jitter_ms: 0,
             max_backoff_ms: 200,
             max_retry_horizon_ms: 10_000,
             timeout_ms: 1_000,
             idempotency_key: "order:1"
           }

    assert Policy.backoff_ms(Policy.retry_options(policy), 1) == 50
    assert Policy.backoff_ms(Policy.retry_options(policy), 2) == 100
    assert Policy.backoff_ms(Policy.retry_options(policy), 20) == 200
  end

  test "rejects invalid module policy at compile time" do
    invalid_policies = [
      "retry: [max_attempts: 0]",
      "retry: [backoff: :random]",
      "retry: [base_ms: -1]",
      "retry: [jitter_ms: -1]",
      "retry: [max_backoff_ms: -1]",
      "retry: [unknown: true]",
      "timeout: 0",
      "timeout: {:hours, 25}"
    ]

    Enum.each(invalid_policies, fn options ->
      module = "InvalidActivity#{System.unique_integer([:positive])}"

      assert_raise ArgumentError, ~r/invalid activity policy/, fn ->
        Code.compile_string("""
        defmodule #{module} do
          use Continuum.Activity, #{options}
          def run(value), do: value
        end
        """)
      end
    end)
  end

  test "rejects an invalid retry horizon" do
    assert_raise ArgumentError, ~r/worst-case retry horizon|worst-case execution time/, fn ->
      Policy.normalize!(
        retry: [
          max_attempts: 3,
          base_ms: 500,
          max_backoff_ms: 500,
          max_retry_horizon_ms: 3_100
        ],
        timeout: 1_000
      )
    end
  end

  test "invalid call-time policy and idempotency keys fail before durable scheduling" do
    assert_policy_failure_without_task(InvalidKeyFlow, :input, :idempotency_key)
    assert_policy_failure_without_task(OverrideFlow, %{max_attempts: 0}, :max_attempts)
  end

  property "valid bounded policies normalize to canonical retry options" do
    check all(
            max_attempts <- integer(1..10),
            timeout_ms <- integer(1..1_000),
            base_ms <- integer(0..100),
            max_backoff_ms <- integer(base_ms..1_000),
            backoff <- member_of([:constant, :exponential])
          ) do
      policy =
        Policy.normalize!(
          retry: [
            max_attempts: max_attempts,
            backoff: backoff,
            base_ms: base_ms,
            max_backoff_ms: max_backoff_ms,
            max_retry_horizon_ms: 1_000_000
          ],
          timeout: timeout_ms
        )

      assert Keyword.keys(Policy.retry_options(policy)) == [
               :max_attempts,
               :backoff,
               :base_ms,
               :jitter_ms,
               :max_backoff_ms,
               :max_retry_horizon_ms
             ]

      assert Policy.backoff_ms(Policy.retry_options(policy), max_attempts) <= max_backoff_ms
    end
  end

  test "retry jitter stays within the configured bounded window" do
    retry = [base_ms: 100, jitter_ms: 25, max_backoff_ms: 1_000]
    delays = Enum.map(1..100, fn _ -> Policy.backoff_ms(retry, 1) end)

    assert Enum.all?(delays, &(&1 in 100..125))
    assert Enum.uniq(delays) |> length() > 1
  end

  test "retry jitter survives at max backoff" do
    retry = [backoff: :exponential, base_ms: 100, jitter_ms: 50, max_backoff_ms: 1_000]

    # Attempt 10 is far past the ramp, so every delay sits at the cap.
    delays = Enum.map(1..200, fn _ -> Policy.backoff_ms(retry, 10) end)

    assert Enum.all?(delays, &(&1 in 950..1_000))
    assert length(Enum.uniq(delays)) > 1
    assert Enum.max(delays) <= 1_000
  end

  test "retry jitter never pushes a delay past max backoff" do
    retry = [base_ms: 5_000, jitter_ms: 250, max_backoff_ms: 1_000]
    delays = Enum.map(1..200, fn _ -> Policy.backoff_ms(retry, 1) end)

    assert Enum.all?(delays, &(&1 in 750..1_000))
    assert length(Enum.uniq(delays)) > 1
  end

  test "a jitter window wider than max backoff still respects the cap" do
    retry = [base_ms: 100, jitter_ms: 5_000, max_backoff_ms: 200]
    delays = Enum.map(1..200, fn _ -> Policy.backoff_ms(retry, 1) end)

    assert Enum.all?(delays, &(&1 in 0..200))
    assert length(Enum.uniq(delays)) > 1
  end

  defp assert_policy_failure_without_task(workflow, input, field) do
    {:ok, run_id} = Continuum.start(workflow, input, journal: Postgres)
    pump(run_id)

    assert {:error,
            %{
              state: :failed,
              error: %Continuum.RunFailure{kind: :error, reason: %ArgumentError{} = error}
            }} = Continuum.await(run_id, 1_000, journal: Postgres)

    assert Exception.message(error) =~ "invalid activity policy #{field}"
    assert Repo.aggregate(ActivityTask, :count) == 0
  end

  defp pump(run_id, attempts \\ 100)

  defp pump(run_id, attempts) when attempts > 0 do
    if Repo.one(from(r in Run, where: r.id == ^run_id, select: r.state)) == "failed" do
      :ok
    else
      assert {:ok, _count} =
               Continuum.Runtime.Dispatcher.dispatch_once(owner: "activity-policy")

      Process.sleep(5)
      pump(run_id, attempts - 1)
    end
  end

  defp pump(_run_id, 0), do: flunk("policy failure did not reach a terminal run")
end
