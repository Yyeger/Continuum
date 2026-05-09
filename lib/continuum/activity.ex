defmodule Continuum.Activity do
  @moduledoc """
  Defines an activity — the only place where side effects are allowed.

      defmodule Payments.Charge do
        use Continuum.Activity,
          retry: [max_attempts: 5, backoff: :exponential, base_ms: 500],
          timeout: {:seconds, 30}

        @impl true
        def run(order_id, amount) do
          Stripe.charge(order_id, amount)
        end
      end

  Activities are not subject to the determinism scanner — they can do
  arbitrary I/O, talk to NIFs, raise, etc. Their return value is journaled
  on first success and replayed on workflow resume.

  Implementing the optional `c:idempotency_key/1` callback stores the key in
  the durable task payload. Once an activity result has been committed,
  another task for the same activity module and key reuses that committed
  result without running the activity body again. This suppresses duplicate
  execution after Continuum has recorded success; external systems should
  still receive the same key because a worker can crash after the side effect
  succeeds but before Continuum commits the result.
  """

  @type retry_policy :: keyword()
  @type duration :: {:seconds | :minutes | :hours, pos_integer()}

  @callback run(any()) :: {:ok, term()} | {:error, term()}
  @doc """
  Returns a stable idempotency key for a scheduled activity call, or `nil`.

  Keys are scoped by activity module, not by run. Any run invoking the same
  activity module with the same key receives the same committed result.
  """
  @callback idempotency_key(any()) :: binary() | nil

  @optional_callbacks [run: 1, idempotency_key: 1]

  defmacro __using__(opts) do
    retry = Keyword.get(opts, :retry, max_attempts: 1)
    timeout = Keyword.get(opts, :timeout, {:seconds, 30})

    quote do
      @behaviour Continuum.Activity

      @continuum_activity_retry unquote(retry)
      @continuum_activity_timeout unquote(timeout)

      def __continuum_activity__ do
        %{
          module: __MODULE__,
          retry: @continuum_activity_retry,
          timeout: @continuum_activity_timeout
        }
      end
    end
  end
end
