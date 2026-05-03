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
  the durable task payload. v0.1 preserves this metadata for future
  exactly-once-ish execution, but it does not yet deduplicate duplicate
  activity execution with a side table.
  """

  @type retry_policy :: keyword()
  @type duration :: {:seconds | :minutes | :hours, pos_integer()}

  @callback run(any()) :: {:ok, term()} | {:error, term()}
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
