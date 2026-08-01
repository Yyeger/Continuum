defmodule Continuum.Activity.Policy do
  @moduledoc """
  Validated, normalized execution policy for an activity call.

  Retry limits include both execution timeouts and backoff delays, so a policy
  cannot silently schedule work beyond its configured retry horizon.
  """

  @default_timeout_ms 30_000
  @max_timeout_ms 86_400_000
  @default_max_backoff_ms 60_000
  @default_retry_horizon_ms 86_400_000
  @retry_keys [
    :max_attempts,
    :backoff,
    :base_ms,
    :jitter_ms,
    :max_backoff_ms,
    :max_retry_horizon_ms
  ]

  @enforce_keys [
    :max_attempts,
    :backoff,
    :base_ms,
    :jitter_ms,
    :max_backoff_ms,
    :max_retry_horizon_ms,
    :timeout_ms,
    :idempotency_key
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          max_attempts: pos_integer(),
          backoff: :constant | :exponential,
          base_ms: non_neg_integer(),
          jitter_ms: non_neg_integer(),
          max_backoff_ms: non_neg_integer(),
          max_retry_horizon_ms: pos_integer(),
          timeout_ms: pos_integer(),
          idempotency_key: binary() | nil
        }

  @doc "Returns the largest supported per-attempt timeout in milliseconds."
  @spec max_timeout_ms() :: pos_integer()
  def max_timeout_ms, do: @max_timeout_ms

  @doc "Normalizes and validates activity execution options."
  @spec normalize!(keyword()) :: t()
  def normalize!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      invalid!(:options, opts, "must be a keyword list")
    end

    retry = Keyword.get(opts, :retry, max_attempts: 1)
    timeout_ms = opts |> Keyword.get(:timeout, @default_timeout_ms) |> duration_ms!(:timeout)
    idempotency_key = Keyword.get(opts, :idempotency_key)

    validate_timeout!(timeout_ms)
    validate_idempotency_key!(idempotency_key)

    retry
    |> normalize_retry!()
    |> Map.merge(%{timeout_ms: timeout_ms, idempotency_key: idempotency_key})
    |> then(&struct!(__MODULE__, &1))
    |> validate_retry_horizon!()
  end

  def normalize!(opts), do: invalid!(:options, opts, "must be a keyword list")

  @doc "Returns the canonical retry keyword stored with durable activity work."
  @spec retry_options(t()) :: keyword()
  def retry_options(%__MODULE__{} = policy) do
    [
      max_attempts: policy.max_attempts,
      backoff: policy.backoff,
      base_ms: policy.base_ms,
      jitter_ms: policy.jitter_ms,
      max_backoff_ms: policy.max_backoff_ms,
      max_retry_horizon_ms: policy.max_retry_horizon_ms
    ]
  end

  @doc false
  @spec backoff_ms(keyword(), pos_integer()) :: non_neg_integer()
  def backoff_ms(retry, attempt) do
    base_ms = Keyword.get(retry || [], :base_ms, 1_000)
    max_backoff_ms = Keyword.get(retry || [], :max_backoff_ms, @default_max_backoff_ms)

    delay =
      case Keyword.get(retry || [], :backoff, :constant) do
        :exponential -> exponential_delay(base_ms, attempt, max_backoff_ms)
        _legacy_or_constant -> base_ms
      end

    jitter_ms = Keyword.get(retry || [], :jitter_ms, 0)
    jitter = if jitter_ms > 0, do: :rand.uniform(jitter_ms + 1) - 1, else: 0

    min(delay + jitter, max_backoff_ms)
  end

  defp normalize_retry!(retry) when is_list(retry) do
    unless Keyword.keyword?(retry) do
      invalid!(:retry, retry, "must be a keyword list")
    end

    unknown = Keyword.keys(retry) -- @retry_keys

    if unknown != [] do
      invalid!(:retry, retry, "contains unknown options: #{inspect(Enum.uniq(unknown))}")
    end

    max_attempts = Keyword.get(retry, :max_attempts, 1)
    backoff = Keyword.get(retry, :backoff, :constant)
    base_ms = Keyword.get(retry, :base_ms, 1_000)
    jitter_ms = Keyword.get(retry, :jitter_ms, 0)
    max_backoff_ms = Keyword.get(retry, :max_backoff_ms, @default_max_backoff_ms)

    max_retry_horizon_ms =
      Keyword.get(retry, :max_retry_horizon_ms, @default_retry_horizon_ms)

    positive_integer!(:max_attempts, max_attempts)

    unless backoff in [:constant, :exponential] do
      invalid!(:backoff, backoff, "must be :constant or :exponential")
    end

    non_negative_integer!(:base_ms, base_ms)
    non_negative_integer!(:jitter_ms, jitter_ms)
    non_negative_integer!(:max_backoff_ms, max_backoff_ms)
    positive_integer!(:max_retry_horizon_ms, max_retry_horizon_ms)

    if base_ms > max_backoff_ms do
      invalid!(:base_ms, base_ms, "must not exceed max_backoff_ms (#{max_backoff_ms})")
    end

    %{
      max_attempts: max_attempts,
      backoff: backoff,
      base_ms: base_ms,
      jitter_ms: jitter_ms,
      max_backoff_ms: max_backoff_ms,
      max_retry_horizon_ms: max_retry_horizon_ms
    }
  end

  defp normalize_retry!(retry), do: invalid!(:retry, retry, "must be a keyword list")

  defp validate_timeout!(timeout_ms) do
    positive_integer!(:timeout, timeout_ms)

    if timeout_ms > @max_timeout_ms do
      invalid!(:timeout, timeout_ms, "must not exceed #{@max_timeout_ms}ms")
    end
  end

  defp validate_idempotency_key!(key) when is_binary(key) or is_nil(key), do: :ok

  defp validate_idempotency_key!(key) do
    invalid!(:idempotency_key, key, "must be a binary or nil")
  end

  defp validate_retry_horizon!(%__MODULE__{} = policy) do
    execution_ms = policy.timeout_ms * policy.max_attempts

    if execution_ms > policy.max_retry_horizon_ms do
      invalid!(
        :max_retry_horizon_ms,
        policy.max_retry_horizon_ms,
        "is shorter than the #{execution_ms}ms worst-case execution time"
      )
    end

    total_ms = retry_horizon_ms(policy, execution_ms)

    if total_ms > policy.max_retry_horizon_ms do
      invalid!(
        :max_retry_horizon_ms,
        policy.max_retry_horizon_ms,
        "is shorter than the #{total_ms}ms worst-case retry horizon"
      )
    end

    policy
  end

  defp duration_ms!(value, _field) when is_integer(value), do: value
  defp duration_ms!({:seconds, n}, _field) when is_integer(n), do: n * 1_000
  defp duration_ms!({:minutes, n}, _field) when is_integer(n), do: n * 60_000
  defp duration_ms!({:hours, n}, _field) when is_integer(n), do: n * 3_600_000

  defp duration_ms!(value, field),
    do: invalid!(field, value, "must be milliseconds or a duration tuple")

  defp retry_horizon_ms(%__MODULE__{max_attempts: 1}, execution_ms), do: execution_ms

  defp retry_horizon_ms(%__MODULE__{} = policy, execution_ms) do
    Enum.reduce_while(1..(policy.max_attempts - 1), execution_ms, fn attempt, total ->
      next = total + backoff_ceiling_ms(policy, attempt)

      if next > policy.max_retry_horizon_ms,
        do: {:halt, next},
        else: {:cont, next}
    end)
  end

  defp backoff_ceiling_ms(policy, attempt) do
    retry = retry_options(policy)
    base = Keyword.put(retry, :jitter_ms, 0) |> backoff_ms(attempt)
    min(base + policy.jitter_ms, policy.max_backoff_ms)
  end

  defp exponential_delay(0, _attempt, _max_backoff_ms), do: 0

  defp exponential_delay(base_ms, attempt, max_backoff_ms) do
    exponent = max(attempt - 1, 0)

    Enum.reduce_while(1..max(exponent, 1), base_ms, fn _, delay ->
      cond do
        exponent == 0 -> {:halt, base_ms}
        delay >= max_backoff_ms -> {:halt, max_backoff_ms}
        delay > div(max_backoff_ms, 2) -> {:halt, max_backoff_ms}
        true -> {:cont, delay * 2}
      end
    end)
  end

  defp positive_integer!(_field, value) when is_integer(value) and value > 0, do: :ok
  defp positive_integer!(field, value), do: invalid!(field, value, "must be a positive integer")

  defp non_negative_integer!(_field, value) when is_integer(value) and value >= 0, do: :ok

  defp non_negative_integer!(field, value) do
    invalid!(field, value, "must be a non-negative integer")
  end

  defp invalid!(field, value, message) do
    raise ArgumentError,
          "invalid activity policy #{field}: #{inspect(value)} #{message}"
  end
end
