defmodule Continuum.Test.Repo.Migrations.AddTimerOwnerSeq do
  use Ecto.Migration

  # The seq of the event that armed the timer — the `timer_started`, or the
  # `signal_awaited` for a signal timeout. Both writers know it at schedule
  # time. Without it, firing a timer had to load and decode the run's entire
  # event history to find the owning event, inside the `FOR UPDATE`
  # transaction: quadratic on exactly the workload timers exist for, a
  # `timer(days(30))` in a loop.
  #
  # Nullable on purpose. Timers armed before this migration keep the old
  # full-history lookup, so no backfill is required and no in-flight timer is
  # disturbed.
  def up do
    alter table(:continuum_timers) do
      add(:owner_seq, :bigint)
    end
  end

  def down do
    alter table(:continuum_timers) do
      remove(:owner_seq)
    end
  end
end
