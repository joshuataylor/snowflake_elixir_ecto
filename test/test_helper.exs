Code.require_file("./support/repo.exs", __DIR__)

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :snowflake_elixir_ecto, adapter: Ecto.Adapters.Snowflake

  def create_prefix(prefix) do
    "create schema #{prefix}"
  end

  def drop_prefix(prefix) do
    "drop schema #{prefix}"
  end

  def uuid do
    Ecto.UUID
  end
end

Code.require_file "./support/quick_migration.exs", __DIR__

ExUnit.start()