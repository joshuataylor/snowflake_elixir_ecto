defmodule Ecto.Integration.SnowflakeMigrationsTest do
  use ExUnit.Case, async: false

  alias Ecto.Integration.PoolRepo

  @moduletag :capture_log
  @base_migration 3_000_000

  setup do
    Application.put_env(:snowflake_elixir_ecto, PoolRepo,
      username: System.get_env("SNOWFLAKE_USERNAME"),
      password: System.get_env("SNOWFLAKE_PASSWORD"),
      host: System.get_env("SNOWFLAKE_HOST"),
      account_name: System.get_env("SNOWFLAKE_ACCOUNT_NAME"),
      database: "test_#{Ecto.UUID.generate|>String.replace("-", "")}",
      schema: "test_#{Ecto.UUID.generate|>String.replace("-", "")}",
      role: System.get_env("SNOWFLAKE_ROLE"),
      pool_size: 5,
      timeout: 100_000_000,
      warehouse: System.get_env("SNOWFLAKE_WAREHOUSE"),
      show_sensitive_data_on_connection_error: true,
      queue_target: 1000,
      queue_interval: 10000,
    )

    {:ok, _} = Ecto.Adapters.Snowflake.ensure_all_started(PoolRepo.config(), :temporary)

    _ = Ecto.Adapters.Snowflake.storage_down(PoolRepo.config())
    :ok = Ecto.Adapters.Snowflake.storage_up(PoolRepo.config())

    {:ok, _pid} = PoolRepo.start_link()

    :ok = Ecto.Migrator.up(PoolRepo, 0, Ecto.Integration.QuickMigration)

    :ok
  end

  defmodule DuplicateTableMigration do
    use Ecto.Migration

    def change do
      create_if_not_exists table(:duplicate_table)
      create_if_not_exists table(:duplicate_table)
    end
  end

  defmodule DuplicateTableCreateMigration do
    use Ecto.Migration

    def change do
      create table(:duplicate_table)
      create table(:duplicate_table)
    end
  end

  test "Double create if not exists works" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        num = @base_migration + System.unique_integer([:positive])
        Ecto.Migrator.up(PoolRepo, num, DuplicateTableMigration, log: false)
      end)

    assert log =~ ~s(DUPLICATE_TABLE already exists, statement succeeded)
  end

  test "Double create if exists works" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        num = @base_migration + System.unique_integer([:positive])
        Ecto.Migrator.up(PoolRepo, num, DuplicateTableMigration, log: false)
      end)

    assert log =~ ~s(DUPLICATE_TABLE already exists, statement succeeded)
  end
end
