defmodule Ecto.Integration.SnowflakeTransactionTest do
  use ExUnit.Case, async: false

  alias Ecto.Integration.{PoolRepo, PoolRepo}
  alias Ecto.Integration.Post
  alias SnowflakeEx.SnowflakeConnectionServer

  require Logger
  @timeout 180_000

  def params do
    Application.get_all_env(:snowflake_elixir_ecto)
  end

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

  describe "aborts on corrupted transactions" do
    test "outside sandbox" do
      PoolRepo.transaction(fn ->
        bar = PoolRepo.query("INVALID")
        {:error, _} = bar
      end)

#      PoolRepo.transaction fn ->
#        # This will taint the whole inner transaction
#        {:error, _} = PoolRepo.query("INVALID")
#
#        assert_raise SnowflakeEx.Error, ~r/current transaction is aborted/, fn ->
#          PoolRepo.insert(%Post{}, skip_transaction: true)
#        end
#      end
    end
  end

  defp run_snowflake_sql(sql, snowflake_pid) do
    SnowflakeConnectionServer.query(snowflake_pid, sql)
  end
end
