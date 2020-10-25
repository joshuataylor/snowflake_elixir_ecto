defmodule Ecto.Integration.StorageTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Ecto.Adapters.Snowflake
  alias Ecto.Integration.PoolRepo

  def params do
    Application.get_env(:snowflake_elixir_ecto, PoolRepo)
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
      queue_interval: 10000
    )

    {:ok, _} = Ecto.Adapters.Snowflake.ensure_all_started(PoolRepo.config(), :temporary)

    {:ok, _} = PoolRepo.start_link()

    :ok
  end

  def wrong_params do
    Keyword.merge(params(),
      username: "randomuser",
      password: "password1234"
    )
  end

  def drop_database() do
    run_snowflake_sql("DROP DATABASE #{params()[:database]};")
  end

  def create_database() do
    run_snowflake_sql("CREATE DATABASE #{params()[:database]};")
  end

  def create_posts() do
    run_snowflake_sql("CREATE TABLE posts (title varchar(20));")
  end

  def run_snowflake_sql(sql) do
    PoolRepo.query(sql)
  end

  test "storage up (twice in a row)" do
    assert Snowflake.storage_up(params()) == :ok
    assert Snowflake.storage_up(params()) == {:error, :already_up}
  after
    drop_database()
  end

  test "storage down (twice in a row)" do
    create_database()
    assert Snowflake.storage_down(params()) == :ok
    assert Snowflake.storage_down(params()) == {:error, :already_down}
  end

  test "storage up and down (wrong credentials)" do
    refute Snowflake.storage_up(wrong_params()) == :ok
    create_database()
    refute Snowflake.storage_down(wrong_params()) == :ok
  after
    drop_database()
  end

  defmodule Migration do
    use Ecto.Migration
    def change, do: :ok
  end

  test "storage status is up when database is created" do
    create_database()
    assert :up == Snowflake.storage_status(params())
  after
    drop_database()
  end

  test "storage status is down when database is not created" do
    create_database()
    drop_database()
    assert :down == Snowflake.storage_status(params())
  end

  test "storage status is an error when wrong credentials are passed" do
    assert Snowflake.storage_status(wrong_params()) ==
             {:error,
              {:error,
               %RuntimeError{message: "\"Incorrect username or password was specified.\""}}}
  end
end
