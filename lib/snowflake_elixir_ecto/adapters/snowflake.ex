defmodule Ecto.Adapters.Snowflake do
  @moduledoc """
  Adapter module for Snowflake.

  It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON.
  There isn't an Elixir Arrow library (yet!), so it seems that setting an earlier Java version seems
  to give us back JSON results.

  One of the major notes is you will need to enable Snowflakes `QUOTED_IDENTIFIERS_IGNORE_CASE` setting, which you can
  find here: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#third-party-tools-and-case-insensitive-identifier-resolution

  Note that this can be done on an account or if needed on a session level which you can set below.

  ## Features
    * Nothing yet :-(

  ## Roadmap
    * Full query support (including joins, preloads and associations)
    * Support for transactions
    * Support for data migrations
    * Support for ecto.create and ecto.drop operations
    * Support for transactional tests via `Ecto.Adapters.SQL`

  ## Thanks
  I just want to thank the ecto_sql library for being amazing, and being able to copy most of the decoding code from that.

  ## Options

  Snowflake is a little bit different than most other adapters (Postgres, MySQL, etc) as it communicates over
  HTTP and not a binary protocol. There is support for both waiting for a query (synchronous) and async queries.

  To add Snowflake to your app, you need to do the folowing:

      config :your_app, YourApp.Repo,
        ...

  ### Connection options

    * `:host` - Server hostname, including https. Example: "https://xxx.us-east-1.snowflakecomputing.com"
    * `:username` - Username for your account.
    * `:password` - Password for your account.
    * `:warehouse` - Warehouse to use on Snowflake. If none set, will use default for the account.
    * `:account_name` - Account name. This is usually the name between the https:// and us-east-1 (or whatever region).
    * `:database` - the database to connect to.
    * `:schema` - the schema to connect to.
    * `:async` - If set to true, will issue a query then connect every `:async_interval` to see if the query has completed.
    * `:async_query_interval` - How often to check if the query has completed.
    * `:maintenance_database` - Specifies the name of the database to connect to when
      creating or dropping the database. Defaults to `"info"`
    * `:pool` - The connection pool module, defaults to `DBConnection.ConnectionPool`
    * `:connect_timeout` - The timeout for establishing new connections (default: 30000)
    * `:prepare` - How to prepare queries, either `:named` to use named queries
      or `:unnamed` to force unnamed queries (default: `:named`)
    * `:socket_options` - Specifies socket configuration
    * `:show_sensitive_data_on_connection_error` - show connection data and
      configuration whenever there is an error attempting to connect to the
      database

  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :snowflake_elixir,
    migration_lock: false

  @behaviour Ecto.Adapter.Storage

  import SnowflakeExEcto.Type, only: [encode: 2, decode: 2]

  # Support arrays in place of IN
  @impl true
  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers({:in, sub}, {:in, sub}), do: [{:array, sub}]
  def dumpers(:binary_id, type), do: [type, Snowflake.Ecto.UUID]
  def dumpers(ecto_type, type), do: [type, &encode(&1, ecto_type)]

  @impl true
  def loaders(:binary_id, type), do: [Snowflake.Ecto.UUID, type]
  def loaders(ecto_type, type), do: [&decode(&1, ecto_type), type]

  @impl true
  def execute(adapter_meta, query_meta, query, params, _opts) do
    lots =
      params
      |> Enum.with_index()
      |> Enum.map(fn {value, index} ->
        {
          "#{index + 1}",
          %{
            type: convert_select_type(value),
            value: Jason.encode!(value)
          }
        }
      end)
      |> Map.new()

    Ecto.Adapters.SQL.execute(adapter_meta, query_meta, query, params, field_types: lots)
  end

  @impl true
  def insert(
        adapter_meta,
        %{source: source, prefix: prefix, schema: schema},
        params,
        {kind, conflict_params, _} = on_conflict,
        returning,
        _opts
      ) do
    field_types =
      params
      |> Keyword.keys()
      |> Enum.with_index()
      |> Enum.map(fn {key, index} ->
        {
          "#{index + 1}",
          %{
            type: convert_type(schema.__schema__(:type, key)),
            value: Jason.encode!(Keyword.get(params, key))
          }
        }
      end)
      |> Map.new()

    {fields, values} = :lists.unzip(params)

    sql = @conn.insert(prefix, source, fields, [fields], on_conflict, returning)

    Ecto.Adapters.SQL.struct(
      adapter_meta,
      @conn,
      sql,
      :insert,
      source,
      [],
      values ++ conflict_params,
      kind,
      returning,
      field_types: field_types
    )
  end

  @impl true
  def supports_ddl_transaction? do
    true
  end

  @creates [:create, :create_if_not_exists]
  alias Ecto.Migration.Table

  @impl true
  def execute_ddl(adapter, {command, %Table{} = table, columns}, options) when command in @creates do
    db = Keyword.get(adapter.repo.config, :database)
    schema = Keyword.get(adapter.repo.config, :schema)

    table_name = "#{db}.#{schema}.#{table.name}"

    query = [
      "CREATE TABLE ",
      Ecto.Adapters.Snowflake.Connection.if_do(command == :create_if_not_exists, "IF NOT EXISTS "),
      table_name,
      ?\s,
      ?(,
      Ecto.Adapters.Snowflake.Connection.column_definitions(table, columns),
      Ecto.Adapters.Snowflake.Connection.pk_definition(columns, ", "),
      ?),
      Ecto.Adapters.Snowflake.Connection.options_expr(table.options)
    ]

    result = Ecto.Adapters.SQL.query!(adapter.repo, query, [], options)

    logs = result |> ddl_logs()

    {:ok, logs}
  end

  def ddl_logs(%SnowflakeEx.Result{} = result) do
    %{messages: messages} = result

    for message <- messages do
      %{message: message, severity: severity} = message

      {severity, message, []}
    end
  end


  @impl true
  def storage_up(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    schema =
      Keyword.fetch!(opts, :schema)

    command = ~s(CREATE DATABASE "#{database}")

    s = run_query(command, opts)

    case s do
      {:ok, _} ->
        run_query("USE DATABASE #{database}", opts)
        run_query("CREATE SCHEMA #{schema}", opts)
        run_query("USE SCHEMA #{schema}", opts)

        :ok

      {:error, %{snowflake: %{code: :duplicate_database}}} ->
        {:error, :already_up}

      {:error, %SnowflakeEx.Result{messages: messages}} ->
        error = hd(messages).message

        cond do
          is_binary(error) and String.contains?(error, "does not exist or not authorized.") ->
            {:error, :already_up}

          is_binary(error) and String.contains?(error, "already exists.") ->
            {:error, :already_up}

          %RuntimeError{} -> {:error, error}

          true ->
            {:error, Exception.message(error)}
        end
        {:error, %RuntimeError{} = error} -> {:error, Exception.message(error)}
    end
  end

  @impl true
  def storage_down(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    command = "DROP DATABASE \"#{database}\""

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{snowflake: %{code: :invalid_catalog_name}}} ->
        {:error, :already_down}
      {:error, %RuntimeError{} = error} -> {:error, Exception.message(error)}

      {:error, %SnowflakeEx.Result{messages: messages}} ->
        error = hd(messages).message
        cond do
          is_binary(error) and String.contains?(error, "does not exist or not authorized.") ->
            {:error, :already_down}

          is_binary(error) and String.contains?(error, "already exists.") ->
            {:error, :already_down}

          true ->
            {:error, Exception.message(error)}
        end
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    check_database_query = "show databases like '#{database}'"

    case run_query(check_database_query, opts) do
      {:ok, %{num_rows: 0}} -> :down
      {:ok, %{num_rows: _num_rows}} -> :up
      other -> {:error, other}
    end
  end

  def lock_for_migrations(_meta, _opts, fun) do
    fun.()
  end

  defp run_query(sql, opts) do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:snowflake_elixir)

    opts =
      opts
      |> Keyword.drop([:name, :log, :pool, :pool_size])
      |> Keyword.put(:backoff_type, :stop)
      |> Keyword.put(:max_restarts, 0)

    task =
      Task.Supervisor.async_nolink(Ecto.Adapters.SQL.StorageSupervisor, fn ->
        {:ok, conn} = SnowflakeEx.SnowflakeConnectionServer.start_link(opts)

        value = SnowflakeEx.SnowflakeConnectionServer.query(conn, sql, [], opts)
        GenServer.stop(conn)
        value
      end)

    timeout = Keyword.get(opts, :timeout, 180_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:ok, result}} ->
        {:ok, result}

      {:ok, {:error, error}} ->
        {:error, error}

      {:exit, {%{__struct__: struct} = error, _}}
      when struct in [SnowflakeEx.Error, DBConnection.Error] ->
        {:error, error}

      {:exit, reason} ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end

  defp convert_type(:integer), do: "FIXED"
  defp convert_type(:string), do: "TEXT"
  defp convert_type(:boolean), do: "BOOLEAN"
  # @todo fix this to be proper date
  defp convert_type(:date), do: "DATE"
  defp convert_type(:time), do: "TIME"
  defp convert_type(i), do: "TEXT"

  defp convert_select_type(i) when is_integer(i), do: "FIXED"
  defp convert_select_type(i) when is_boolean(i), do: "BOOLEAN"
  defp convert_select_type(i) when is_bitstring(i), do: "TEXT"
  defp convert_select_type(i) when is_list(i), do: "ARRAY"
  defp convert_select_type(_), do: "TEXT"
end
