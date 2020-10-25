# SnowflakeExEcto

Integrates Snowflake (SnowflakeElixir) with [ecto_sql](), which allows you to use your schemas with Snowflake!

Right now this is a major WIP, and is not ready for production use.

## Installation

The package can be installed by adding `snowflake_elixir_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflake_elixir_ecto, "~> 0.1.0"}
  ]
end
```

## Configuration

In your config/config.exs file, add the following:
```
config :my_app, MyApp.Repo,
  username: "yourusername",
  password: "supersecret",
  host: "https://xxx.your-region.snowflakecomputing.com",
  account_name: "lol1234",
  database: "SNOWFLAKE_TEST_DRIVER",
  schema: "PUBLIC",
  role: "ENGINEER",
  pool_size: 1,
  warehouse: "COMPUTE_WH"
```

Then in your `repo.ex`:
```elixir
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Snowflake
end
```

## Connection options
```markdown
* `:host` - Server hostname, including https. Example: "https://xxx.us-east-1.snowflakecomputing.com"
* `:username` - Username for your account.
* `:password` - Password for your account.
* `:warehouse` - Warehouse to use on Snowflake. If none set, will use default for the account.
* `:account_name` - Account name. This is usually the name between the https:// and us-east-1 (or whatever region).
* `:database` - the database to connect to.
* `:schema` - the schema to connect to.
* `:async` - If set to true, will issue a query then connect every `:async_interval` to see if the query has completed.
```

# Data Mapping
WIP, TODO

## Snowflake Differences compared to a "traditional" RDBMS
Snowflake isn't like Postgres/MySQL or another database you might be use to. There are a few gotchas you need to watch out for.

Before using this library, it's recommended that you learn how Snowflake works at a basic level.

### Indexing
- Snowflake does not support indexes, at all.
- Snowflake does not support unique indexes. You can create them, but it's not enforced. [See contraints](https://docs.snowflake.com/en/sql-reference/constraints-overview.html#supported-constraint-types)

### Inserts
- Snowflake does not support RETURNING. Thoughts around how to handle this are [here](#todo)
- Snowflake does not have UPSERT support.

### Data binding
- Snowflake must insert integers as strings in the JSON for binding, so it's encoded as "0", otherwise you'll get `compilation error:\nUnsupported data type 'java.lang.Integer'`
