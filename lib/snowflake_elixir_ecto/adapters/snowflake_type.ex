defmodule SnowflakeExEcto.Type do
  @moduledoc """
  Encodes a type to what Snowflake expects, or decodes a type from what Snowflake gives.
  """
  def encode(value, :boolean) do
    value
  end

  def encode(value, :time) do
    {:ok, v, _} = DateTime.from_iso8601("1970-01-01 #{value}Z")
    ms_since_epoch = DateTime.to_unix(v) * 1000
    ms_in_day = 86400 * 1000

    ms_since_midnight = rem(rem(ms_since_epoch, ms_in_day) + ms_in_day, ms_in_day)
    nanos_since_midnight = ms_since_midnight * 1000 * 1000

    {:ok, to_string(nanos_since_midnight)}
  end

  def encode(value, :integer) do
    {:ok, to_string(value)}
  end

  def encode(value, :naive_datetime) do
    foo = value
    |> NaiveDateTime.to_iso8601()

    {:ok, foo}
  end

  def encode(value, :uuid), do: Ecto.UUID.load(value)

  def encode(value, _type) do
    {:ok, to_string(value)}
  end

  def decode(value, :decimal) do
    {:ok, Decimal.new(value)}
  end

  def decode(value, :date) when is_binary(value) do
    unix_time = String.to_integer(value) * 86400

    case DateTime.from_unix(unix_time) do
      {:ok, time} -> {:ok, DateTime.to_date(time)}
      _ -> {:error, value}
    end
  end

  def decode(value, :naive_datetime) do
    String.replace(value, ".", "")
    |> String.slice(0..-4)
    |> String.to_integer()
    |> DateTime.from_unix(:microsecond)
  end

  def decode(value, _type) do
    {:ok, value}
  end
end
