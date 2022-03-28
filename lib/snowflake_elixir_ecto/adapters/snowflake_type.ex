defmodule SnowflakeExEcto.Type do
  @moduledoc """
  Encodes a type to what Snowflake expects, or decodes a type from what Snowflake gives.
  """
  def encode(value, :time) do
    {:ok, v, _} = DateTime.from_iso8601("1970-01-01 #{value}Z")
    ms_since_epoch = DateTime.to_unix(v) * 1000
    ms_in_day = 86400 * 1000

    ms_since_midnight = rem(rem(ms_since_epoch, ms_in_day) + ms_in_day, ms_in_day)
    nanos_since_midnight = ms_since_midnight * 1000 * 1000

    {:ok, to_string(nanos_since_midnight)}
  end

  def encode(true, :boolean), do: "1"
  def encode(false, :boolean), do: "0"
  def encode(value, :integer), do: {:ok, value}
  def encode(value, :naive_datetime), do: {:ok, NaiveDateTime.to_iso8601(value)}
  def encode(value, {:array, :string}), do: {:ok, value}
  def encode(value, :uuid), do: value
  def encode(value, type), do: {:ok, to_string(value)}

  def decode("1", :boolean), do: {:ok, true}
  def decode("0", :boolean), do: {:ok, false}

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

  def decode(value, {:maybe, :utc_datetime}), do: decode(value, :utc_datetime)
  def decode(value, :integer) when is_bitstring(value), do: {:ok, String.to_integer(value)}

  def decode(value, :utc_datetime) when is_bitstring(value) do
    # we can get 1440 from here, not sure why?
    value
    |> String.split(" ")
    |> hd
    |> String.replace(".", "")
    |> String.slice(0..-4)
    |> String.to_integer()
    |> DateTime.from_unix(:microsecond)
  end

  def decode(value, _type), do: {:ok, value}
end
