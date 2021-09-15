defmodule Snowflake.Ecto.UUID do
  use Ecto.Type

  @impl true
  def dump(foo) do
    {:ok, foo}
  end

  def load(uuid) do
    {:ok, uuid}
  end
end