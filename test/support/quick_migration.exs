defmodule Ecto.Integration.QuickMigration do
  use Ecto.Migration

  def change do
    # IO.puts "TESTING MIGRATION LOCK"
    # Process.sleep(10000)

    create table(:posts) do
      add(:title, :string, size: 100)
      add(:counter, :integer)
      add(:blob, :binary)
      add(:bid, :binary_id)
      add(:uuid, :uuid)
      add(:meta, :map)
      add(:links, {:map, :string})
      add(:intensities, {:map, :float})
      add(:public, :boolean)
      add(:cost, :decimal, precision: 2, scale: 1)
      add(:visits, :integer)
      add(:wrapped_visits, :integer)
      add(:intensity, :float)
      add(:author_id, :integer)
      add(:posted, :date)
      timestamps(null: true)
    end
  end
end
