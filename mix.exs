defmodule SnowflakeExEcto.MixProject do
  use Mix.Project

  @source_url "https://github.com/joshuataylor/snowflake_elixir_ecto"

  def project do
    [
      app: :snowflake_elixir_ecto,
      version: "0.2.1",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      description: "Snowflake driver written in pure Elixir, using db_connection",
      deps: deps(),
      package: package(),
      docs: docs()
    ]
  end

  defp package do
    [
      maintainers: ["Josh Taylor"],
      licenses: ["MIT"],
      links: %{
        GitHub: @source_url
      }
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:snowflake_elixir, "~> 0.2.0"},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: [
        "CHANGELOG.md",
        "README.md"
      ]
    ]
  end
end
