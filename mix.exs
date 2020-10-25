defmodule SnowflakeExEcto.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflake_elixir_ecto,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      source_url: "https://github.com/joshuataylor/snowflake_elixir",
      description: "Snowflake driver written in pure Elixir, using db_connection",
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:snowflake_elixir, "~> 0.1.0"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false}
    ]
  end
end
