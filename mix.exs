defmodule IMFastTable.MixProject do
  use Mix.Project

  def project do
    [
      app: :i_m_fast_table,
      version: "0.3.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      package: package(),
      deps: deps()
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
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:ex_doc, "~> 0.34", only: :dev, runtime: false, warn_if_outdated: true},
    ]
  end

  defp package() do
    [
      name: "i_m_fast_table",
      description: "Module to manage/access an in-memory table with primary_key and secondary indexes.",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/mailcmd/IMFastTable"},
      source_url: "https://github.com/mailcmd/IMFastTable",
    ]
  end
end
