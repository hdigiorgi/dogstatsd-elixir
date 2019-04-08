defmodule DogStatsd.Mixfile do
  use Mix.Project

  def project do
    [
      app: :dogstatsd,
      version: "0.0.1",
      elixir: "~> 1.0",
      deps: deps(),
      description: "A client for DogStatsd, an extension of the StatsD metric server for Datadog."
     ]
  end

  def application do
    [applications: []]
  end

  defp deps do
    []
  end

end
