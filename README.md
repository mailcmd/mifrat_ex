# Mifrat (acronym of Many Indexed Fields Random Access Table)

Module to manage an in-memory table with primary_key and secondary indexes. The objective is to have
a way to store complex temporary records with fast access through any indexed field.

## How its work:

See [HOW_ITS_WORKS.md](HOW_ITS_WORKS.md) document.

## Use

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `i_m_fast_table` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:mifrat_ex, "~> 0.3.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/mifrat_ex>.

