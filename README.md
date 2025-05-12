# IMFastTable

Module to manage an in-memory table with primary_key and secondary indexes.

# To create a table:

```elixir
table = IMFastTable.new(:lease_manager, [
  { :mac, :primary_key },     # MAC Address
  { :ip, :indexed },          # IP Address
  { :amac, :indexed },        # Remote Agente MAC Address
  { :rip, :indexed },         # Relay Agent IP Address
  { :exp, :indexed_non_uniq}, # Expire time in unix timestamp
  { :upd, :unindexed}         # Last update time in unix timestamp
])
```
or
```elixir
table = IMFastTable.new(:lease_manager, [{ :mac, :primary_key }, { :ip, :indexed }, { :amac, :indexed }, { :rip, :indexed }, { :exp, :indexed_non_uniq}, { :upd, :unindexed}])
```

# To destroy an existing table
```elixir
IMFastTable.destroy(:lease_manager)
```


# To test of stress
```elixir
IMFastTable.stress_load(table)
```

# How are datas stored?

```
## Calling new store the struct
{:fields, [mac: :primary_key, ip: :indexed, amac: :indexed, rip: :indexed, exp: :indexed_non_uniq, upd: unindexed]}

## When inserting...
```elixir
IMFastTable.insert(:lease_manager, [187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611])
```
```
# In table :lease_manager
{187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611}

# In table :lease_manager_ip_index
{ 3187736577, 187649973288960}
# In table :lease_manager_amac_index
{ 187649973288960, 187649973288960}
# In table :lease_manager_rip_index
{ 168430081, 187649973288960}
# In table :lease_manager_exp_index
{ 1746742811, 187649973288960}
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `i_m_fast_table` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:i_m_fast_table, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/i_m_fast_table>.

