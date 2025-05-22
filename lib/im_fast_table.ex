defmodule IMFastTable do
  @moduledoc """

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

  **NOTE**: Order is important!!

  # To destroy an existing table
  ```elixir
  IMFastTable.destroy(:lease_manager)
  ```

  # To test of stress
  ```elixir
  IMFastTable.stress_load(table)
  ```

  # How are datas stored?

  ## Calling new() stores this struct first
  ```
  {:fields, [mac: :primary_key, ip: :indexed, amac: :indexed, rip: :indexed, exp: :indexed_non_uniq, upd: unindexed]}
  ```

  ## When inserting...
  ```elixir
  IMFastTable.insert(:lease_manager, [187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611])
  ```

  ### Store in table :lease_manager
  ```
  {187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611}
  ```
  ### Store in table :lease_manager_ip_index
  ```
  { 3187736577, 187649973288960}
  ```
  ### Store in table :lease_manager_amac_index
  ```
  { 187649973288960, 187649973288960}
  ```
  ### Store in table :lease_manager_rip_index
  ```
  { 168430081, 187649973288960}
  ```
  ### Store n table :lease_manager_exp_index
  ```
  { 1746742811, 187649973288960}
  ```

  """

  @spec new(atom(), keyword(), keyword()) :: atom() | :ets.tid()
  def new(table_name, fields, options \\ []) do
    autosave = Keyword.get(options, :autosave, false)
    path = Keyword.get(options, :path, nil)
    period = Keyword.get(options, :period, 300_000)

    cond do
      autosave and not is_binary(path) ->
        raise(ArgumentError, message: "Bad options: 'autosave = true' make mandatory 'path' parameter")

      autosave ->
        table = case load(path) do
          {:ok, table} ->
            table
          {:error, _} ->
            table = :ets.new(table_name, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
            :ets.insert(table, {:fields, fields})
            new_indexes(table_name, fields)
            table
        end
        {:ok, ref} = :timer.apply_interval(period, fn ->
          [{_, _, path}] = :ets.lookup(table, :autosave)
          IMFastTable.store(table_name, path)
        end)
        :ets.insert(table, {:autosave, ref, String.to_charlist(path)})
        table

      true ->
        table = :ets.new(table_name, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
        :ets.insert(table, {:fields, fields})
        new_indexes(table_name, fields)
        table
    end
  end
  defp new_indexes(_, []), do: :ok
  defp new_indexes(table_name, [field | fields]) when not is_tuple(field),
    do: new_indexes(table_name, fields)
  defp new_indexes(table_name, [{_, :unindexed} | fields]),
    do: new_indexes(table_name, fields)
  defp new_indexes(table_name, [{_, :primary_key} | fields]),
    do: new_indexes(table_name, fields)
  defp new_indexes(table_name, [{field_name, :indexed_non_uniq} | fields]) do
    index_name = get_table_index_name(table_name, field_name)
    :ets.new(index_name, [:bag, :public, :named_table, read_concurrency: true, write_concurrency: true])
    new_indexes(table_name, fields)
  end
  defp new_indexes(table_name, [{field_name, :indexed} | fields]) do
    index_name = get_table_index_name(table_name, field_name)
    :ets.new(index_name, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    new_indexes(table_name, fields)
  end

  @spec destroy(atom() | :ets.tid()) :: true
  def destroy(table) do
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    case :ets.lookup(table, :autosave) do
      [] -> :ok
      [{:autosave, ref, path}] ->
        :timer.cancel(ref)
        store(table, path)
    end
    destroy_indexes(table, fields)
    :ets.delete(table)
  end
  defp destroy_indexes(_, []), do: :ok
  defp destroy_indexes(table, [field | fields]) when not is_tuple(field),
    do: destroy_indexes(table, fields)
  defp destroy_indexes(table, [{_, index_type} | fields]) when index_type in [:unindexed, :primary_key],
    do: destroy_indexes(table, fields)
  defp destroy_indexes(table, [{field_name, index_type} | fields]) when index_type in [:indexed, :indexed_non_uniq] do
    index_name = get_table_index_name(table, field_name)
    :ets.delete(index_name)
    destroy_indexes(table, fields)
  end


  # IMFastTable.insert(table, [187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611])
  # IMFastTable.insert(table, {187649973288960, 3187736577, 187649973288960, 168430081, 1746742811, 1746735611})

  @spec insert(atom() | :ets.tid(), [...] | tuple()) :: :duplicate_record | [:skip | true | false]
  def insert(table, record) when is_tuple(record), do: insert(table, Tuple.to_list(record))
  def insert(table, record) do
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    primary_key_idx = find_primary_key_idx(fields)
    primary_key = :lists.nth(primary_key_idx, record)
    current_record = :ets.lookup(table, primary_key)

    tuple_record = List.to_tuple(record)

    case current_record do
      [^tuple_record] ->
        :duplicate_record

      [] ->
        :ets.insert(table, List.to_tuple(record))
        insert_indexes(
          table,
          :lists.enumerate(record),
          fields,
          primary_key
        )

      [cur_rec] ->
        :ets.insert(table, List.to_tuple(record))
        delete_indexes(
          table,
          cur_rec |> Tuple.to_list() |> :lists.enumerate(),
          fields,
          primary_key
        ) ++ insert_indexes(
          table,
          :lists.enumerate(record),
          fields,
          primary_key
        )
    end
  end
  defp insert_indexes(_, [], _, _), do: []
  defp insert_indexes(table, [ {idx, data} | datas ], fields, primary_key) do
    result = case :lists.nth(idx, fields) do
      {_, index_type} when index_type in [:primary_key, :unindexed] ->
        :skip

      {field_name, index_type} when index_type in [:indexed, :indexed_non_uniq] ->
        index_table = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
        :ets.insert(index_table, {data, primary_key})
    end
    [result | insert_indexes(table, datas, fields, primary_key) ]
  end

  @spec delete(atom() | :ets.tid(), any()) :: :not_found | [:skip | true | false ]
  def delete(table, primary_key) do
    record = :ets.lookup(table, primary_key)
    if record == [] do
      :not_found
    else
      record = record
        |> hd()
        |> Tuple.to_list()
      fields = Keyword.get(:ets.lookup(table, :fields), :fields)
      delete_indexes(
        table,
        :lists.enumerate(record),
        fields,
        primary_key
      ) ++ [:ets.delete(table, primary_key)]
    end
  end
  defp delete_indexes(_, [], _, _), do: []
  defp delete_indexes(table, [ {idx, data} | datas ], fields, primary_key) do
    result = case :lists.nth(idx, fields) do
      {_, index_type} when index_type in [:primary_key, :unindexed] ->
        :skip

      {field_name, :indexed} ->
        index_table = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
        :ets.delete(index_table, data)

      {field_name, :indexed_non_uniq} ->
        index_table = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
        :ets.delete_object(index_table, {data, primary_key})
    end

    [result | delete_indexes(table, datas, fields, primary_key) ]
  end

  @spec delete(atom() | :ets.tid(), any(), any()) :: :ok
  def delete(table, field_name, key) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    :ets.lookup(table_index, key)
      |> Enum.each(fn {_, primary_key} ->
           delete(table, primary_key)
         end)
  end

  @spec delete_many(any(), list()) :: :ok
  def delete_many(_table, []), do: :ok
  def delete_many(table, [primary_key | primary_keys]) do
    delete(table, primary_key)
    delete_many(table, primary_keys)
  end

  @spec delete_range(atom() | :ets.tid(), any(), any(), any()) :: list()
  def delete_range(table, field_name, from, to) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [true]}]
    :ets.select_delete(table_index, filter)
  end

  @spec count_range(atom() | :ets.tid(), any(), any(), any()) :: list()
  def count_range(table, field_name, from, to) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [true]}]
    :ets.select_count(table_index, filter)
  end


  @spec get(atom() | :ets.tid(), any()) :: :not_found | tuple()
  def get(table, primary_key) do
    case :ets.lookup(table, primary_key) do
      [] -> :not_found
      [record] -> record
    end
  end

  @spec get(atom() | :ets.tid(), any(), any()) :: list()
  def get(table, field_name, key) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    :ets.lookup(table_index, key)
      |> Enum.map(fn {_, primary_key} ->
           get(table, primary_key)
         end)
  end

  @spec get_map(atom() | :ets.tid(), any()) :: :not_found | map()
  def get_map(table, primary_key) do
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    case get(table, primary_key) do
      :not_found ->
        :not_found

      tuple ->
        tuple
          |> Tuple.to_list()
          |> :lists.enumerate()
          |> Enum.map(fn {idx, value} ->
                {key, _} = :lists.nth(idx, fields)
                {key, value}
              end)
          |> Enum.into(%{})
    end

  end

  @spec get_map(atom() | :ets.tid(), any(), any()) :: [map()]
  def get_map(table, field_name, key) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    :ets.lookup(table_index, key)
      |> Enum.map(fn {_, primary_key} ->
           get_map(table, primary_key)
         end)
  end

  @spec get_range(atom() | :ets.tid(), any(), any(), any(), list()) :: list()
  def get_range(table, field_name, from, to, opts \\ [return: :keys])
  def get_range(table, field_name, from, to, return: :records) do
    get_range(table, field_name, from, to, return: :keys)
      |> Enum.map(fn {_, primary_key} -> get(table, primary_key) end)

  end
  def get_range(table, field_name, from, to, return: :keys) do
    table_index = get_table_index_name(Keyword.get(:ets.info(table), :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [:"$_"]}]
    :ets.select(table_index, filter)
  end

  def store(table, path) do
    :ets.tab2file(table, path)
  end
  def load(path) do
    :ets.file2tab(path)
  end

  ################################################################################
  # Small helpers
  ################################################################################
  defp find_primary_key_idx(keyword_list) do
    Enum.find_index(keyword_list, fn {_,t} -> t == :primary_key end) + 1
  end

  defp get_table_index_name(table_name, field_name) do
    table_name
      |> to_string()
      |> Kernel.<>("_#{field_name}_index")
      |> String.to_atom()
  end

end
