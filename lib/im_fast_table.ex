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
  # require IMFastTable
  ################################################################################
  # Macros
  ################################################################################

  defmacro filter_string(pattern, guard \\ "true", return \\ "full_record") do
    quote do
      f =
        Code.eval_string("""
        fn #{unquote(pattern)} #{unquote(return) == "full_record" && " = full_record" || ""} when #{unquote(guard)} ->
          #{unquote(return)}
        end
        """) |> elem(0)
      :ets.fun2ms(f)
    end
  end

  defmacro filter(pattern, guard \\ true, return \\ quote do: full_record) do
    quote do
      f =
        fn unquote(pattern) = full_record when unquote(guard) ->
          unquote(return)
        end
      :ets.fun2ms(f)
    end
  end

  ################################################################################
  # API
  ################################################################################


  ################################################################################
  # destroy
  ################################################################################
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

  ################################################################################
  # new
  ################################################################################
  @spec new(atom(), keyword(), keyword()) :: atom() | :ets.tid()
  def new(table_name, fields, options \\ []) do
    autosave = Keyword.get(options, :autosave, false)
    path = Keyword.get(options, :path, nil)
    period = Keyword.get(options, :period, 300_000)

    fields = fields ++ [{:sys_flag, :indexed_non_uniq}]

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
          store(table_name, path)
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

  ################################################################################
  # insert
  ################################################################################
  @spec insert(atom() | :ets.tid(), [...] | tuple()) :: :duplicate_record | [:skip | true | false]
  def insert(table, record) when is_tuple(record), do: insert(table, Tuple.to_list(record))
  def insert(table, record) do
    record = record ++ [:ok]
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
        remove_indexes(
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
        index_table = get_table_index_name(:ets.info(table, :name), field_name)
        :ets.insert(index_table, {data, primary_key})
    end
    [result | insert_indexes(table, datas, fields, primary_key) ]
  end

  ################################################################################
  # remove
  ################################################################################
  @spec remove(atom() | :ets.tid(), any()) :: :not_found | [:skip | true | false ]
  def remove(table, primary_key) do
    record = :ets.lookup(table, primary_key)
    if record == [] do
      :not_found
    else
      record = record
        |> hd()
        |> Tuple.to_list()
      fields = Keyword.get(:ets.lookup(table, :fields), :fields)
      remove_indexes(
        table,
        :lists.enumerate(record),
        fields,
        primary_key
      ) ++ [:ets.delete(table, primary_key)]
    end
  end
  defp remove_indexes(_, [], _, _), do: []
  defp remove_indexes(table, [ {idx, data} | datas ], fields, primary_key) do
    result = case :lists.nth(idx, fields) do
      {_, index_type} when index_type in [:primary_key, :unindexed] ->
        :skip

      {field_name, :indexed} ->
        index_table = get_table_index_name(:ets.info(table, :name), field_name)
        :ets.delete(index_table, data)

      {field_name, :indexed_non_uniq} ->
        index_table = get_table_index_name(:ets.info(table, :name), field_name)
        :ets.delete_object(index_table, {data, primary_key})
    end

    [result | remove_indexes(table, datas, fields, primary_key) ]
  end

  @spec remove(atom() | :ets.tid(), any(), any()) :: :ok
  def remove(table, field_name, key) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    :ets.lookup(table_index, key)
      |> Enum.each(fn {_, primary_key} ->
           remove(table, primary_key)
         end)
  end


  ################################################################################
  # delete
  ################################################################################
  @spec delete(atom() | :ets.tid(), any()) :: :not_found | [:skip | true | false ]
  def delete(table, primary_key) do
    pattern = table
      |> build_pattern()
      |> String.replace("sys_flag", "_sys_flag")
    return = String.replace(pattern, "_sys_flag", ":deleted")
    :ets.select_replace(table, filter_string(pattern, "mac == #{primary_key}", return))
  end

  @spec delete(atom() | :ets.tid(), any(), any()) :: :ok
  def delete(table, field_name, key) do
    get(table, field_name, key, return: :keys)
      |> Enum.each(fn {_, pk} ->
           delete(table, pk)
         end)
  end

  ################################################################################
  # delete_list
  ################################################################################
  @spec delete_list(any(), list()) :: :ok
  def delete_list(_table, []), do: :ok
  def delete_list(table, [primary_key | primary_keys]) do
    delete(table, primary_key)
    delete_list(table, primary_keys)
  end

  @spec delete_range(atom() | :ets.tid(), any(), any(), any()) :: list()
  def delete_range(table, field_name, from, to) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [true]}]
    :ets.select_delete(table_index, filter)
  end

  ################################################################################
  # count_range
  ################################################################################
  @spec count_range(atom() | :ets.tid(), any(), any(), any()) :: list()
  def count_range(table, field_name, from, to) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [true]}]
    :ets.select_count(table_index, filter)
  end

  ################################################################################
  # get/2
  ################################################################################
  @spec get(atom() | :ets.tid(), any()) :: :not_found | tuple()
  def get(table, primary_key) do
    case :ets.lookup(table, primary_key) do
      [] -> :not_found
      [record] -> record
    end
  end

  ################################################################################
  # get/3
  ################################################################################
  @spec get(atom() | :ets.tid(), any(), any(), map() | list()) :: list()
  def get(table, field_name, key, opts \\ %{return: :records})
  def get(table, field_name, key, [_|_] = opts), do:
    get(table, field_name, key, Enum.into(opts, %{}))
  def get(table, field_name, key, %{return: :records}) do
    get(table, field_name, key, %{return: :keys})
      |> Enum.map(fn {_, primary_key} ->
           get(table, primary_key)
         end)
  end
  def get(table, field_name, key, %{return: :keys}) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    :ets.lookup(table_index, key)
  end

  ################################################################################
  # get_map/2
  ################################################################################
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

  ################################################################################
  # get_map/3
  ################################################################################
  @spec get_map(atom() | :ets.tid(), any(), any()) :: [map()]
  def get_map(table, field_name, key) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    :ets.lookup(table_index, key)
      |> Enum.map(fn {_, primary_key} ->
           get_map(table, primary_key)
         end)
  end

  ################################################################################
  # get_range
  ################################################################################
  @spec get_range(atom() | :ets.tid(), any(), any(), any(), map()) :: list()
  def get_range(table, field_name, from, to, opts \\ %{return: :keys, limit: :infinity})
  def get_range(table, field_name, from, to, [_|_] = opts), do:
    get_range(table, field_name, from, to, Enum.into(opts, %{}))
  def get_range(table, field_name, from, to, %{return: :records} = opts) do
    get_range(table, field_name, from, to, %{opts|return: :keys})
      |> Enum.map(fn {_, primary_key} -> get(table, primary_key) end)
  end
  def get_range(table, field_name, from, to, opts) do
    table_index = get_table_index_name(:ets.info(table, :name), field_name)
    filter = [{{:"$1", :_}, [{:andalso, {:>=, :"$1", from}, {:<, :"$1", to}}], [:"$_"]}]
    case Map.get(opts, :limit, :infinity) do
      :infinity -> :ets.select(table_index, filter)
      limit when is_integer(limit) -> :ets.select(table_index, filter, limit) |> elem(0)
      _ -> :ets.select(table_index, filter)
    end
  end

  ################################################################################
  # store
  ################################################################################
  def store(table, path) do
    :ets.tab2file(table, path |> to_charlist())
  end

  ################################################################################
  # load
  ################################################################################
  def load(path) do
    case path |> to_charlist() |> :ets.file2tab() do
      {:error, _} = error -> error
      {:ok, table} ->
        reindex(table)
        {:ok, table}
    end
  end

  ################################################################################
  # reindex
  ################################################################################
  def reindex(table) do
    table = :ets.info(table, :name)
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    reindex_indexes(table, fields)
    :ets.tab2list(table)
      |> Enum.each(fn record ->
        primary_key = elem(record, 0)
        insert_indexes(
          table,
          record |> Tuple.to_list() |> :lists.enumerate(),
          fields,
          primary_key
        )
      end)
  end
  defp reindex_indexes(_, []), do: :ok
  defp reindex_indexes(table, [field | fields]) when not is_tuple(field),
    do: reindex_indexes(table, fields)
  defp reindex_indexes(table, [{_, index_type} | fields]) when index_type in [:unindexed, :primary_key],
    do: reindex_indexes(table, fields)
  defp reindex_indexes(table, [{field_name, index_type} | fields]) when index_type in [:indexed, :indexed_non_uniq] do
    index_name = get_table_index_name(table, field_name)
    try do
      :ets.delete(index_name)
    rescue
      _ -> :ok
    end
    new_indexes(table, [{field_name, index_type}])
    reindex_indexes(table, fields)
  end

  ################################################################################
  # custom_filter
  ################################################################################
  def custom_filter(table, guard \\ "true", return \\ "full_record") do
    pattern = build_pattern(table, guard)
    filter_string(pattern, guard, return)
  end

  ################################################################################
  # custom_count
  ################################################################################
  @spec custom_count(:ets.tid(), :full | String.t(), String.t()) :: integer()
  def custom_count(table, :full, guard) do
    custom_count(table, build_pattern(table), guard)
  end
  def custom_count(table, pattern, guard) do
    :ets.select_count(table, filter_string(pattern, guard, return))
  end

  ################################################################################
  # custom_search
  ################################################################################
  def custom_search(table, pattern \\ :full, guard \\ "true", return \\ "full_record")
  def custom_search(table, :full, guard, return) do
    pattern = build_pattern(table)
    custom_search(table, pattern, guard, return)
  end
  def custom_search(table, pattern, guard, return) do
    :ets.select(table, filter_string(pattern, guard, return))
  end

  ################################################################################
  # custom_delete
  ################################################################################
  @spec custom_delete(:ets.tid(), :full | String.t(), String.t()) :: integer()
  def custom_delete(table, :full, guard) do
    custom_delete(table, build_pattern(table), guard)
  end
  def custom_delete(table, pattern, guard) do
    pattern = String.replace(pattern, "sys_flag", "_sys_flag")
    return = String.replace(pattern, "_sys_flag", ":deleted")
    :ets.select_replace(table, filter_string(pattern, guard, return))
  end

  ################################################################################
  # custom_update
  ################################################################################
  # def custom_update(table, guard \\ "true") do
  #   :ets.select_delete(table, custom_filter(guard, []))
  # end


  ################################################################################
  # Small helpers
  ################################################################################
  defp build_pattern(table, guard \\ nil) do
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    guard =
      if guard != nil do
        " #{guard} "
      else
        " #{fields |> Enum.map(fn {n,_} -> to_string(n) end) |> Enum.join(" ")} "
      end

    "{"
      |> Kernel.<>(
        fields
          |> Enum.map(fn {field_name, _} ->
                str = to_string(field_name)
               if String.match?(guard, ~r"[^a-z0-9_]{1}#{str}[^a-z0-9_]{1}") do
                 str
               else
                 "_"
               end
             end)
          |> Enum.join(", ")
         )
      |> Kernel.<>("}")
  end

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
