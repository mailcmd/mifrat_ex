defmodule IMFastTable do
  @moduledoc """

  Module to manage an in-memory table with primary_key and secondary indexes.

  """

  ################################################################################
  # Macros
  ################################################################################
  defmacro filter_string(pattern, guard \\ "true", return \\ "full_record") do
    quote do
      f =
        Code.eval_string("""
        fn #{unquote(pattern)} #{unquote(return) == "full_record" && " = full_record" || ""}
           #{unquote(guard) in ["", "true"] && "" || " when #{unquote(guard)}"} ->
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

  ### destroy/1
  @doc """
  Delete the table and his indexes. No data survive this process. If the table has enable
  **autosave**, before delete the data will be flushed to disk (see `new/3`).
  """
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

  ### new/2 | new/3
  @doc """
  Create a table. The fields is a keyword list; each pair has a field name (the key) and an index
  type (the value). The order of the fields is important and the primary key must be the first.
  ```
  new(:users, [
    id: :primary_key
    name: :unindexed,
    year: :indexed_non_uniq,
    phone: :indexed
  ], [
    gc_period: 3_600_000 # 1 hour
  ])
  ```
  The fields type available are:
  - `:primary_key`: just one field can has this type and it is the main index of the table.
  - `:indexed`: the field will has an auxiliary uniq index.
  - `:indexed_non_uniq`: the field will has an auxiliary non uniq index.
  - `:unindexed`: the field won't be indexed, it is just data.

  The options available are:
  - `:autosave`: Force the flush of the table to disk every `:period` (see below) in `:path` file
  (see below). If true make mandatory `:path`. Default `false`.
  - `:period`: Set how often autosave will flush to disk. It is a value in miliseconds. Default
  `300_000` (5 minutes).
  - `:path`: The path filename where the table will be flushed. Ignored if `autosave: false`.
  - `:gc_period`: Garbage collector period. Default 60_000 miliseconds (1 minute).
  """
  @spec new(name :: atom(), fields :: keyword(), opts :: keyword()) :: :ets.tid()
  def new(table_name, fields, options \\ []) do
    autosave = Keyword.get(options, :autosave, false)
    path = Keyword.get(options, :path, nil)
    period = Keyword.get(options, :period, 300_000)
    # :gc_period random offset is to avoid collision with :autosave
    gc_period = Keyword.get(options, :gc_period, 60_000 + Enum.random(5..10))

    fields = fields ++ [{:sys_flag, :indexed_non_uniq}]

    table = cond do
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
    {:ok, _ref} = :timer.apply_interval(gc_period, fn ->
      garbage_collector(table_name)
    end)
    table
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


  ### insert/2
  @doc """
  Insert or update a record. If the primary key does not exists it insert, otherwise update.
  The fields values must follow the order declared with `new/2` or `new/3`. It can be a list or a
  tuple.
  ```
  insert(:users, {1, "Jorge Luis Borges", 1899, 542915040798})
  ```
  or
  ```
  insert(:users, [1, "Jorge Luis Borges", 1899, 542915040798])
  ```
  """
  @spec insert(table :: atom() | :ets.tid(), list() | tuple()) :: :duplicate_record | [:skip | true | false]
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

  ### delete/2
  @doc """
  Mark as deleted a record. The record is not removed phisically from the table and can be recovered
  using `recover/2` or `recover/3`.
  """
  @spec delete(table :: atom() | :ets.tid(), primary_key :: any()) :: :not_found | integer()
  def delete(table, primary_key) do
    fields = Keyword.get(:ets.lookup(table, :fields), :fields)
    [ {primary_key_field, _} | _ ] = fields
    pattern = table
      |> build_pattern()
      |> String.replace("sys_flag", "_sys_flag")
    return = String.replace(pattern, "_sys_flag", ":deleted")
    guard = "#{primary_key_field} == #{primary_key}"

    case :ets.select_replace(table, filter_string(pattern, guard, return)) do
      0 -> :not_found
      count -> count
    end
    [cur_rec] = :ets.select(table, filter_string(pattern, guard, pattern |> String.replace(", _sys_flag", "")))
    remove_indexes(
      table,
      cur_rec |> Tuple.to_list() |> :lists.enumerate(),
      fields,
      primary_key
    )
  end

  ### delete/3
  @doc """
  Mark as deleted a record. The record is not removed phisically from the table and can be recovered
  using `recover/2` or `recover/3`.
  """
  @spec delete(table :: atom() | :ets.tid(), field_name :: atom(), key :: any()) :: :ok
  def delete(table, field_name, key) do
    get(table, field_name, key, return: :keys)
      |> Enum.each(fn {_, pk} ->
           delete(table, pk)
         end)
  end

  ### delete_list/2
  @doc """
  Mark as deleted many records referenced with its primary key. The records are not removed phisically
  from the table and can be recovered using `recover/2` or `recover/3`.
  """
  @spec delete_list(table :: atom() | :ets.tid(), pk_list :: list()) :: :ok
  def delete_list(_table, []), do: :ok
  def delete_list(table, [primary_key | primary_keys]) do
    delete(table, primary_key)
    delete_list(table, primary_keys)
  end

  ### delete_range/3
  @doc """
  Mark as deleted a range of records referenced by its primary key. The records are not removed
  phisically from the table and can be recovered using `recover/2` or `recover/3`.
  """
  @spec delete_range(table :: atom() | :ets.tid(), from :: any(), to :: any()) :: :ok
  def delete_range(table, from, to) do
    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :fields), :fields)
    guard = "#{primary_key_field} >= #{from} and #{primary_key_field} <= #{to}"
    custom_delete(table, :full, guard)
  end

  ### delete_range/4
  @doc """
  Mark as deleted a range of records referenced by the key in `field_name`. The records are not removed
  phisically from the table and can be recovered using `recover/2` or `recover/3`.
  """
  @spec delete_range(table :: atom() | :ets.tid(), field_name :: atom(),
                     from :: any(), to :: any()) :: :ok
  def delete_range(table, field_name, from, to) do
    list = get_range(table, field_name, from, to)
      |> Enum.map( fn {_, v} -> v end)
    delete_list(table, list)
  end

  ### custom_delete/3
  @doc """
  For internal use.
  """
  # Allow mark as deleted many records selected by a `pattern` and/or `guard`. The `pattern` and
  # the `guard` are strings. The records are not removed phisically from the table and can be recovered
  # using `recover/2` or `recover/3`.
  # Example:
  # iex> custom_delete(:users, "{id, name, year, phone, sys_flag}", "year == 1942")
  # is equal to
  # iex> custom_delete(:users, :full, "year == 1942")
  # The patter MUST include all fields name or order plus the internal field name `sys_flag`.
  @spec custom_delete(table :: atom() | :ets.tid(), :full | String.t(), String.t()) :: integer()
  def custom_delete(table, :full, guard) do
    custom_delete(table, build_pattern(table), guard)
  end
  def custom_delete(table, pattern, guard) do
    pattern = String.replace(pattern, "sys_flag", "_sys_flag")
    return = String.replace(pattern, "_sys_flag", ":deleted")
    :ets.select_replace(table, filter_string(pattern, guard, return))

    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :fields), :fields)

    pattern = build_pattern(table, "#{guard} #{primary_key_field} sys_flag")
      |> String.replace("sys_flag", ":deleted")
    deleted = :ets.select(table, filter_string(pattern, guard, primary_key_field))
    table_index = get_table_index_name(:ets.info(table, :name), :sys_flag)
    :ets.insert(table_index, {:deleted, deleted})

    pattern = String.replace(pattern, ":deleted", ":ok")
    ok = :ets.select(table, filter_string(pattern, "", primary_key_field))
    :ets.insert(table_index, {:ok, ok})
    :ok
  end

  ### count/1
  @doc """
  Return the count of records not marked as deleted.
  """
  @spec count(table :: atom() | :ets.tid()) :: integer()
  def count(table) do
    pattern = table
      |> build_pattern("sys_flag")
      |> String.replace("sys_flag", ":ok")

    :ets.select_count(table, filter_string(pattern, "", "true"))
  end

  ### custom_count
  @doc """
  Return the count of records that match with the `pattern` and/or `guard`. The `pattern` and the
  `guard` are strings.
  """
  @spec custom_count(table :: atom() | :ets.tid(), pattern :: :full | String.t(),
                     guard :: String.t()) :: integer()
  def custom_count(table, pattern \\ :full, guard \\ "true")
  def custom_count(table, :full, guard) do
    custom_count(table, build_pattern(table, guard), guard)
  end
  def custom_count(table, pattern, guard) do
    :ets.select_count(table, filter_string(pattern, guard, "true"))
  end

  ### get/2
  @doc """
  Return the full record referenced by the `primary_key`.
  """
  @spec get(table :: atom() | :ets.tid(), primary_key :: any()) :: :not_found | tuple()
  def get(table, primary_key) do
    case :ets.lookup(table, primary_key) do
      [] -> :not_found
      [record] -> record
    end
  end

  ### get/3
  @doc """
  Return the record of the auxiliary table index or the full record of the table. Use the index
  of `field_name` for the search.
  Option `:return` can be:
    - `return: :records`: return a list of full records from the table
    - `return: :keys`: return a list of tuples from the table index (`{key, primary_key}`)
  """
  @spec get(table :: atom() | :ets.tid(), field_name :: atom(), key :: any(), options :: map() | list()) :: list()
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

  ### get_range
  @doc """
  Return a list of records referenced by the key in `field_name` and with values between `from` and
  `to`.
  """
  @spec get_range(table :: atom() | :ets.tid(), field_name :: atom(),
                     from :: any(), to :: any()) :: list()
  def get_range(table, field_name, from, to, opts \\ %{return: :keys, limit: :infinity})
  def get_range(table, field_name, from, to, [_|_] = opts), do:
    get_range(table, field_name, from, to, Enum.into(opts, %{}))
  def get_range(table, field_name, from, to, %{return: :records} = opts) do
    get_range(table, field_name, from, to, %{opts|return: :keys})
      |> Enum.map(fn
        {_, primary_key} -> get(table, primary_key)
        record -> record
      end)
  end
  def get_range(table, field_name, from, to, opts) do
    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :fields), :fields)
    if field_name == primary_key_field do
      pattern = build_pattern(table, "#{primary_key_field} sys_flag")
        |> String.replace("sys_flag", ":ok")
      filter = filter_string(pattern, "#{primary_key_field} >= #{from} and #{primary_key_field} <= #{to}")
      case Map.get(opts, :limit, :infinity) do
        :infinity -> :ets.select(table, filter)
        limit when is_integer(limit) -> :ets.select(table, filter, limit) |> elem(0)
        _ -> :ets.select(table, filter)
      end
    else
      table_index = get_table_index_name(:ets.info(table, :name), field_name)
      filter = filter_string("{k, pk}", "k >= #{from} and k <= #{to}")
      case Map.get(opts, :limit, :infinity) do
        :infinity -> :ets.select(table_index, filter)
        limit when is_integer(limit) -> :ets.select(table_index, filter, limit) |> elem(0)
        _ -> :ets.select(table_index, filter)
      end
    end
  end

  ### store/2
  @doc """
  Flush the table to disk in the pathname.
  """
  def store(table, pathname) do
    :ets.tab2file(table, pathname |> to_charlist())
  end

  ### load/1
  @doc """
  Load the table from the pathname.
  """
  def load(pathname) do
    case pathname |> to_charlist() |> :ets.file2tab() do
      {:error, _} = error -> error
      {:ok, table} ->
        reindex(table)
        {:ok, table}
    end
  end


  ### reindex/1
  # For internar use. Anyway, you can call this function directly just as
  # any other api call.
  @doc false
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

  ### remove/2
  # For internar use by the garbage collector. Anyway, you can call this function directly just as
  # any other api call.
  @doc false
  @spec remove(table :: atom() | :ets.tid(), primary_key :: any()) :: :not_found | [:skip | true | false ]
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

  ### garbage_collector/1
  # For internar use. The module will run every :gc_period this function. Anyway, you can call
  # directly this function at will.
  @doc false
  @spec garbage_collector(table :: atom() | :ets.tid()) :: integer()
  def garbage_collector(table) do
    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :fields), :fields)
    pattern = build_pattern(table, "sys_flag #{primary_key_field}")
      |> String.replace("sys_flag", ":deleted")
    case custom_search(table, pattern, "", primary_key_field) do
      [] -> 0
      list -> garbage_collector_remover(table, list)
    end
  end
  defp garbage_collector_remover(table, list, count \\ 0)
  defp garbage_collector_remover(_, [], count), do: count
  defp garbage_collector_remover(table, [primary_key | rest], count) do
    remove(table, primary_key)
    garbage_collector_remover(table, rest, count + 1)
  end


  ### custom_filter/3
  # For use in future features. Anyway, you can call this function directly just as any other
  # api call.
  @doc false
  def custom_filter(table, guard \\ "true", return \\ "full_record") do
    pattern = build_pattern(table, guard)
    filter_string(pattern, guard, return)
  end

  ### custom_search/4
  def custom_search(table, pattern \\ :full, guard \\ "true", return \\ "full_record")
  def custom_search(table, :full, guard, return) do
    pattern = build_pattern(table)
    custom_search(table, pattern, guard, return)
  end
  def custom_search(table, pattern, guard, return) do
    :ets.select(table, filter_string(pattern, guard, return))
  end

  def list_table_indexes(table) do
    Keyword.get(:ets.lookup(table, :fields), :fields)
      |> Enum.map(fn {k, _} -> get_table_index_name(:ets.info(table, :name), k) end)
  end

  ################################################################################
  # Small helpers
  ################################################################################
  def build_pattern(table, guard \\ nil) do
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
