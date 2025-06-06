defmodule IMFastTable do
  @moduledoc """

  Module to manage an in-memory table with primary_key and secondary indexes.

  """

  ################################################################################
  # Macros
  ################################################################################
  defmacro filter_string(pattern, guard \\ "true", return \\ "full_record") do
    quote generated: true do
      f =
        Code.eval_string("""
        fn (#{unquote(pattern)} #{unquote(return) == "full_record" && " = full_record" || ""})
           #{unquote(guard) in ["", "true"] && "" || " when #{unquote(guard)}"} ->
          #{unquote(return)}
        end
        """) |> elem(0)
      :ets.fun2ms(f)
    end
  end

  defmacro filter(pattern, guard \\ true, return \\ quote do: full_record) do
    quote generated: true do
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
  @spec destroy(atom() | :ets.tid()) :: :ok | :error
  def destroy(table) do
    # if autosave: true cancel autosave timer
    case :ets.lookup(table, :_autosave) do
      [] -> :ok
      [{:_autosave, ref, path}] ->
        :timer.cancel(ref)
        store(table, path)
    end

    # cancel garbage collector timer
    # [{:_gc, ref}] = :ets.lookup(table, :_gc)
    # :timer.cancel(ref)

    # delete the main table
    :ets.delete(table) && :ok || :error
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
    ...opts...
  ])
  ```
  The fields type available are:
  - `:primary_key`: just one field can has this type and it is the main index of the table.
  - `:indexed`: the field will has an auxiliary uniq index.
  - `:indexed_non_uniq`: the field will has an auxiliary non uniq index.
  - `:unindexed`: the field won't be indexed, it is just data.

  The options available are:
  - `:_autosave`: Force the flush of the table to disk every `:period` (see below) in `:path` file
  (see below). If true make mandatory `:path`. Default `false`.
  - `:period`: Set how often autosave will flush to disk. It is a value in miliseconds. Default
  `300_000` (5 minutes).
  - `:path`: The path filename where the table will be flushed. Ignored if `autosave: false`.
  - `:initial_load`: If `true` and `autosave: true` will try to load from `:path` the table. Ignored
  if `autosave: false`. Default is `true`.
  """
  @spec new(name :: atom(), fields :: keyword(), opts :: keyword()) :: :ets.tid()
  def new(table_name, fields, options \\ []) do
    autosave = Keyword.get(options, :autosave, false)
    path = Keyword.get(options, :path, nil)
    period = Keyword.get(options, :period, 300_000)
    initial_load = Keyword.get(options, :initial_load, true)
    # :_gc_period random offset is to reduce risk of collision with :_autosave
    # gc_period = Keyword.get(options, :gc_period, 60_000) + Enum.random(5000..10000)

    fields = fields

    table = cond do
      autosave and (not is_binary(path) or path == "") ->
        raise(ArgumentError, message: "Bad options: 'autosave = true' make mandatory 'path' parameter")

      autosave and initial_load ->
        table = case load(path) do
          {:ok, table} ->
            table
          {:error, _} ->
            table = :ets.new(table_name, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
            :ets.insert(table, {:_fields, fields})
            table
        end
        {:ok, ref} = :timer.apply_interval(period, fn ->
          [{_, _, path}] = :ets.lookup(table, :_autosave)
          store(table_name, path)
        end)
        :ets.insert(table, {:_autosave, ref, path})
        table

      true ->
        table = :ets.new(table_name, [:ordered_set, :public, :named_table, read_concurrency: true, write_concurrency: true])
        :ets.insert(table, {:_fields, fields})
        table
    end
    # {:ok, ref} = :timer.apply_interval(gc_period, fn ->
    #   garbage_collector(table_name)
    # end)
    # :ets.insert(table, {:_gc, ref})
    table
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
    record = record
    fields = Keyword.get(:ets.lookup(table, :_fields), :_fields)
    primary_key_idx = find_primary_key_idx(fields)
    primary_key = :lists.nth(primary_key_idx, record)
    current_record = :ets.lookup(table, primary_key)

    ## NOTE: Missing control of duplicated secondary uniq index (I promise to do it!)

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
        delete(table, primary_key)
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

      {field_name, index_type} when index_type == :indexed ->
        table_index = get_table_index_name(field_name)
        :ets.insert(table, {{table_index, data}, primary_key})

      {field_name, index_type} when index_type  == :indexed_non_uniq ->
        table_index = get_table_index_name(field_name)
        new_list = case get(table, {table_index, data}) do
          {_, pk_list} -> [ primary_key | pk_list ]
          :not_found -> [ primary_key ]
        end
        :ets.insert(table, {{table_index, data}, new_list})
      end
    [result | insert_indexes(table, datas, fields, primary_key) ]
  end

  ### delete/2
  @doc """
  Delete a record by primary_key
  """
  @spec delete(table :: atom() | :ets.tid(), primary_key :: any()) :: :not_found | :ok
  def delete(table, primary_key) do
    fields = Keyword.get(:ets.lookup(table, :_fields), :_fields)
    current_record = :ets.lookup(table, primary_key)
    if current_record != [] do
      :ets.delete(table, primary_key)
      delete_indexes(
        table,
        current_record |> hd() |> Tuple.to_list() |> :lists.enumerate(),
        fields,
        primary_key
      )
    else
      :not_found
    end
  end
  defp delete_indexes(_, [], _, _), do: []
  defp delete_indexes(table, [ {idx, data} | datas ], fields, primary_key) do
    result = case :lists.nth(idx, fields) do
      {_, index_type} when index_type in [:primary_key, :unindexed] ->
        :skip

      {field_name, :indexed} ->
        table_index = get_table_index_name(field_name)
        :ets.delete(table, {table_index, data})

      {field_name, :indexed_non_uniq} ->
        table_index = get_table_index_name(field_name)
        with {_, pk_list} <- get(table, {table_index, data}),
             new_list when is_list(new_list) <- List.delete(pk_list, primary_key) do
          if new_list == [] do
            :ets.delete(table, {table_index, data})
          else
            :ets.insert(table, {{table_index, data}, new_list})
          end
          :ok
        else
          _ -> :not_found
        end
    end

    [result | delete_indexes(table, datas, fields, primary_key) ]
  end


  ### delete/3
  @doc """
  Delete one or more records by a secondary_key
  """
  @spec delete(table :: atom() | :ets.tid(), field_name :: atom(), key :: any()) :: integer()
  def delete(table, field_name, key) do
    table_index = get_table_index_name(field_name)
    case :ets.lookup(table, {table_index, key}) do
      [{_, list}] when is_list(list) ->
        delete_list(table, list)
        length(list)
      [{_, pk}] ->
        delete(table, pk)
        1
    end
  end

  ### delete_list/2
  @doc """
  Delete many records referenced with its primary key.
  """
  @spec delete_list(table :: atom() | :ets.tid(), pk_list :: list()) :: :ok
  def delete_list(_table, []), do: :ok
  def delete_list(table, [primary_key | primary_keys]) do
    delete(table, primary_key)
    delete_list(table, primary_keys)
  end

  ### delete_range/3
  @doc """
  Delete a bunch of records referenced by a range of its primary key.
  """
  @spec delete_range(table :: atom() | :ets.tid(), from :: any(), to :: any()) :: :ok
  def delete_range(table, from, to) do
    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :_fields), :_fields)
    guard = "#{primary_key_field} >= #{from} and #{primary_key_field} <= #{to}"
    custom_search(table, :full, guard)
      |> Enum.each(fn tuple ->
        delete(table, elem(tuple, 0))
      end)
  end

  ### delete_range/4
  @doc """
  Delete a bunch of records referenced by a range of one of its secondary indexes keys.
  """
  @spec delete_range(table :: atom() | :ets.tid(), field_name :: atom(),
                     from :: any(), to :: any()) :: integer()
  def delete_range(table, field_name, from, to) do
    list = get_range(table, field_name, from, to)
    list
      |> Enum.each(fn {_, pk} -> delete(table, pk) end)
    length(list)
  end

  ### delete_all/1
  @doc """
  WARNING!!! This function remove physically every record in the table.
  """
  @spec delete_all(table :: atom() | :ets.tid()) :: :ok | :error
  def delete_all(table) do
    [fields] = :ets.lookup(table, :_fields)
    [autosave] = :ets.lookup(table, :_autosave)
    if :ets.delete_all_objects(table) do
      :ets.insert(table, fields)
      :ets.insert(table, autosave)
      :ok
    else
      :error
    end
  end

  ### custom_delete/3
  @doc false
  # For internal use.
  # REMEMBER: The pattern MUST include all fields name in order.
  @spec custom_delete(table :: atom() | :ets.tid(), :full | String.t(), String.t()) :: integer()
  def custom_delete(table, :full, guard) do
    custom_delete(table, build_pattern(table), guard)
  end
  def custom_delete(table, pattern, guard) do
    # return = build_pattern(table)
    :ets.select(table, filter_string(pattern, guard))
      |> Enum.map(fn tuple ->
        delete(table, elem(tuple, 0))
      end)
      |> length()
  end

  ### count/1
  @doc """
  Return the number of records of the table.
  """
  @spec count(table :: atom() | :ets.tid()) :: integer()
  def count(table) do
    :ets.select_count(table, filter_string(build_pattern(table, ""), "", "true"))
  end

  ### custom_count
  @doc """
  Return the count of records that match with the `pattern` and/or `guard`. The `pattern` and the
  `guard` are strings.
  If pattern is `:full` it will be expanded to a tuple of all fields taking the field names declared
  with `new/2`.
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
  Return the full record referenced by the primary_key.
  """
  @spec get(table :: atom() | :ets.tid(), primary_key :: any()) :: :not_found | tuple()
  def get(table, primary_key) do
    case :ets.lookup(table, primary_key) do
      [record] -> record
      _ -> :not_found
    end
  end

  ### get/3
  @doc """
  Return one o more records using a secondary index key. Return the the auxiliary indexed record or
  the full main record of the table.
  Option `:return` can be:
    - `return: :records`: return a list of full records from the table
    - `return: :keys`: return a list of auxiliary secondary index record (`{{:<index>, key}, primary_key}`)
  """
  @spec get(table :: atom() | :ets.tid(), field_name :: atom(), key :: any(), options :: map() | list())
            :: list(tuple())
  def get(table, field_name, key, opts \\ %{return: :records})
  def get(table, field_name, key, [_|_] = opts), do:
    get(table, field_name, key, Enum.into(opts, %{}))
  def get(table, field_name, key, %{return: :records}) do
    case get(table, field_name, key, %{return: :keys}) do
      [] = list ->
        list

      list ->
        list
          |> Enum.map(fn {_, pk} ->
              get(table, pk)
            end)
    end
  end
  def get(table, field_name, key, %{return: :keys}) do
    table_index = get_table_index_name(field_name)
    :ets.lookup(table, {table_index, key})
  end

  ### get_range/4 get_range/5
  @doc """
  Delete a bunch of records referenced by a range of one of its primary key or its secondary indexes
  keys.
  If you have a table like this:
  ```
  new(:users, [
    id: :primary_key
    name: :unindexed,
    year: :indexed_non_uniq,
    phone: :indexed
  ], [
    ...
  ])
  ```
  You can get a range of records using the primary key:
  ```
  get_range(table, :id, 100, 200)  # get the records with id >= 100 and <= 200
  ```
  or get a range of records using the key of a secondary index:
  ```
  get_range(table, :year, 1940, 1950)  # get the records with year >= 1940 and <= 1950
  ```
  Just as in `get/3` you can get a list of full records from the table or a list of auxiliary
  secondary index record (return: :records or return: :keys)
  """
  @spec get_range(table :: atom() | :ets.tid(), field_name :: atom(),
                     from :: any(), to :: any()) :: list()
  def get_range(table, field_name, from, to, opts \\ %{return: :keys, limit: :infinity})
  def get_range(table, field_name, from, to, [_|_] = opts), do:
    get_range(table, field_name, from, to, Enum.into(opts, %{}))
  def get_range(table, field_name, from, to, %{return: :records} = opts) do
    get_range(table, field_name, from, to, %{opts|return: :keys})
      |> Enum.flat_map(fn
        {_, list} when is_list(list) -> list |> Enum.map(&(get(table, &1)))
        {_, pk} -> [get(table, pk)]
        record -> [record]
      end)
  end
  def get_range(table, field_name, from, to, opts) do
    [ {primary_key_field, _} | _ ] = Keyword.get(:ets.lookup(table, :_fields), :_fields)
    if field_name == primary_key_field do
      pattern = build_pattern(table, "#{primary_key_field}")
      filter = filter_string(pattern, "#{primary_key_field} >= #{from} and #{primary_key_field} <= #{to}")
      case Map.get(opts, :limit, :infinity) do
        :infinity -> :ets.select(table, filter)
        limit when is_integer(limit) -> :ets.select(table, filter, limit) |> elem(0)
        _ -> :ets.select(table, filter)
      end
    else
      table_index = get_table_index_name(field_name)
      filter = filter_string("{{#{table_index}, k}, _pk}", "k >= #{from} and k <= #{to}")
      case Map.get(opts, :limit, :infinity) do
        :infinity -> :ets.select(table, filter)
        limit when is_integer(limit) -> :ets.select(table, filter, limit) |> elem(0)
        _ -> :ets.select(table, filter)
      end
    end
  end

  ### record_to_map/2
  @doc """
  Convert a tuple record into a map. If the first argument is not a tuple, return just the argument
  passed. That is because many times the record come from a `get/n` call and can take as value
  :not_found.
  """
  @spec record_to_map(record :: tuple(), table :: atom() | :ets.tid()) :: map()
  def record_to_map(record, _table) when not is_tuple(record), do: record
  def record_to_map(record, table) do
    Keyword.get(:ets.lookup(table, :_fields), :_fields)
      |> Enum.map(fn {f, _} -> f end)
      |> :lists.enumerate()
      |> Enum.map(fn {i, k} -> {k, elem(record, i-1)} end)
      |> Enum.into(%{})
  end

  ### map_to_record/2
  @doc """
  Convert a map into a tuple record
  """
  @spec map_to_record(map :: map(), table :: atom() | :ets.tid()) :: map()
  def map_to_record(%{} = map, table) do
    Keyword.get(:ets.lookup(table, :_fields), :_fields)
      |> Enum.reduce([], fn {f, _} -> map[f] end)
      |> List.to_tuple()
  end

  ### store/2
  @doc """
  Flush the table to disk in the file pointed by pathname.
  """
  @spec store(table :: atom() | :ets.tid(), pathname :: String.t()) :: true | false
  def store(table, pathname) do
    :ets.tab2file(table, pathname |> to_charlist())
  end

  ### load/1
  @doc """
  Load the table from file in pathname.
  """
  @spec load(pathname :: String.t()) :: {:ok, table :: atom() | :ets.tid()} | {:error, term()}
  def load(pathname) do
    pathname |> to_charlist() |> :ets.file2tab()
  end

  ### reindex/1
  # For internar use. Anyway, you can call this function directly just as
  # any other api call.
  @doc """
  Clear every secondary indexed record and rebuild the secondary index info.
  """
  def reindex(table) do
    fields = Keyword.get(:ets.lookup(table, :_fields), :_fields)
    :ets.select_delete(table, filter_string("{pk, _}", "pk not in [:_autosave, :_fields]", "true"))

    :ets.tab2list(table)
      |> Stream.filter(fn record -> elem(record, 0) not in [:_autosave, :_fields] end)
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

  ### garbage_collector/1
  # For internar use. The module will run every :_gc_period this function. Anyway, you can call
  # directly this function at will.
  # @doc false
  # @spec garbage_collector(table :: atom() | :ets.tid()) :: integer()
  # def garbage_collector(table) do
  #   pattern = build_pattern(table, "")
  #     |> String.replace("_}", ":deleted}")
  #   :ets.select_delete(table, filter_string(pattern, "", "true"))
  # end

  ### custom_filter/3
  # For use in future features. Anyway, you can call this function directly just as any other
  # api call.
  @doc false
  def custom_filter(table, guard \\ "true", return \\ "full_record") do
    pattern = build_pattern(table, guard)
    filter_string(pattern, guard, return)
  end

  ### custom_search/4
  # For internar use. Anyway, you can call directly this function at will.
  @doc false
  def custom_search(table, pattern \\ :full, guard \\ "true", return \\ "full_record")
  def custom_search(table, :full, guard, return) do
    pattern = build_pattern(table, guard)
    custom_search(table, pattern, guard, return)
  end
  def custom_search(table, pattern, guard, return) do
    :ets.select(table, filter_string(pattern, guard, return))
  end

  ### custom_update/4
  # For internar use. Anyway, you can call  directly this function at will.
  # WARNING: Do not use this function to update fields that are :indexed or :indexed_non_uniq
  # TODO: raise an error if you try to update a field :indexed or :indexed_non_uniq
  @doc false
  def custom_update(table, pattern \\ :full, guard \\ "true", return \\ "full_record")
  def custom_update(table, :full, guard, return) do
    pattern = build_pattern(table)
    custom_update(table, pattern, guard, return)
  end
  def custom_update(table, pattern, guard, return) do
    :ets.select_replace(table, filter_string(pattern, guard, return))
  end

  def list_table_indexes(table, type \\ [:indexed, :indexed_non_uniq]) do
    Keyword.get(:ets.lookup(table, :_fields), :_fields)
      |> Enum.filter(fn {_, t} -> t in type end)
      |> Enum.map(fn {k, _} -> get_table_index_name(k) end)
  end

  ################################################################################
  # Small helpers
  ################################################################################
  @doc false
  def build_pattern(table, guard \\ nil) do
    fields = Keyword.get(:ets.lookup(table, :_fields), :_fields)
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

  # defp find_field_idx(keyword_list, field_name) do
  #   Enum.find_index(keyword_list, fn {f,_} -> f == field_name end) + 1
  # end

  # defp get_table_index_name(table_name, field_name) do
  #   table_name
  #     |> to_string()
  #     |> Kernel.<>("_#{field_name}_index")
  #     |> String.to_atom()
  # end

  defp get_table_index_name(field_name) do
    String.to_atom("_#{field_name}_index")
  end

end
