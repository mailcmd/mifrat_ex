defmodule IMFastTable.StressTest do
  import IMFastTable

  def fill(count \\ 1000) do
    try do
      destroy(:users)
    rescue
      _ -> IO.puts("Table does not exist!")
    end
    table = new(:users, [
      id: :primary_key,
      name: :unindexed,
      year: :indexed_non_uniq,
      phone: :indexed
    ], [
      autosave: true,
      path: "/tmp/table.ets",
      initial_load: true
    ])

    1..count
      |> Enum.each(fn id ->
        insert(:users, {
          id,
          random_name(),
          Enum.random(1900..2000),
          random_phone()
        })
      end)

    table
  end

  def test() do
    try do
      destroy(:users)
    rescue
      _ -> :ok
    end
    table = new(:users, [
      id: :primary_key,
      name: :unindexed,
      year: :indexed_non_uniq,
      phone: :indexed,
      category: :indexed_non_uniq,
      dni: :indexed
    ], [
    ])

    phone15 = random_phone();
    dni15 = random_phone();
    insert(table, {1, random_name(), 1950, random_phone(), 1, random_phone()})
    insert(table, {2, random_name(), 1950, random_phone(), 2, random_phone()})
    insert(table, {3, random_name(), 1950, random_phone(), 1, random_phone()})
    insert(table, {4, random_name(), 1950, random_phone(), 2, random_phone()})
    insert(table, {5, random_name(), 1960, random_phone(), 1, random_phone()})
    insert(table, {6, random_name(), 1960, random_phone(), 2, random_phone()})
    insert(table, {7, random_name(), 1960, random_phone(), 1, random_phone()})
    insert(table, {8, random_name(), 1960, random_phone(), 2, random_phone()})
    insert(table, {9, random_name(), 1970, random_phone(), 1, random_phone()})
    insert(table, {10, random_name(), 1970, random_phone(), 2, random_phone()})
    insert(table, {11, random_name(), 1970, random_phone(), 1, random_phone()})
    insert(table, {12, random_name(), 1970, random_phone(), 2, random_phone()})
    insert(table, {13, random_name(), 1980, random_phone(), 1, random_phone()})
    insert(table, {14, random_name(), 1980, random_phone(), 2, random_phone()})
    insert(table, {15, random_name(), 1980, phone15, 1, dni15})
    insert(table, {16, random_name(), 1980, random_phone(), 2, random_phone()})
    insert(table, {17, random_name(), 1990, random_phone(), 1, random_phone()})
    insert(table, {18, random_name(), 1990, random_phone(), 2, random_phone()})
    insert(table, {19, random_name(), 1990, random_phone(), 1, random_phone()})
    insert(table, {20, random_name(), 1990, random_phone(), 2, random_phone()})

    # get/2
    IO.inspect(
      get(table, 15),
      label: "\nGET/2\n"
    )
    # get/3
    IO.inspect(
      get(table, :phone, phone15),
      label: "\nGET/3 - by phone\n"
    )
    # get/3
    IO.inspect(
      get(table, :dni, dni15),
      label: "\nGET/3 - by dni\n"
    )

    # get/3
    IO.inspect(
      get(table, :year, 1980),
      label: "\nGET/3 - by year (1980)\n"
    )

    # get/3
    IO.inspect(
      get(table, :category, 1),
      label: "\nGET/3 - by category (1)\n"
    )

    # get_range/5
    IO.inspect(
      get_range(table, :id, 15, 20),
      label: "\nGET_RANGE/5 - by id (15..20)\n"
    )

    # get_range/5
    IO.inspect(
      get_range(table, :year, 1980, 1990),
      label: "\nGET_RANGE/5 - by year (1980..1990)\n"
    )

    # get_range/5
    IO.inspect(
      get_range(table, :phone, phone15, phone15 + 100000000),
      label: "\nGET_RANGE/5 - by phone (#{phone15}..#{phone15 + 100000000})\n"
    )

    # get_range/5
    IO.inspect(
      get_range(table, :dni, dni15, dni15 + 100000000),
      label: "\nGET_RANGE/5 - by dni (#{dni15}..#{dni15 + 100000000})\n"
    )

    # count/1
    IO.inspect(
      count(table),
      label: "\nCOUNT/1\n"
    )

    # custom_count/2
    IO.inspect(
      custom_count(table, "{_, _, _, _, 2, _}"),
      label: "\nCUSTOM_COUNT/2 ({_, _, _, _, 2, _})\n"
    )

    # custom_count/3
    IO.inspect(
      custom_count(table, "{_, _, year, _, cat, _}", "year < 1980 and cat == 2"),
      label: "\nCUSTOM_COUNT/3 ({_, _, year, _, cat, _} -> year < 1980 and cat == 2)\n"
    )

    IO.puts "\nTesting 'delete/2';\nCurrent status:"
    {_id, _name, year, phone, category, dni} = get(table, 15)
    year_list = get(table, :year, year, return: :keys)
    category_list = get(table, :category, category, return: :keys)
    phone_list = get(table, :phone, phone, return: :keys)
    dni_list = get(table, :dni, dni, return: :keys)

    IO.inspect(year_list, label: "year list #{year}")
    IO.inspect(category_list, label: "category list #{category}")
    IO.inspect(phone_list, label: "phone list #{phone}")
    IO.inspect(dni_list, label: "dni list #{dni}")

    IO.puts "\nStatus deleting id: 15..."
    delete(table, 15)

    year_list = get(table, :year, year, return: :keys)
    category_list = get(table, :category, category, return: :keys)
    phone_list = get(table, :phone, phone, return: :keys)
    dni_list = get(table, :dni, dni, return: :keys)

    IO.inspect(year_list, label: "year list #{year}")
    IO.inspect(category_list, label: "category list #{category}")
    IO.inspect(phone_list, label: "phone list #{phone}")
    IO.inspect(dni_list, label: "dni list #{dni}")

    IO.puts "\nInserting again id: 15..."
    insert(table, {15, random_name(), 1980, phone15, 1, dni15})

    year_list = get(table, :year, year, return: :keys)
    category_list = get(table, :category, category, return: :keys)
    phone_list = get(table, :phone, phone, return: :keys)
    dni_list = get(table, :dni, dni, return: :keys)

    IO.inspect(year_list, label: "year list #{year}")
    IO.inspect(category_list, label: "category list #{category}")
    IO.inspect(phone_list, label: "phone list #{phone}")
    IO.inspect(dni_list, label: "dni list #{dni}")

    IO.puts "\nTeting 'delete/3':\nStatus deleting year: #{year}..."
    delete(table, :year, year)
    IO.inspect(
      custom_search(table, "{_, _, _, _, _, _}"),
      label: "Without year #{year}\n"
    )

    IO.inspect(
      get(table, :year, year, return: :keys),
      label: "using 'get/3' for year: #{year}\n"
    )

    IO.inspect(
      get(table, :phone, phone15, return: :keys),
      label: "using 'get/3' for phone: #{phone15}\n"
    )

    IO.puts "\nInserting again year: 1980..."
    insert(table, {13, random_name(), 1980, random_phone(), 1, random_phone()})
    insert(table, {14, random_name(), 1980, random_phone(), 2, random_phone()})
    insert(table, {15, random_name(), 1980, phone15, 1, dni15})
    insert(table, {16, random_name(), 1980, random_phone(), 2, random_phone()})
    IO.inspect(custom_search(table, "{_, _, _, _, _, _}"))

    IO.puts "\nTeting 'delete_list/2':\nStatus deleting: #{inspect([13,14,15,16], charlists: :as_lists)}..."
    delete_list(table, [13,14,15,16])
    IO.inspect(
      custom_search(table, "{_, _, _, _, _, _}")
    )

    IO.puts "\nInserting again #{inspect([13,14,15,16], charlists: :as_lists)}..."
    insert(table, {13, random_name(), 1980, random_phone(), 1, random_phone()})
    insert(table, {14, random_name(), 1980, random_phone(), 2, random_phone()})
    insert(table, {15, random_name(), 1980, phone15, 1, dni15})
    insert(table, {16, random_name(), 1980, random_phone(), 2, random_phone()})
    IO.inspect(custom_search(table, "{_, _, _, _, _, _}"))

    IO.puts "\nTeting 'delete_range/3':\nStatus deleting from 8 to 12 ..."
    delete_range(table, 8, 12)
    IO.inspect(
      custom_search(table, "{_, _, _, _, _, _}")
    )

    IO.puts "\nInserting again 8 to 12..."
    insert(table, {8, random_name(), 1960, random_phone(), 2, random_phone()})
    insert(table, {9, random_name(), 1970, random_phone(), 1, random_phone()})
    insert(table, {10, random_name(), 1970, random_phone(), 2, random_phone()})
    insert(table, {11, random_name(), 1970, random_phone(), 1, random_phone()})
    insert(table, {12, random_name(), 1970, random_phone(), 2, random_phone()})
    IO.inspect(
      custom_search(table, "{_, _, _, _, _, _}")
    )

    IO.puts "\nTeting 'delete_range/3':\nStatus deleting from 2 to 12 ..."
    delete_range(table, 2, 12)
    IO.inspect(:ets.tab2list(table))


    :ready
  end

  ##########################
  ### Helpers
  defp random_name() do
    (for _ <- 1..Enum.random(5..10), into: "", do: <<Enum.random(~c"abcdefghijklmnopqrstuvwxyz")>>)
    <> " " <>
    (for _ <- 1..Enum.random(5..10), into: "", do: <<Enum.random(~c"abcdefghijklmnopqrstuvwxyz")>>)
  end

  defp random_phone() do
    (for _ <- 1..9, into: "", do: <<Enum.random(~c"0123456789")>>)
      |> String.to_integer()
  end
end
