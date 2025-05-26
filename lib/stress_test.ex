defmodule IMFastTable.StressTest do
  import IMFastTable

  def fill(count \\ 1000) do
    try do
      destroy(:users)
    rescue
      _ -> :ok
    end
    table = new(:users, [
      id: :primary_key,
      name: :unindexed,
      year: :indexed_non_uniq,
      phone: :indexed
    ], [
      gc_period: 3_600_000 # 1 hour
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
