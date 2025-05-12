defmodule LeaseManager do
  # use Agent

  # def start_link(name \\ nil) do
  #   Agent.start_link(fn ->
  #     IMFastTable.new([
  #       {:mac, :primary_key},      # MAC Address
  #       :ip,                       # IP Address
  #       :amac,                     # Remote Agente MAC Address
  #       :rip,                      # Relay Agent IP Address
  #       {:exp, :indexed_non_uniq}, # Expire time in unix timestamp
  #       {:upd, :unindexed}         # Last update time in unix timestamp
  #     ])
  #   end, name: name || __MODULE__)
  # end

  # def handle_info(_, state) do
  #   {:noreply, state}
  # end

  # def insert(map) do
  #   Agent.update(__MODULE__, fn table ->
  #     {_, field_name} = table[:fields][table[:primary_key]]
  #     case map[field_name] do
  #       nil ->
  #         Log.log(:error, "[DHCP][LeaseManager]: Missing field '#{field_name}' in lease data (#{inspect map})")
  #         table
  #       primary_key ->
  #         IMFastTable.get_map(table, primary_key)
  #     end
  #     IMFastTable.insert()
  #   end)
  # end

end
