# In Memory Fast Table

## About the storage structure

The table is created by detailing the fields. The name of the field, the type of field (if it is PK, 
IDX or IDX_NOT_UNIQ) and the order in which they are declared are important. 
From there, internal table records are created that detail the structure and 
holds certain metadata that allows the table to be managed. 
For every extra index of the table (not for the PK), when you insert data in the table, an auxiliary 
record with this structure is inserted in the table.

```
{{:_<field_name>_index, idx_value}, primary_key}
```

## Insertion
Suppose the structure of the table is:
```elixir
[
    {:id, :primary_key},
    {:name, :unindexed},
    {:year_of_birth, :indexed_non_uniq},
    {:phone, :indexed}
]
```

and the table is called *:users*. 

In an insertion you could add a record such as:
```elixir
%{
    id: 1,
    name: "Carlos Poncho",
    year_of_birth: 1972,
    phone: 2915031105
}
```
This causes the following actions:

1. Add the complete record to the table:
```
{1, "Carlos Poncho", 1972, 2915031105}
```
2. Add a index record for year:
```
{{:_year_index, 1972}, 1}
``` 
3. Add a index record for phone:
```
{{:_phone_index, 2915031105}, 1}
```

At this point we would have the table with the following information:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{{:_year_index, 1972}, 1}|
|{{:_phone_index, 2915031105}, 1}|

If we now add other records:
``` 
{2, "Juan de las Pelotas", 1976, 2915010203}
``` 
|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{{:_year_index, 1972}, 1}|
|{{:_phone_index, 2915031105}, 1}|
|{2, "Juan de las Pelotas", 1976, 2915010203}|
|{{:_year_index, 1976}, 2}|
|{{:_phone_index, 2915010203}, 2}|

And then we add another one:
``` 
{3, "Pablo Marmol", 1972, 2915040506}
``` 
|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{{:_year_index, 1972}, 1}|
|{{:_phone_index, 2915031105}, 1}|
|{2, "Juan de las Pelotas", 1976, 2915010203}|
|{{:_year_index, 1976}, 2}|
|{{:_phone_index, 2915010203}, 2}|
|{3, "Pablo Marmol", 1972, 2915040506}|
|{{:_year_index, 1972}, 3}|
|{{:_phone_index, 2915040506}, 3}|

## Update
Now let's assume that we want to modify the registry:
```
{2, "Juan de las Pelotas", 1976, 2915010203}
```
by
```
{2, "Juan de las Pelotas", 1972, 2915012233}
```

The table would now look like this:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{{:_year_index, 1972}, 1}|
|{{:_phone_index, 2915031105}, 1}|
|{2, "Juan de las Pelotas", **1972**, **2915012233**}|
|{{:_year_index, **1972**}, 2}|
|{{:_phone_index, **2915012233**}, 2}|
|{3, "Pablo Marmol", 1972, 2915040506}|
|{{:_year_index, 1972}, 3}|
|{{:_phone_index, 2915040506}, 3}|

