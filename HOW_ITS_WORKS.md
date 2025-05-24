# In Memory Fast Table

## About the storage structure

The table is created by detailing the fields. The name of the field, the type of field (if it is PK, 
IDX or IDX_NOT_UNIQ) and the order in which they are declared are important. 
From there, internal table records are created that detail the structure and 
holds certain metadata that allows the table to be managed. 
For each extra index of the table (not for the PK) an auxiliary table is created. For the indexes 
without repetition a table with this record structure is created:

```
{idx_value, primary_key}
```

For indexes with repetition, a table with this record structure is created:
```
{idx_value, [primary_key1, primary_ke2, ...]}
```

## Insertion
Suppose the structure of the table is:
```elixir
[
    {:id, :primary_key},
    {:name, :unindexed},
    {:year_of_birth, :indexed_non_uniq},
    {:phone, indexed}
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

1. The index entry of *:year_of_birth* is created/added to the corresponding auxiliary table 
(*:users_year_of_birdh_index*):
```
{1972, [1]}
```
2. The index entry of *:phone* is created/added to the corresponding auxiliary table (*:users_phone_index*):
```
{2915031105, 1}
```
3. The complete record is added to the main table (*:users*):
```
{1, "Carlos Poncho", 1972, 2915031105}
``` 

At this point we would have the tables with the following information:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 

|users_year_of_birdh_index|
|-----|
|{1972, [1]}|

|users_phone_index|
|-----|
|{2915031105, 1}|

If we were to add other records:
``` 
{2, "Juan de las Pelotas", 1976, 2915010203}
``` 
|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1976, 2915010203}|

|users_year_of_birdh_index|
|-----|
|{1972, [1]}|
|{1976, [2]}|

|users_phone_index|
|-----|
|{2915031105, 1}|
|{2915010203, 2}|

And we added another one:

``` 
{3, "Pablo Marmol", 1972, 2915040506}
``` 
|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1976, 2915010203}|
|{3, "Pablo Marmol", 1972, 2915040506}|

|users_year_of_birdh_index|
|-----|
|{1972, [3, 1]}|
|{1976, [2]}|

|users_phone_index|
|-----|
|{2915031105, 1}|
|{2915010203, 2}|
|{2915040506, 3}|

## Update
Now let's assume that we want to modify the registry:
```
{2, "Juan de las Pelotas", 1976, 2915010203}
```
by
```
{2, "Juan de las Pelotas", 1972, 2915012233}
```

The following happens when updating:

1. First the indexes are updated:
   
|users_year_of_birdh_index|
|-----|
|{1972, [**2**, 3, 1]}|

|users_phone_index|
|-----|
|{2915031105, 1}|
|{**2915012233**, 2}|
|{2915040506, 3}|

2. The main register is then updated:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1972, 2915012233}|
|{3, "Pablo Marmol", 1972, 2915040506}|

## Logical deletion
Logical deletion does not make the information disappear but marks it for later deletion 
. If we want to logically delete the record with PK = 3, the indexes would remain the same and the main table
would look like this:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1972, 2915012233}|
|~~{3, "Pablo Marmol", 1972, 2915040506}~~|

## Physical deletion
The physical deletion removes the main record and its references in the indexes. If we want to fiscally delete 
the record with PK = 3, the tables would look like this:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1972, 2915012233}|

|users_year_of_birdh_index|
|-----|
|{1972, [2, 1]}|

|users_phone_index|
|-----|
|{2915031105, 1}|
|{2915012233, 2}|


