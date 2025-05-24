# In Memory Fast Table

## Funcionamiento
La tabla se crea detallando los campos. Importa el nombre del campo, el tipo de campo (si es PK, 
IDX o IDX_NOT_UNIQ) y el orden en que se los declara. 
A partir de allí se crean unos registros internos de la tabla que detallan la estructura y 
mantienen ciertos metadatos que permiten manejar la tabla. 
Por cada índice extra de la tabla (no para la PK) se crea una tabla auxiliar. Para los índices 
sin repetición se crea una tabla con la estructura de registro:
```
{idx_value, primary_key}
```

Para los índices con repetición se crea una tabla con la estructura de registro:
```
{idx_value, [primary_key1, primary_ke2, ...]}
```

## Insersión
Supongamos que la estructura de la tabla es: 
```
[
    {:id, :primary_key},
    {:name, :unindexed},
    {:year_of_birth, :indexed_non_uniq},
    {:phone, indexed}
]
```

y la tabla se llama *:users*. 

En una insersión se podría agregar un registro como:
```
{
    id: 1,
    name: "Carlos Poncho",
    year_of_birth: 1972,
    phone: 2915031105
}
```
Esto provoca las siguiente acciones:

1. Se crea/agrega el índice de *:year_of_birth* a la tabla auxiliar correspondiente 
(*:users_year_of_birdh_index*):
```
{1972, [1]}
```
2. Se crea/agrega el índice de *:phone* a la tabla auxiliar correspondiente (*:users_phone_index*):
```
{2915031105, 1}
```
3. Se agrega el registro completo en la tabla principal (*:users*):
```
{1, "Carlos Poncho", 1972, 2915031105}
``` 

En este momento tendríamos las tablas con la siguiente información:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 

|users_year_of_birdh_index|
|-----|
|{1972, [1]}|

|users_phone_index|
|-----|
|{2915031105, 1}|

Si agregáramos otros registros:
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

Y agregamos otro:

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

## Actualización
Ahora vamos a suponer que queremos modificar el registro: 
```
{2, "Juan de las Pelotas", 1976, 2915010203}
```
por
```
{2, "Juan de las Pelotas", 1972, 2915012233}
```

Al hacer el update ocurre lo siguiente:

1. Primero se actualizan los índices:
   
|users_year_of_birdh_index|
|-----|
|{1972, [**2**, 3, 1]}|

|users_phone_index|
|-----|
|{2915031105, 1}|
|{**2915012233**, 2}|
|{2915040506, 3}|

2. Luego se actualiza el registro principal:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1972, 2915012233}|
|{3, "Pablo Marmol", 1972, 2915040506}|

## Borrado lógico
El borrado lógico no hace desaparecer la información sino que la marca para su posterior 
eliminación. Si queremos borrar lógicamente el registro con PK = 3 los índices quedarían igual y la tabla
principal luciría de este modo:

|users|
|-----|
|{1, "Carlos Poncho", 1972, 2915031105}| 
|{2, "Juan de las Pelotas", 1972, 2915012233}|
|~~{3, "Pablo Marmol", 1972, 2915040506}~~|

## Borrado físico
El borrado físico elimina el registro principal y sus referencias en los índices. Si queremos borrar 
lógicamente el registro con PK = 3 las tablas quedarían de este modo:

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


