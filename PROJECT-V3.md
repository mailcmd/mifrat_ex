# Ideas for the version 3 of IMFastTable
A query language to get/set/del records in the table. Something like:

```
get <field_1>, <field_2> from <table> where <expr>
```
For example for a table like this: 
```
users: {
    id: :primary_key
    name: :unindexed,
    year: :indexed_non_uniq,
    phone: :indexed
}
```
A query could be: 
```
from :users, get: {id, name}, where: id > 20 and year == 1980
```

