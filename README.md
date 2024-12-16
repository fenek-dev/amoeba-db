# amoeba-db
Stupidly simple and naïve db. Mostly for educational purpose.

## File Layouts

```
                        MAIN FILE
|--------------------------HEADERS--------------------------|
| magic string 10B | table count 4B |                       | 
|--------------------MAP OF TABLES--------------------------|
| id 16B | table name 32B |                                 |
| id 16B | table name 32B |                                 |
|-----------------------------------------------------------|

                        TABLE FILE
|------------------------HEADERS----------------------------|
| table name 32B | page num 4B | col num 4B |               |
| col1 name 32B | col1 type 2B | col1 size 2B |             |
|------------------------PAGE-------------------------------|
| type 2B | next page 8B |                                  |
| prev page 8B | row count 4B | free space 4B |             |
|------------------------DATA-------------------------------|
| column1 , column2 , column3 \n                            |
|-----------------------------------------------------------|
```