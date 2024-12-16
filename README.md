# amoeba-db
Stupidly simple and naïve db. Mostly for educational purpose.

## File Layout

```
|--------------------------HEADERS--------------------------|
| magic number 10B | table count 4B |                       | 
|--------------------MAP OF TABLES--------------------------|
| table1 pages 4B | table offset 8B | table name 64B |      |
| table2 pages 4B | table offset 8B | table name 64B |      |
|------------------------TABLE PAGE-------------------------|
| table name 64B | page num 8B | next page address 8B |     |
|------------------------TABLE DATA-------------------------|
| column1 , column2 , column3 \n                            |
|-----------------------------------------------------------|
```