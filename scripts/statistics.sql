### Count number of tables in corpus - 145542475
select count(id) from new_tables_tokenized_full;

### Count number of union tables in corpus - 57753431
select count(distinct unionid) from union_tables_new;

### Count total number of columns - 870624784
select sum(count_per_table) from (select count(distinct colid) as count_per_table from main_tokenized group by tableid) as colcount;

### Count total number of columns in union tables - 454381162
select sum(count_per_table) from (select count(distinct colid) as count_per_table from main_tokenized inner join union_tables_new on main_tokenized.tableid=union_tables_new.tableid group by unionid) as colcount;

### Count total number of rows - 1518783939
select sum(numRows) from new_tables_tokenized_full;

### Count total numer of rows in union tables - 1518783939
select sum(rows) from (select avg(numRows_union) as rows from union_tables_new group by unionid) as t;

### Get Median Number of Rows in Tables - 4
select median(numRows) over() from new_tables_tokenized_full;

### Get Median Number of Rows in Uniontables - 4
select median(rows) over() from (select avg(numRows_union) as rows from union_tables_new group by unionid) as t;

### Avg Count Tables of UnionTable
select avg(c) from (select unionid,count(tableid) as c from union_tables_new group by unionid having co
unt(tableid) > 1) as t;

### Single Row Tables union with small or large Tables?
### Avg Rows for UnionTable tables
select unionid,avg(numRows) from tables_with_union where unionid in (select unionid from union_tables_new where tableid in (select id from new_tables_tokenized_full where numRows=1)) group by unionid having avg(numRows) > 1;