# SQLDSS (HEC-DSS v8) #
### TSV ### 
```sql
create table tsv(
    time_series integer,
    block_start_date integer, -- encoded -?\d+\d{2}\d{2} for extended dates
    deleted integer not null default (0),
    data blob,
    primary key (time_series, block_start_date),
    foreign key (time_series) references time_series (key));
```