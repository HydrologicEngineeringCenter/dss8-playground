# SQLDSS (HEC-DSS v8) #
### TSV_INFO ### 
```sql
create table tsv_info(
    time_series integer,
    block_start_date integer,       -- encoded -?\d+\d{2}\d{2} for extended dates
    value_count integer not null,
    first_time integer not null,    -- encoded -?\d+\d{2}\d{2} d{2}:d{2}:d{2} for extended dates
    last_time integer not null,     -- encoded -?\d+\d{2}\d{2} d{2}:d{2}:d{2} for extended dates
    min_value real,
    max_value real,
    last_update integer not null,   -- Unix epoch millisecionds
    primary key (time_series, block_start_date),
    foreign key (time_series) references time_series (key));
```