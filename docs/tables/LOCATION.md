# SQLDSS (HEC-DSS v8) #
### LOCATION ### 
```sql
create table location(
    key integer primary key,
    name text not null collate nocase, -- entire location identifier; can't be empty
    info text default (''));           -- JSON object

-- column key auto-populates

create unique index idx_location on location (name);
```

`name` is the full location identifier, stored as given and matched without regard to upper/lower case; see
[Location Names](../naming/LocationNames.md).

The `info` field, if not empty, is expected to be a valid JSON string. It takes the place of the supplemental
info (user header) of location records in HEC-DSS v7. Should it become useful to filter locations on values
within the JSON, the table can be given dedicated columns (and indexes) for those values.
