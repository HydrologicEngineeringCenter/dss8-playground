# SQLDSS (HEC-DSS v8) #
### LOCATION ### 
```sql
create table location(
    key integer primary key,
    base_location integer,
    sub_location text default ('') collate nocase,
    info text default (''), -- JSON object
    foreign key (base_location) references base_location (key));
    
-- column key auto-populates

create unique index idx_location on location (base_location, sub_location);
```