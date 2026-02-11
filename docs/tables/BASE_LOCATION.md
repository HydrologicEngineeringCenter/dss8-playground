# SQLDSS (HEC-DSS v8) #
### BASE_LOCATION ### 
```sql
create table base_location(
    key integer primary key,
    context text not null default ('') collate nocase, -- office, A pathname part, ...
    name text not null collate nocase);
    
-- column key auto-populates

create unique index idx_base_location on base_location (context, name);
```