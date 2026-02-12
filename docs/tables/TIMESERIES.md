# SQLDSS (HEC-DSS v8) #
### TIMESERIES ### 
```sql
create table time_series(
    key integer primary key,
    deleted integer not null default (0),
    location integer not null,
    parameter integer not null,
    parameter_type text not null,
    interval text not null,
    duration text not null,
    version text default ('') collate nocase,
    interval_offset text default (''), -- ISO 8601 (e.g., PT15M)
    foreign key (location) references location (key),
    foreign key (parameter) references parameter (key),
    foreign key (parameter_type) references parameter_type (name),
    foreign key (interval) references interval (name),
    foreign key (duration) references duration (name));
    
-- column key auto-populates

create unique index idx_time_series on time_series (location, parameter, parameter_type, interval, duration, version);
```
