# SQLDSS (HEC-DSS v8) #
### PARAMETER ### 
```sql
create table parameter(
    key integer primary key,
    base_parameter text not null collate nocase,
    sub_parameter text default ('') collate nocase,
    foreign key (base_parameter) references base_parameter (name));
    
-- column key auto-populates

create unique index idx_parameter on parameter (base_parameter, sub_parameter);
```