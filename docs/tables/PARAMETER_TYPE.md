# SQLDSS (HEC-DSS v8) #
### PARAMETER_TYPE ### 
```sql
create table parameter_type(
    name text collate nocase primary key);
    
insert into parameter_type (name) values ('CONST');
insert into parameter_type (name) values ('INST-CUM');
insert into parameter_type (name) values ('INST-VAL');
insert into parameter_type (name) values ('PER-AVER');
insert into parameter_type (name) values ('PER-CUM');
insert into parameter_type (name) values ('PER-MAX');
insert into parameter_type (name) values ('PER-MIN');
```
