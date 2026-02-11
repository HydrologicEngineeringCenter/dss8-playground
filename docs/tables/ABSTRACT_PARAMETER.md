# SQLDSS (HEC-DSS v8) #
### ABSTRACT_PARAMETER ###
```sql
create table abstract_parameter(
    key integer primary key,
    name text unique collate nocase);
    
-- column key auto-populates
    
insert into abstract_parameter (name) values ('Angle');
insert into abstract_parameter (name) values ('Angular Speed');
insert into abstract_parameter (name) values ('Area');
insert into abstract_parameter (name) values ('Areal Volume Rate');
insert into abstract_parameter (name) values ('Conductance');
insert into abstract_parameter (name) values ('Conductivity');
insert into abstract_parameter (name) values ('Count');
insert into abstract_parameter (name) values ('Currency');
insert into abstract_parameter (name) values ('Elapsed Time');
insert into abstract_parameter (name) values ('Electromotive Potential');
insert into abstract_parameter (name) values ('Energy');
insert into abstract_parameter (name) values ('Force');
insert into abstract_parameter (name) values ('Hydrogen Ion Concentration Index');
insert into abstract_parameter (name) values ('Irradiance');
insert into abstract_parameter (name) values ('Irradiation');
insert into abstract_parameter (name) values ('Length');
insert into abstract_parameter (name) values ('Linear Speed');
insert into abstract_parameter (name) values ('Mass Concentration');
insert into abstract_parameter (name) values ('None');
insert into abstract_parameter (name) values ('Phase Change Rate Index');
insert into abstract_parameter (name) values ('Power');
insert into abstract_parameter (name) values ('Pressure');
insert into abstract_parameter (name) values ('Temperature');
insert into abstract_parameter (name) values ('Turbidity');
insert into abstract_parameter (name) values ('Volume');
insert into abstract_parameter (name) values ('Volume Rate');
insert into abstract_parameter (name) values ('Electric Charge Rate');
insert into abstract_parameter (name) values ('Frequency');
insert into abstract_parameter (name) values ('Currency Per Volume');
insert into abstract_parameter (name) values ('Quantity Per Length');
insert into abstract_parameter (name) values ('Temerature Index');
insert into abstract_parameter (name) values ('Mass');
insert into abstract_parameter (name) values ('Mass Per Volume');
insert into abstract_parameter (name) values ('Mass Rate');
insert into abstract_parameter (name) values ('Depth Velocity');
```