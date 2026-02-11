# SQLDSS (HEC-DSS v8) #
### BASE_PARAMETER ### 
```sql
create table base_parameter(
    name text collate nocase primary key,
    abstract_parameter_key integer not null,
    default_si_unit text not null,
    default_en_unit text not null,
    long_name text,
    foreign key (abstract_parameter_key) references abstract_parameter (key),
    foreign key (default_si_unit) references unit (name),
    foreign key (default_en_unit) references unit (name))

insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('%', 19, '%, '%', 'Percent');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Area', 3, 'm2, 'ft2', 'Surface Area');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Dir', 1, 'deg, 'deg', 'Direction');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Code', 19, 'n/a, 'n/a', 'Coded Information');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Conc', 18, 'mg/l, 'ppm', 'Concentration');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Cond', 6, 'umho/cm, 'umho/cm', 'Conductivity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Count', 7, 'unit, 'unit', 'Count');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Currency', 8, '$, '$', 'Currency');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Depth', 16, 'mm, 'in', 'Depth');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Elev', 16, 'm, 'ft', 'Elevation');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Energy', 11, 'MWh, 'MWh', 'Energy');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Evap', 16, 'mm, 'in', 'Evaporation');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('EvapRate', 17, 'mm/day, 'in/day', 'Evaporation Rate');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Flow', 26, 'cms, 'cfs', 'Flow Rate');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Frost', 16, 'cm, 'in', 'Ground Frost');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Opening', 16, 'm, 'ft', 'Opening Height');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('pH', 13, 'su, 'su', 'pH');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Power', 21, 'MW, 'MW', 'Power');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Precip', 16, 'mm, 'in', 'Precipitation');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Pres', 22, 'kPa, 'in-hg', 'Pressure');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Rad', 15, 'J/m2, 'langley', 'Irradiation');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Speed', 17, 'kph, 'mph', 'Speed');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Stage', 16, 'm, 'ft', 'Stage');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Stor', 25, 'm3, 'ac-ft', 'Storage');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Temp', 23, 'C, 'F', 'Temperature');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Thick', 16, 'cm, 'in', 'Thickness');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Timing', 9, 'sec, 'sec', 'Timing');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Turb', 24, 'JTU, 'JTU', 'Turbidity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Volt', 10, 'volt, 'volt', 'Voltage');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Travel', 16, 'km, 'mi', 'Accumulated Travel');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('SpinRate', 2, 'rpm, 'rpm', 'Spin Rate');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Irrad', 14, 'W/m2, 'langley/min', 'Irradiance');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('TurbJ', 24, 'JTU, 'JTU', 'Turbidity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('TurbN', 24, 'NTU, 'NTU', 'Turbidity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Fish', 7, 'unit, 'unit', 'Fish Count');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Dist', 16, 'km, 'mi', 'Distance');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Ratio', 19, 'n/a, 'n/a', 'Ratio');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('TurbF', 24, 'FNU, 'FNU', 'Turbidity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Volume', 25, 'm3, 'ft3', 'Volume');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Height', 16, 'm, 'ft', 'Height');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Rotation', 1, 'deg, 'deg', 'Rotation');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Length', 16, 'm, 'ft', 'Length');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Width', 16, 'm, 'ft', 'Width');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Coeff', 19, 'n/a, 'n/a', 'Coefficient');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Head', 16, 'm, 'ft', 'Head');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Current', 27, 'ampere, 'ampere', 'Current');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Freq', 28, 'Hz, 'Hz', 'Frequency');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Probability', 19, 'n/a, 'n/a', 'Probability');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('DepthVelocity', 35, 'm2/s, 'ft2/s', 'Depth Velocity');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Text', 19, 'n/a, 'n/a', 'Text Data');
insert into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name) values ('Binary', 19, 'n/a, 'n/a', 'Binary Data');
```
