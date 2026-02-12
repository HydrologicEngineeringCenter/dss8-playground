# SQLDSS (HEC-DSS v8) #
## Table Structure ##
### Reference Tables <sup>1</sup> ###
[DSS_INFO](DSS_INFO.md)  Info about the DSS file (currently just version number)  
[ABSTRACT_PARAMETER](ABSTRACT_PARAMETER.md)  Universe of available abstract parameters <sup>2</sup>    
[UNIT](UNIT.md)  Universe of available units  
[UNIT_ALIAS](UNIT_ALIAS.md)  Universe of available aliases for units  
[UNIT_CONVERSION](UNIT_CONVERSION.md)  Conversions between all compatible units <sup>3</sup>  
[PARAMETER_TYPE](PARAMETER_TYPE.md)  Universe of available parameter types <sup>4</sup>  
[INTERVAL](INTERVAL.md)  Universe of available value recurrence intervals  
[DURATION](DURATION.md)  Universe of available value durations  
[BASE_PARAMETER](BASE_PARAMETER.md) Universe of available base parameters  
### User Tables <sup>5</sup> ###
[PARAMETER](PARAMETER.md)  Parameters represented by values  
[BASE_LOCATION](BASE_LOCATION.md)  Base locations with optional context  
[LOCATION](LOCATION.md)  Locations within base locations  
[TIMESERIES](TABLE_STRUCTURE.md)  Time series specifications  
[TSV](TSV.md)  Time series value blocks  
[TSV_INFO](TSV_INFO.md)  Stats for time series value blocks

<sup>1</sup> These tables are created and populated when a new SQLDSS file is created. Although they define the universe
of available parameters, units, etc..., they can be modified to support custom items. We could specify a directory from
which local additions to the reference tables are kept via environment variable or Java property - much like the
`parameters_units.def` and `unitConversions.def` files in current Java programs.

<sup>2</sup> Abstract parameters are "baser" than base parameters. Base parameters of `Height`, `Depth`, `Dist`, `Elev`,
and `Stage` all have the abstract parameter of `Length`. Units are also tied to abstract parameters, so different base
parameters with the same abstract parameter support the same group of units.

<sup>3</sup> That is, all units of a single abstract parameter. Linear unit conversions are specified by the `factor` and
`offset` in the equation `value_in_target_unit = value_in_source_unit * factor + offset`. Non-linear conversions are
specified by a formula applied to the source-unit value. The formula is specified in RPN format with `ARG1` indicating
the source-unit value and all formula tokens delimited by whitespace. The formula for squaring a value and adding 100.0
would be `ARG1 2 ^ 100 +`.

<sup>4</sup> SQLDSS uses a superset of HEC-DSS v7 data types as parameter types.

<sup>5</sup> These table are created when a new SQLDSS file is created.