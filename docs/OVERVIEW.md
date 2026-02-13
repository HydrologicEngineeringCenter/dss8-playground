# SQLDSS (HEC-DSS v8) #
## Overview ##
SQLDSS (aka HEC-DSS v8) is a new implementation of the HEC Data Storage System built
on a foundation of SQLite. In this implementation the internally developed data storage engine (data formats,
locations, and access methods) is replaced with the externally maintained and widely used SQLite file format
and access libraries.

To the extent practical, all data is stored as columns in relational tables and is may be accessed via
SQL, whether via either programming language-specific library or the `sqlite3` command line utility. However,
for performance reasons some data is stored in BLOB columns and requires custom packing/unpacking, making
such data opaque to `sqlite3`. The table structure is inspired by the CWMS database structure, although the
SQLDSS version is simplified. See the [TABLE_STRUCTURE](tables/TABLE_STRUCTURE.md) document for details of
the relational tables.

Like the CWMS database, SQLDSS defines storage units for each base parameter, which eliminates specifying units for
data records. Instead, values must be converted to the storage unit for their parameter when storing, and converted to
requested units (if any) when retrieving. Currently, like CWMS, the storage units are the `default_si_unit` from the
[base parameter](tables/BASE_PARAMETER.md) table, although it would be simple to allow users to choose to use the
`default_en_unit` instead when creating SQLDSS files.

Also like the CWMS database, all time values stored in the database are UTC times. Value times must be converted to UTC
when storing and converted to the requested time zone (if any) when retrieving.

The java implementation uses static classes/methods as much as possible to minimize the overhead of class construction
and destruction.

The only data type currently supported is regular time series, although adding irregular time series should
not be complicated.

## [SQLite File & Connection Settings](SqliteFile+ConnectionSettings.md) ##
## [Dates & Times](Dates+Times.md) ##
## [Core vs API Layers](CoreVsApiLayers.md) ##
## [Data Names](DataNames.md) ##
## [Time Series Storage](TimeSeriesStorage.md) ##



