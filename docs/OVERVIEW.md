# SQLDSS (HEC-DSS v8) #
## Overview ##
SQLDSS (aka HEC-DSS v8) is a prototype implementation of the HEC Data Storage System built
on a foundation of SQLite.

**Goal**

To build a successor to HEC-DSS v7 which:
- is easily maintainable
    - uses an industry standard, well maintained data storage engine
    - requires minimal layers of software to utilize
- is easily extensible
    - new data types
    - new ways to store existing data types
- is accessible from multiple programming languages
- compares favorably to HEC-DSS v7 in performance
- provides a low barrier to transition from HEC-DSS v7

In this prototype the internally developed data HEC-DSS storage engine is replaced with the externally maintained and
widely used SQLite file format and access libraries, which are available for most widely-used programming languages. This
prototype implementation is in Java.

**File Names**

Like its SQLite foundation, SQLDSS enforces no file naming conventions. SQLDSS files are free to - but not required to -
use the `.dss` file name extension, although this may confuse users about which files are HEC-DSS v7 and which are
SQLDSS.

**Data Definition in SQL**

To the extent practical, all data is stored as columns in relational tables and is may be accessed via
SQL, whether via either programming language-specific library or the `sqlite3` command line utility. However,
for performance reasons some data is stored in BLOB columns and requires custom packing/unpacking, making
such data opaque to `sqlite3`.<sup>*</sup> The table structure is inspired by the CWMS database structure, although the
SQLDSS version is simplified. See the [table structure](tables/TABLE_STRUCTURE.md) document for details of
the relational tables.

<sup>*</sup> SQLite extensions could be created such that loading them in a CLI session would allow meaningful display
of data in BLOB columns.

**Units and Time Zones**

Like the CWMS database, SQLDSS defines storage units for each base parameter, which eliminates specifying units for
data records. Instead, values must be converted to the storage unit for their parameter when storing, and converted to
requested units (if any) when retrieving. Currently, like CWMS, the storage units are the `default_si_unit` from the
[base parameter](tables/BASE_PARAMETER.md) table, although it would be simple to allow users to choose to use the
`default_en_unit` instead when creating SQLDSS files.

In the interest of not tying unit conversions to any specific programming language library, unit conversions between
all supported compatible units are stored in the [unit conversion](tables/UNIT_CONVERSION.md) table. The conversions
specify factors and offsets for linear conversions and RPN functions for non-linear conversions. Linear conversions are
trivial to implement in any programming language, and RPN functions are simple, if not trivial, to implement.

Also like the CWMS database, all time values are stored in the database without time zones; they are expected to be either
time zone naïve or in UTC. Time zone naïve data may be stored and retrieved without time zone conversion, while data
with time zones must be convert to UTC on storage and from UTC on retrieval.

**Java Implementation**

The java implementation uses static classes/methods as much as possible to minimize the overhead of class construction
and destruction.

**Current State**

- The only data type currently supported is regular time series, although adding irregular time series should
not be complicated.
- Time series currently supports only the HEC-DSS v6 fields/methods. HEC-DSS v7 features such as extended quality codes,
integer notes, and character notes are not currently supported.

## [Performance Comparisons](PerformanceComparisons.md) ##
## [SQLite File & Connection Settings](SqliteFile+ConnectionSettings.md) ##
## [Dates & Times](Dates+Times.md) ##
## [Core vs API Layers](CoreVsApiLayers.md) ##
## Data Names ##
SQLDSS doesn't adhere to the conventional HEC-DSS pathname structure, as in the CWMS database, and different data types
are free to have different naming conventions. If an API layer is used, it is responsible for translation between the
API-centric names and the core SQLDSS names.

* [Locations](naming/LocationNames.md)
* [Parameters](naming/ParameterNames.md)
* [Parameter Types](naming/ParameterTypeNames.md)
* [Intervals](naming/IntervalNames.md)
* [Durations](naming/DurationNames.md)
* [Time Series](naming/TimeSeriesNames.md)
* [Time Series Catalogs](naming/TimeSeriesCatalogNames.md)

## Data Storage ##
* [Locations](storage/LocationStorage.md)
* [Time Series](storage/TimeSeriesStorage.md)





