# SQLDSS (HEC-DSS v8) #
## Data Names ##
SQLDSS doesn't adhere to the conventional HEC-DSS pathname structure, as in the CWMS database, and different data types
are free to have different naming conventions. If an API layer is used, it is responsible for translation between the
API-centric names and the core SQLDSS names.

### Location Names ###
Like CWMS, SQLDSS uses base locations and sub-locations, separated by the first hyphen `-`character. Base locations
comprise an optional context, which appears before the base-location name and is separated by a colon `:` character.
* *context*`:`*base_location*`-`*sub_location*
* *context*`:`*base_location*
* *base_location*
* *base_location*`-`*sub_location*

The only constraints on location names are:
* Neither of `:`, `-` may appear in a context.
* `-` may not appear in a base location

SQLDSS imposes no meaning on the context. The DSS7 API layer maps it to the A pathname part, while a CWMS API layer
might map it to an office.

### Parameter Names ###
Like CWMS, SQLDSS uses base locations and sub-locations, separated by the first hyphen `-`character. Base parameters
are constrained by the [BASE_PARAMETER](tables/BASE_PARAMETER.md) table; sub-parameters are unconstrained.

### Parameter Type Names ###
SQLDSS parameter types are a superset of HEC-DSS v7 data types. See [parameter types](tables/PARAMETER_TYPE.md) document.

### Interval Names ###
SQLDSS uses the same interval names as CWMS. See [intervals](tables/INTERVAL.md) document.

### Duration Names ###
SQLDSS uses the same duration names as CWMS. See [durations](tables/DURATION.md) document.

### Time Series Names ###
Like HEC-DSS v7 and CWMS, SQLDSS uses six parts to identify a time series; It uses the same six parts as
CWMS:
* Location
* Parameter
* Parameter Type
* Interval
* Duration
* Version

SQLDSS uses the pipe `|` character to delimit portions of the time series name. Like CWMS and unlike
HEC-DSS v7, the delimiters are internal only (i.e., the names do not begin or end with the delimiter).

| System     | Example Time Series Name                                                     |
|------------|------------------------------------------------------------------------------|
| SQLDSS     | `SWT:Greensburg\|Elev-Pool\|INST-VAL\|1Hour\|0\|Raw-Dcp`                     |
| CWMS       | `Greensburg.Elev-Pool.INST-VAL.1Hour.0.Raw-Dcp` (office=SWT)                 |
| HEC-DSS v7 | `/SWT/Greensburg/Elev-Pool//1Hour/Raw-Dcp/` (data type=INST-VAL<sup>*</sup>) |

<sup>*</sup> The HEC-DSS v7 API mapping sets the data type on storing. On retrieving, API mapping tries
each of the possible parameter types in sequence and the first time series found is retrieved. HEC-DSS v7
doesn't support multiple data types for the same record.

**Constraints**:
* The base parameter, parameter type, interval, and duration must all exist in the database
* Neither of the location context (if present) or location name may include the colon `:` character
* None of the parts may contain the pipe `|` character.

### Time Series Catalog Names ###
Like HEC-DSS v7, SQLDSS can generate condensed and uncondensed catalogs of time series <sup>*</sup>.
Where HEC-DSS v7 puts the block start date or the time series extents in the D pathname parts, SQLDSS appends this
information after the time series name, delimited by a pipe `|` character.

| System     | Un-condensed Example                                               | Condensed Example                                                                         |
|------------|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| SQLDSS     | `SWT:Greensburg\|Elev-Pool\|INST-VAL\|1Hour\|0\|Raw-Dcp\|20250601` | `SWT:Greensburg\|Elev-Pool\|INST-VAL\|1Hour\|0\|Raw-Dcp\|20250612120000 - 20250930180000` |
| HEC-DSS v7 | `/SWT/Greensburg/Elev-Pool/01Jun2025/1Hour/Raw-Dcp/`               | `/SWT/Greensburg/Elev-Pool/12Jun2025 - 30Sep2025/1Hour/Raw-Dcp/`                          |

<sup>*</sup> See the [time series storage](TimeSeriesStorage.md) document for details on block storage of time series.