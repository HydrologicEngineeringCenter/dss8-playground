# SQLDSS (HEC-DSS v8) #
## Time Series Names ##
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

<sup>*</sup> See the [time series storage](../storage/TimeSeriesStorage.md) document for details on block storage of time series.