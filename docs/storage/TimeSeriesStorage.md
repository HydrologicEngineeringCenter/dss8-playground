# SQLDSS (HEC-DSS v8) #
## Time Series Storage ##

## Content ##

Currently, SQLDSS stores the same time series information as HEC-DSS v6: times, values, and 32-bit quality codes. It does
not store any of the HEC-DSS v7 additions: extended quality codes, integer-based notes, or character-based notes.

## Format ##

Like HEC-DSS v7, SQLDSS stores time series values in blocks<sup>1</sup>. The block size for each interval is specified in the
[interval](../tables/INTERVAL.md) table. Currently the block sizes are the same as for HEC-DSS v7. To investigate the 
changes with using other block sizes, simply update the data before opening a new SQLDSS file.

Also like HEC-DSS v7 the SQLDSS block start dates are the first day in the block size for a specified value time.

| Interval | Block Size | Value Time     | Block Start Date |
|----------|------------|----------------|------------------|
| 1Minute  | 1Day       | 12Aug2025 0700 | 12Aug2025        |   
| 1Hour    | 1Month     | 12Aug2025 0700 | 01Aug2025        |   
| 1Day     | 1Year      | 12Feb2025 0700 | 01Jan2025        |   

Although SQLDSS and HEC-DSS v7 use the same block sizes, they differ in the first and last value times that can be
stored to each block. HEC-DSS v7 always allows a value at 24:00 on the last day of the block to be stored as the last
value of the block, requiring the first possible value to be the _first value after 00:00_ on the first day of the block.
Conversely, SQLDSS always allows a value at 00:00 on the first day of the block to be stored in the block, requiring
the last possible value to be the _last value before 24:00_ on the last day of the block.

| System     | Interval | Block Size | Block Start Date | First Possible Time | Last Possible Time |
|------------|----------|------------|------------------|---------------------|--------------------|
| HEC-DSS v7 | 1Minute  | 1Day       | 12Aug2025        | 12Aug2025 00:01     | 12Aug2025 24:00    |  
| SQLDSS     | 1Minute  | 1Day       | 12Aug2025        | 12Aug2025 00:00     | 12Aug2025 23:59    |  
| HEC-DSS v7 | 1Hour    | 1Month     | 01Aug2025        | 01Aug2025 00:01     | 30Aug2025 24:00    |
| SQLDSS     | 1Hour    | 1Month     | 01Aug2025        | 01Aug2025 00:00     | 30Aug2025 23:59    |
| HEC-DSS v7 | 1Day     | 1Year      | 01Jan2025        | 01Jan2025 00:01     | 31Dec2025 24:00    |
| SQLDSS     | 1Day     | 1Year      | 01Jan2025        | 01Jan2025 00:00     | 31Dec2025 23:59    |

When using the HEC-DSS v7 API layer to retrieve data using a pathname that includes a D part, and without a time window,
the code should set the retrieve time window according to HEC-DSS v7 block conventions and not just retrieve a single
SQLDSS block.

As seen in the [tsv table](../tables/TSV.md) document, each block contains:
* a reference to the time series
* a block start date (see [dates & times](../Dates+Times.md) document for details)
* a deleted flag
* a BLOB<sup>2</sup> that contains the block information

<sup>1</sup> Like in previous versions of HEC-DSS, the terms `block` and `record` are somewhat interchangeable in the context of time
series. The term `record` is used more in context of time series catalogs. The term `block` is used more in the context of
time series storage, where data for a single time series name ("pathname" in previous versions of HEC-DSS) is stored in
multiple blocks, each of which holds a specific time window of the entire data set.

<sup>2</sup> See the [Handling BLOB Columns](../BlobColumns.md) document for details

### Time Series Block Formats ###
**Regular Time Series**


The Regular time series blocks are broken into header and body portions:

_Header_
* Record Type (int-8): must be 105 (see [record types](../RecordTypes.md) document)
* Version (int-8): must be 1 - other versions may be created in the future
* ValueCount (int-32): specifies number of values in block
* QualityFLag (int-8): 0 or 1 - specifies if the block contains quality codes
* FirstValueTime (int-64) - date time of first value in block (see [dates & times](../Dates+Times.md) document for details)

_Body_
* Values (ValueCount * float-64): the values for the block
* QualityCodes (ValueCount * int-32, only if QualityFlag == 1): the quality codes for the values

When storing:
* If the `interval_offset` value in the [timeseries](../tables/TIMESERIES.md) table _is not set_, it is computed and set from
the FirstValueTime and the `interval` value in the [timeseries](../tables/TIMESERIES.md) table
* If the `interval_offset` value in the [timeseries](../tables/TIMESERIES.md) table _is set_ and does not equal the one 
computed from the FirstValueTime and the `interval` value in the [timeseries](../tables/TIMESERIES.md) table, an exception
must be thrown.
* If value times are not all exactly one interval apart, an exception must be thrown.
* If data has no quality codes or if all quality codes are 0, QualityFlag is set to 0 and no quality codes are stored.

When retrieving:
* An exception must be thrown if:
  * RecordType != 105
  * Version != 1
  * the interval offset computed from the FirstValueTime and the `interval` value in the [timeseries](../tables/TIMESERIES.md)
table does not equal the `interval_offset` value in the [timeseries](../tables/TIMESERIES.md) table.
* Times for each value are computed from FirstValueTime and the `interval` value in the [timeseries](../tables/TIMESERIES.md)
table for the referenced time series
* If QualityFlag == 0, no quality codes are read and all values are assigned a quality code of 0.

**Irregular Time Series**

Irregular time series is not yet supported

### Store Rules ##
** Regular Time Series Store Rules **

| Name                        | Value | Note                                             |
|-----------------------------|-------|--------------------------------------------------|
| REPLACE_ALL                 | 0     |                                                  |
| REPLACE_MISSING_VALUES_ONLY | 1     |                                                  |
| REPLACE_ALL_CREATE          | 2     | Alias for REPLACE_ALL for backward compatibility |
| REPLACE_ALL_DELETE          | 3     | Alias for REPLACE_ALL for backward compatibility |
| REPLACE_WITH_NON_MISSING    | 4     |                                                  |
| DO_NOT_REPLACE              | 5     |                                                  |

** Irregular Time Series Store Rules **

| Name                        | Value | Note                                             |
|-----------------------------|-------|--------------------------------------------------|
| REPLACE_ALL                 | 0     |                                                  |
| MERGE                       | 0     | Alias for REPLACE_ALL for backward compatibility |
| DELETE_INSERT               | 1     |                                                  |
| REPLACE_MISSING_VALUES_ONLY | 2     |                                                  |
| REPLACE_WITH_NON_MISSING    | 3     |                                                  |
| DO_NOT_REPLACE              | 4     |                                                  |

It is the responsibility of the code storing time series over existing time series to merge the data according to the 
specified store rule for each block. Resulting empty blocks should be deleted