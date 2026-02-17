# SQLDSS (HEC-DSS v8) #
## Time Series Catalog Names ##
Like HEC-DSS v7, SQLDSS can generate condensed and uncondensed catalogs of time series <sup>*</sup>.
Where HEC-DSS v7 puts the block start date or the time series extents in the D pathname parts, SQLDSS appends this
information after the time series name, delimited by a pipe `|` character.

| System     | Un-condensed Example                                               | Condensed Example                                                                         |
|------------|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| SQLDSS     | `SWT:Olive\|Flow\|INST-VAL\|1Hour\|0\|Obs\|20250601` | `SWT:Olive\|Flow\|INST-VAL\|1Hour\|0\|Obs\|20250612120000 - 20250930180000` |
| HEC-DSS v7 | `/SWT/Olive/Flow/01Jun2025/1Hour/Obs/`               | `/SWT/Olive/Flow/12Jun2025 - 30Sep2025/1Hour/Obs/`                          |

<sup>*</sup> See the [time series storage](../storage/TimeSeriesStorage.md) document for details on block storage of time series.