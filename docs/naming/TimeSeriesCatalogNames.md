# SQLDSS (HEC-DSS v8) #
## Time Series Catalog Names ##
Like HEC-DSS v7, SQLDSS can generate condensed and uncondensed catalogs of time series <sup>*</sup>.
Where HEC-DSS v7 puts the block start date or the time series extents in the D pathname parts, SQLDSS appends this
information after the time series name, delimited by a pipe `|` character.

| System     | Un-condensed Example                                               | Condensed Example                                                                         |
|------------|--------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| SQLDSS     | `SWT:Greensburg\|Elev-Pool\|INST-VAL\|1Hour\|0\|Raw-Dcp\|20250601` | `SWT:Greensburg\|Elev-Pool\|INST-VAL\|1Hour\|0\|Raw-Dcp\|20250612120000 - 20250930180000` |
| HEC-DSS v7 | `/SWT/Greensburg/Elev-Pool/01Jun2025/1Hour/Raw-Dcp/`               | `/SWT/Greensburg/Elev-Pool/12Jun2025 - 30Sep2025/1Hour/Raw-Dcp/`                          |
