# SQLDSS (HEC-DSS v8) #
## Location Storage ##

As indicated in the [location](../tables/LOCATION.md) table document, each location row includes:
* a reference to the base location
* a (possibly empty) sub-location name
* a (possibly empty) info column

The `info` field, if not empty, is expected to be a valid JSON string, and is envisioned to take the place of the
supplemental info (user header) of location records in HEC-DSS v7. In the event that it becomes desirable to filter
locations on some of the information that might be included in the JSON string, the table structure can easily be modified
to include the necessary filter fields (with indexes as appropriate).
