# ToDo

## `|` delimiter vs. legacy DSS F-part

SQLDSS uses `|` to delimit the six name parts, but legacy DSS F-parts (version) may
contain `|`  -- see `TimeSeries.java`.

**Consider:** escape `|` per part, use the legacy `/` as delimiter, or reject piped parts at the v7->v8 mapping boundary. 


## base_parameter naming

**consider:** use 'quantity' instead of base_parameter. https://jcp.org/en/jsr/detail?id=363 