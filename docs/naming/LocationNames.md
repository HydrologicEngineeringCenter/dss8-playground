# SQLDSS (HEC-DSS v8) #
## Location Names ##
A location name is one piece of text, matched without regard to upper/lower case. An API layer can put
whatever structure it wants in that text. DSS7 and CWMS use this form:

> *context*`:`*location*`-`*sub-location*

The context and sub-location are optional. The two API layers map the pieces differently:

| Piece                         | HEC-DSS v7      | CWMS     |
|-------------------------------|-----------------|----------|
| *context*                     | A pathname part | office   |
| *location*`-`*sub-location*   | B pathname part | location |

For example:

| SQLDSS name           | HEC-DSS v7 pathname            | CWMS office | CWMS location     |
|-----------------------|-------------------------------|-------------|-------------------|
| `Greensburg`          | `//Greensburg/…/…/…/…/`        | *(none)*    | `Greensburg`      |
| `Greensburg-Pool`     | `//Greensburg-Pool/…/…/…/…/`   | *(none)*    | `Greensburg-Pool` |
| `SWT:Greensburg-Pool` | `/SWT/Greensburg-Pool/…/…/…/…/`| `SWT`       | `Greensburg-Pool` |

(The `…` parts are the C-F pathname parts, filled from the rest of the time series name.)

For this form to parse cleanly, a context may not contain `:` or `-`, and a location may not contain `-`.
