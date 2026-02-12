# SQLDSS (HEC-DSS v8) #
## Core vs API Layers ##
When reimplementing something like HEC-DSS, attention must be paid to backward compatibility as well as improvement.
While the core of SQLDSS (even above the SQLite engine) is different, API translation layers can make it easier to
use the new code with existing software without major re-writes. The Java implementation of SQLDSS provides the
class `mil.army.usace.hec.sqldss.api.dss7.HecDss` which provides essentially the same API as `hec.heclib.dss.HecDss`
class. Likewise, a `HecTimeSeries` class is also planned.

Other API layers could make it possible to use a CWMS database API or a URL API (à la CDA) without change to the core.
