# SQLDSS (HEC-DSS v8) #
## Location Names ##
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