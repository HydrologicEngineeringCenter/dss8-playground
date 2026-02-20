# SQLDSS (HEC-DSS v8) #
## Handling BLOB Columns ##

Although SQLite handles accessing most columns on systems with different endianness, BLOBs are susceptible to endian
differences. SQLDSS stores BLOBs in little endian format regardless of system architecture - _this must be enforced in
whatever programming language is used to read and write the BLOBs_.
