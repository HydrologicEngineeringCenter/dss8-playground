# SQLDSS (HEC-DSS v8) #
## Record Types ##

SQLDSS uses a subset of HEC-DSS v7 record types. It doesn't include any record type that indicates the values are 32-bit
floating point (floats) vs 64-bit floating point (doubles); It also adds two names for record type numbers that have no
name in HEC-DSS v7:

The Java code has these values in an Enum; they are not in the database.
                 
| Name             | Value | Description                                   |
|------------------|-------|-----------------------------------------------|
| ARR              | 90    | Array                                         |
| RTD              | 105   | Regular-interval time series doubles          |
| RTTD<sup>*</sup> | 106   | Regular-interval time series double pattern   |
| RTPD             | 107   | Regular-interval time series double profile   |
| ITD              | 115   | Irregular-interval time series doubles        |
| ITTD<sup>*</sup> | 116   | Irregular-interval time series double pattern |
| ITPD             | 117   | Irregular-interval time series double profile |
| PDD              | 205   | Paired Data doubles                           |
| TXT              | 300   | Text Data                                     |
| TT               | 310   | Text Table                                    |
| GUT              | 400   | Gridded - Undefined grib with time            |
| GU               | 401   | Gridded - Undefined grid                      |
| GHT              | 410   | Gridded - HRAP grid with time reference       |
| GH               | 411   | Gridded - HRAP grid                           |
| GAT              | 420   | Gridded - Albers with time reference          |
| GA               | 421   | Gridded - Albers                              |
| GST              | 430   | Gridded - Specified Grid with time reference  |
| GS               | 431   | Gridded - Specified Grid                      |
| TIN              | 450   | Spatial - TIN                                 |
| FILE             | 600   | Generic File                                  |
| IMAGE            | 610   | Image                                         |

<sup>*</sup> Name not in HEC-DSS v7