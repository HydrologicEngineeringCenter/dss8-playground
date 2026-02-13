# SQLDSS (HEC-DSS v8) #
## SQLite File & Connection Settings ##

### File Settings ###

**Page Size:**

In preliminary tests with regular time series, a page size of 8192 bytes seems optimal unless
file size is prioritied over read/write performance.

File size grows with page size, with a page size of 512 bytes
resulting in the same file size as an HEC-DSS v7 file, and a page size of 65536 resulting in a file about 50%
larger than an HEC-DSS v7 file.
 
A page size of 8192 results in the greatest performance, with performance dropping both below and above this value.
The resulting file is about 40% larger than an HEC-DSS v7 file.

The following statement must be executed before any tables are created.
```sql
pragma page_size = 8192;
``` 


### Connection Settings ###

**Foreign Keys**

By default, SQLite allows foreign key definitions, but doesn't enforce them. The following statement must be executed
before inserts or updates to user tables to ensure referential integrity.
```sql
pragma foreign_keys = ON;
``` 

**Auto Commit**

By default SQLite has auto-commit enabled. When inserting or updating a large number of records this becomes a performance
problem. Performance can be greatly increased by turning off auto-commit for such activity and performing `commit;`
statements at reasonable intervals and after the inserting/updating is finished, as in the follwoing Java example, in 
which the SqlDss object has:
* a `setAutoCommit(boolean state)` method that sets the auto-commit state of the underlying SQLite file
* a `commit()` method that performs a commit on the underlying SQLite file
```java
    SqlDss sqldss = SqlDss.open(sqlDssFileName);
    sqldss.setAutoCommit(false);
    for (int i = 0; i < tscs.length; ++i) {
        if (i > 0 && i % 100 == 0) {
            sqldss.commit();
        }
        sqldss.putTimeSeriesValues(tscs[i], "REPLACE_ALL");
    }
    sqldss.commit();
    sqldss.close();
```