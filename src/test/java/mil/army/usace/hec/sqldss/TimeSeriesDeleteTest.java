package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import com.google.common.flogger.FluentLogger;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import mil.army.usace.hec.sqldss.core.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimeSeriesDeleteTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    SqlDss _db = null;

    SqlDss getDb() throws IOException, SqlDssException, SQLException, EncodedDateTimeException {
        Path dir = Paths.get("build/test-artifacts", getClass().getSimpleName());
        Files.createDirectories(dir);
        String dbFileName = dir.resolve("tester.sqldss").toString();
        if (_db == null) {
            Files.deleteIfExists(Path.of(dbFileName));
            logger.atInfo().log("Opening new SqlDss: %s", dbFileName);
            _db = SqlDss.open(dbFileName);
        }
        if (!_db.isOpen()) {
            _db = SqlDss.open(dbFileName);
        }
        return _db;
    }

    static TimeSeriesContainer @NotNull [] makeTimeSeriesContainers(int tscCount, int valueCount) throws Exception {
        TimeSeriesContainer[] tscs = new TimeSeriesContainer[tscCount];
        for (int i = 0; i < tscCount; ++i) {
            long startTime = EncodedDateTime.incrementEncodedDateTime(20250101000000L, 43200, i);
            String name = String.format("TestLoc|Code|INST-VAL|1Hour|0|Version %d", i);
            String[] parts = name.split("\\|", -1);
            tscs[i] = new TimeSeriesContainer();
            tscs[i].fullName = name;
            tscs[i].location = parts[0];
            tscs[i].parameter = parts[1];
            tscs[i].type = parts[2];
            tscs[i].units = "n/a";
            tscs[i].interval = Interval.getIntervalMinutes(parts[3]);
            tscs[i].version = parts[5];
            tscs[i].times = new int[valueCount];
            tscs[i].values = new double[valueCount];
            tscs[i].numberValues = valueCount;
            tscs[i].setStartTime(EncodedDateTime.toHecTime(startTime));
            tscs[i].times[0] = tscs[i].getStartTime().value();
            for (int j = 0; j < valueCount; ++j) {
                if (j > 0) {
                    tscs[i].times[j] = tscs[i].times[j-1] + tscs[i].interval;
                }
                tscs[i].values[j] = 10 * i + j;
            }
            HecTime endTime = tscs[i].getEndTime();
            endTime.set(tscs[i].times[valueCount-1]);
            tscs[i].setEndTime(endTime);
        }
        return tscs;
    }

    @Test
    public void testDeleteTimeSeries() throws Exception {
        TimeSeriesContainer[] tscs = makeTimeSeriesContainers(10, 1000);
        try (SqlDss db = getDb()) {
            //-------------------//
            // store all records //
            //-------------------//
            for (TimeSeriesContainer tsc : tscs) {
                TimeSeries.storeTimeSeriesValues(tsc, String.valueOf(Constants.REGULAR_STORE_RULE.REPLACE_ALL), db.getConnection());
            }
            String[] fullCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250101",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250201",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250201",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250301",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250301",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250401",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250401",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20251001",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251001",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251101"
            };
            String[] nonDeletedCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250101",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250201",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250301",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250401",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20251001"
            };
            String[] deletedCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250201",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250301",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250401",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251001",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251101"
            };
            //====================//
            // Full catalog tests //
            //====================//
            // normal (non-deleted) catalog
            String[] catalog = TimeSeries.catalogTimeSeries(null, false, "N", db.getConnection());
            assertEquals(fullCatalog.length, catalog.length);
            for (int i = 0; i < fullCatalog.length; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, false, "D", db.getConnection());
            assertEquals(0, catalog.length);
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, false, "ND", db.getConnection());
            assertEquals(fullCatalog.length, catalog.length);
            for (int i = 0; i < fullCatalog.length; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            //---------------------//
            // delete some records //
            //---------------------//
            TimeSeries.deleteTimeSeriesRecords(deletedCatalog, db.getConnection());
            // normal (non-deleted) catalog
            catalog = TimeSeries.catalogTimeSeries(null, false, "N", db.getConnection());
            assertEquals(nonDeletedCatalog.length, catalog.length);
            for (int i = 0; i < nonDeletedCatalog.length; ++i) {
                assertTrue(nonDeletedCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, false, "D", db.getConnection());
            assertEquals(deletedCatalog.length, catalog.length);
            for (int i = 0; i < deletedCatalog.length; ++i) {
                assertTrue(deletedCatalog[i].equals(catalog[i]));
            }
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, false, "ND", db.getConnection());
            assertEquals(fullCatalog.length, catalog.length);
            for (int i = 0; i < fullCatalog.length; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            //-----------------------//
            // undelete the records //
            //-----------------------//
            TimeSeries.undeleteTimeSeriesRecords(deletedCatalog, db.getConnection());
            // normal (non-deleted) catalog
            catalog = TimeSeries.catalogTimeSeries(null, false, "N", db.getConnection());
            assertEquals(fullCatalog.length, catalog.length);
            for (int i = 0; i < fullCatalog.length; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, false, "D", db.getConnection());
            assertEquals(0, catalog.length);
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, false, "ND", db.getConnection());
            assertEquals(fullCatalog.length, catalog.length);
            for (int i = 0; i < fullCatalog.length; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            //---------------------//
            // filter record names //
            //---------------------//
            catalog = TimeSeries.catalogTimeSeries(".+Version [0-3]", false, "N", db.getConnection());
            assertEquals(8, catalog.length);
            for (int i = 0; i < 8; ++i) {
                assertTrue(fullCatalog[i].equals(catalog[i]));
            }
            //=========================//
            // Condensed catalog tests //
            //=========================//
            String[] fullCondensedCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250101000000 - 20250211150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250201000000 - 20250314150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250301000000 - 20250411150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250401000000 - 20250512150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250501000000 - 20250611150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250601000000 - 20250712150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250701000000 - 20250811150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250801000000 - 20250911150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20250901000000 - 20251012150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251001000000 - 20251111150000"
            };
            String[] deletedRecords = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250501",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250601",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250701",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250801",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20250901",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20251001",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251001",
            };
            String[] nonDeletedCondensedCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 0|20250101000000 - 20250211150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 1|20250201000000 - 20250314150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 2|20250301000000 - 20250411150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250401000000 - 20250430230000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251101000000 - 20251111150000"
            };
            String[] deletedCondensedCatalog = new String[] {
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 3|20250501000000 - 20250512150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 4|20250501000000 - 20250611150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 5|20250601000000 - 20250712150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 6|20250701000000 - 20250811150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 7|20250801000000 - 20250911150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 8|20250901000000 - 20251012150000",
                    "TestLoc|Code|INST-VAL|1Hour|0|Version 9|20251001000000 - 20251031230000"
            };
            // normal (non-deleted) catalog
            catalog = TimeSeries.catalogTimeSeries(null, true, "N", db.getConnection());
            assertEquals(fullCondensedCatalog.length, catalog.length);
            for (int i = 0; i < fullCondensedCatalog.length; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, true, "D", db.getConnection());
            assertEquals(0, catalog.length);
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, true, "ND", db.getConnection());
            assertEquals(fullCondensedCatalog.length, catalog.length);
            for (int i = 0; i < fullCondensedCatalog.length; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
            //---------------------//
            // delete some records //
            //---------------------//
            TimeSeries.deleteTimeSeriesRecords(deletedRecords, db.getConnection());
            // normal (non-deleted) catalog
            catalog = TimeSeries.catalogTimeSeries(null, true, "N", db.getConnection());
            assertEquals(nonDeletedCondensedCatalog.length, catalog.length);
            for (int i = 0; i < nonDeletedCondensedCatalog.length; ++i) {
                assertTrue(nonDeletedCondensedCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, true, "D", db.getConnection());
            assertEquals(deletedCondensedCatalog.length, catalog.length);
            for (int i = 0; i < deletedCondensedCatalog.length; ++i) {
                assertTrue(deletedCondensedCatalog[i].equals(catalog[i]));
            }
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, true, "ND", db.getConnection());
            assertEquals(fullCondensedCatalog.length, catalog.length);
            for (int i = 0; i < fullCondensedCatalog.length; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
            //-----------------------//
            // undelete the records //
            //-----------------------//
            TimeSeries.undeleteTimeSeriesRecords(deletedRecords, db.getConnection());
            // normal (non-deleted) catalog
            catalog = TimeSeries.catalogTimeSeries(null, true, "N", db.getConnection());
            assertEquals(fullCondensedCatalog.length, catalog.length);
            for (int i = 0; i < fullCondensedCatalog.length; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
            // catalog deleted records
            catalog = TimeSeries.catalogTimeSeries(null, true, "D", db.getConnection());
            assertEquals(0, catalog.length);
            // catalog all records
            catalog = TimeSeries.catalogTimeSeries(null, true, "ND", db.getConnection());
            assertEquals(fullCondensedCatalog.length, catalog.length);
            for (int i = 0; i < fullCondensedCatalog.length; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
            //---------------------//
            // filter record names //
            //---------------------//
            catalog = TimeSeries.catalogTimeSeries(".+Version [0-3]", true, "N", db.getConnection());
            assertEquals(4, catalog.length);
            for (int i = 0; i < 4; ++i) {
                assertTrue(fullCondensedCatalog[i].equals(catalog[i]));
            }
        }
    }
}
