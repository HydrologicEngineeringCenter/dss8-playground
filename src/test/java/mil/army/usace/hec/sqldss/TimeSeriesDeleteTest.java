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

public class TimeSeriesDeleteTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    SqlDss _db = null;

    SqlDss getDb() throws IOException, CoreException, SQLException, EncodedDateTimeException {
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
            StringBuilder sb = new StringBuilder();
            for (TimeSeriesContainer tsc : tscs) {
                TimeSeries.putTimeSeriesValues(tsc, String.valueOf(Constants.REGULAR_STORE_RULE.REPLACE_ALL), db.getConnection());
            }
            String[] catalog = TimeSeries.catalogTimeSeries(".+Version [34]", false, "N", db.getConnection());
            for (String name: catalog) {
                sb.append(name).append("\n");
            }
            logger.atInfo().log(sb.toString());
        }
    }
}
