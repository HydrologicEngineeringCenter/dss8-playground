package mil.army.usace.hec.sqldss;

import com.google.common.flogger.FluentLogger;
import mil.army.usace.hec.sqldss.api.dss7.HecSqlDss;
import mil.army.usace.hec.sqldss.core.SqlDss;
import mil.army.usace.hec.sqldss.core.TimeSeries;
import org.apache.poi.ss.formula.functions.T;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import hec.heclib.dss.HecDss;
import hec.heclib.util.Heclib;
import hec.io.TimeSeriesContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class Dss7ComparisonTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    String packageName = Dss7ComparisonTest.class.getPackageName();
    String packageDir = packageName.replace('.', '/');

    static long startTimer() {
        return System.currentTimeMillis();
    }

    static long endTimer(long startTime) {
        return System.currentTimeMillis() - startTime;
    }

    static @NotNull String getFileName(@NotNull URL resource) throws URISyntaxException, IOException {
        return new File(resource.toURI()).getCanonicalPath();
    }

    @ParameterizedTest
    @CsvSource({
            "10",
            "100",
            "1000",
//            "10000",
//            "100000"
    })
    public void compareToDss7(int maxPathnames) throws Exception {
        String sourceBasename = "time_series_source.dss";
        String target7Basename = "tester.dss7.dss";
        String target8Basename = "tester.dss8.dss";
        String sourceFilename = Paths.get(packageDir,sourceBasename).toString();
        String target7Filename = Paths.get(packageDir, target7Basename).toString();
        String target8Filename = Paths.get(packageDir,target8Basename).toString();
        long startTime;
        long elapsedCreateDss7 = -1;
        long elapsedCreateDss8 = -1;
        long elapsedOpenDss7 = -1;
        long elapsedOpenDss8 = -1;
        long elapsedReadDss7 = -1;
        long elapsedReadDss8 = -1;
        long elapsedWriteDss7 = -1;
        long elapsedWriteDss8 = -1;
        long elapsedOverwriteDss7 = -1;
        long elapsedOverwriteDss8 = -1;
        long dss7FileSize;
        long dss8FileSize;

        URL sourceResource = getClass().getClassLoader().getResource(sourceFilename);
        URL target7Resource = new URI(sourceResource.toString().replace(sourceBasename, target7Basename)).toURL();
        URL target8Resource = new URI(sourceResource.toString().replace(sourceBasename, target8Basename)).toURL();
        assertNotNull(sourceResource, "Missing resource: "+sourceResource);

        Heclib.zset("MLVL", "", 0);
        //-----------------------------------//
        // open source DSS7 and read records //
        //-----------------------------------//
        startTime = startTimer();
        HecDss sourceDss = HecDss.open(getFileName(sourceResource));
        elapsedOpenDss7 = endTimer(startTime);
        List<String> pathnames = sourceDss.getPathnameList();
        TimeSeriesContainer[] tscs = new TimeSeriesContainer[Math.min(pathnames.size(), maxPathnames)];
        startTime = startTimer();
        for (int i = 0; i < tscs.length; ++i) {
            tscs[i] = (TimeSeriesContainer) sourceDss.get(pathnames.get(i));
            tscs[i].fileName = null;
        }
        elapsedReadDss7 = endTimer(startTime);
        sourceDss.close();
        //-----------------------------------//
        // create new DSS7 and write records //
        //-----------------------------------//
        Files.deleteIfExists(Path.of(getFileName(target7Resource)));
        startTime = startTimer();
        HecDss targetDss7 = HecDss.open(getFileName(target7Resource));
        elapsedCreateDss7 = endTimer(startTime);
        startTime = startTimer();
        for (TimeSeriesContainer tsc: tscs) {
            targetDss7.put(tsc);
        }
        targetDss7.close();
        elapsedWriteDss7 = endTimer(startTime);
        //------------------------//
        // overwrite DSS7 records //
        //------------------------//
        targetDss7 = HecDss.open(getFileName(target7Resource));
        startTime = startTimer();
        for (TimeSeriesContainer tsc: tscs) {
            targetDss7.put(tsc);
        }
        targetDss7.close();
        elapsedOverwriteDss7 = endTimer(startTime);
        //------------------------//
        // get the DSS7 file size //
        //------------------------//
        dss7FileSize = Files.size(Path.of(target7Resource.toURI()));
        //-----------------------------------//
        // create new DSS8 and write records //
        //-----------------------------------//
        Files.deleteIfExists(Path.of(getFileName(target8Resource)));
        startTime = startTimer();
        HecSqlDss targetDss8 = HecSqlDss.open(getFileName(target8Resource));
        targetDss8.setAutoCommit(false);
        elapsedCreateDss8 = endTimer(startTime);
        startTime = startTimer();
        long t1;
        for (int i = 0; i < tscs.length; ++i) {
            if (i > 0 && i % 100 == 0) {
                targetDss8.commit();
            }
            targetDss8.put(tscs[i]);
        }
        targetDss8.commit();
        targetDss8.close();
        elapsedWriteDss8 = endTimer(startTime);
        //-------------------------------------//
        // open existing DSS8 and read records //
        //-------------------------------------//
        startTime = startTimer();
        targetDss8 = HecSqlDss.open(getFileName(target8Resource));
        elapsedOpenDss8 = endTimer(startTime);
        startTime = startTimer();
        targetDss8.setTrimMissing(true);
        for (int i = 0; i < tscs.length; ++i) {
            logger.atInfo().log("tscs[%d] = %s", i, tscs[i]);
            TimeSeriesContainer tsc = (TimeSeriesContainer) targetDss8.getInUnit(tscs[i].fullName, tscs[i].units);
            assertArrayEquals(tscs[i].times, tsc.times);
            assertArrayEquals(tscs[i].values, tsc.values, 1e-6);
        }
        elapsedReadDss8 = endTimer(startTime);
        targetDss8.done();
        //------------------------//
        // overwrite DSS8 records //
        //------------------------//
        targetDss8 = HecSqlDss.open(getFileName(target8Resource));
        targetDss8.setAutoCommit(false);
        startTime = startTimer();
        for (int i = 0; i < tscs.length; ++i) {
            if (i > 0 && i % 100 == 0) {
                targetDss8.commit();
            }
            targetDss8.put(tscs[i]);
        }
        targetDss8.commit();
        targetDss8.done();
        elapsedOverwriteDss8 = endTimer(startTime);
        //------------------------//
        // get the DSS8 file size //
        //------------------------//
        dss8FileSize = Files.size(Path.of(target8Resource.toURI()));
        //----------------//
        // output results //
        //----------------//
        String format = "Number of Records : %d\n" +
                "DSS7 create new         : %d ms\n" +
                "DSS7 open existing      : %d ms\n" +
                "DSS7 read records       : %d ms\n" +
                "DSS7 write records      : %d ms\n" +
                "DSS7 overwrite records  : %d ms\n" +
                "DSS7 file size          : %d bytes\n" +
                "DSS8 create new         : %d ms (x %.3f)\n" +
                "DSS8 open existing      : %d ms (x %.3f)\n" +
                "DSS8 read records       : %d ms (x %.3f)\n" +
                "DSS8 write records      : %d ms (x %.3f)\n" +
                "DSS8 overwrite records  : %d ms (x %.3f)\n" +
                "DSS8 file size          : %d bytes (x %.3f)\n";
        logger.atInfo().log(
                format,
                tscs.length,
                elapsedCreateDss7,
                elapsedOpenDss7,
                elapsedReadDss7,
                elapsedWriteDss7,
                elapsedOverwriteDss7,
                dss7FileSize,
                elapsedCreateDss8, 1.*elapsedCreateDss8/elapsedCreateDss7,
                elapsedOpenDss8, 1.*elapsedOpenDss8/elapsedOpenDss7,
                elapsedReadDss8, 1.*elapsedReadDss8/elapsedReadDss7,
                elapsedWriteDss8, 1.*elapsedWriteDss8/elapsedWriteDss7,
                elapsedOverwriteDss8, 1.*elapsedOverwriteDss8/elapsedOverwriteDss7,
                dss8FileSize, 1.*dss8FileSize/dss7FileSize
        );
    }
}
