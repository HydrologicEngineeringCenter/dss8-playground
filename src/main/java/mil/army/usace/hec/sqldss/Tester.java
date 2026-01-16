package mil.army.usace.hec.sqldss;

import java.sql.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import hec.heclib.dss.HecDss;
import hec.heclib.util.Heclib;
import hec.io.TimeSeriesContainer;
import mil.army.usace.hec.sqldss.api.dss7.HecSqlDss;

public class Tester {

    public static void main(String[] args) throws Exception {
        String target7Filename = "t:/tester.dss7.dss";
        String target8Filename = "t:/tester.dss8.dss";
        long t1, t2, t3, t4;
        int valueCount = 0;

        Heclib.zset("MLVL", "", 0);
        t1 = System.currentTimeMillis();
        HecDss srcDss = HecDss.open("t:/ensemble_for_beth1.dss", true);
        t2 = System.currentTimeMillis();
        System.out.println("Time to open existing DSS7 file: "+ (t2 - t1));
        List<String> pathnames = srcDss.getPathnameList();
        for (int i = 0; i < pathnames.size(); ++i) {
            pathnames.set(i, pathnames.get(i).replace("1HOUR", "1Hour"));
        }
        //-----------------//
        // time DSS7 reads //
        //-----------------//
        TimeSeriesContainer[] tscs = new TimeSeriesContainer[Math.min(pathnames.size(), 1000)];
        t1 = System.currentTimeMillis();
        for (int i = 0; i < tscs.length; ++i) {
            tscs[i] = (TimeSeriesContainer) srcDss.get(pathnames.get(i));
            valueCount += tscs[i].numberValues;
        }
        t2 = System.currentTimeMillis();
        srcDss.close();
        System.out.println(String.format("Time to read %d pathnames (%d values) from DSS7: %d", tscs.length, valueCount, t2-t1));
        for (int i = 0; i < tscs.length; ++i) {
            tscs[i].fileName = null;
        }
        //------------------//
        // time DSS7 writes //
        //------------------//
        Files.deleteIfExists(Path.of(target7Filename));
        t1 = System.currentTimeMillis();
        HecDss dss7 = HecDss.open(target7Filename);
        t2 = System.currentTimeMillis();
        System.out.println("Time to create empty DSS7 file: "+ (t2 - t1));
        t1 = System.currentTimeMillis();
        for (TimeSeriesContainer tsc2 : tscs) {
            dss7.put(tsc2);
        }
        dss7.done();
        t2 = System.currentTimeMillis();
        System.out.println(String.format("Time to write %d pathnames (%d values) to DSS7: %d", tscs.length, valueCount, t2-t1));
        //------------------//
        // time DSS8 writes //
        //------------------//
        Files.deleteIfExists(Path.of(target8Filename));
        t1 = System.currentTimeMillis();
        HecSqlDss dss8 = HecSqlDss.open(target8Filename);
        dss8.setAutoCommit(false);
        t2 = System.currentTimeMillis();
        System.out.println("Time to create empty DSS8 file: "+ (t2 - t1));
        t1 = System.currentTimeMillis();
        for (int i = 0; i < tscs.length; ++i) {
            if (i > 0 && i % 100 == 0) {
                dss8.commit();
            }
            dss8.put(tscs[i]);
        }
        dss8.commit();
        dss8.close();
        t2 = System.currentTimeMillis();
        System.out.println(String.format("Time to write %d pathnames (%d values) to DSS8: %d", tscs.length, valueCount, t2-t1));
        t1 = System.currentTimeMillis();
        dss8 = HecSqlDss.open(target8Filename);
        t2 = System.currentTimeMillis();
        System.out.println("Time to open existing DSS8 file: "+ (t2 - t1));
        //-----------------//
        // time DSS8 reads //
        //-----------------//
        t1 = System.currentTimeMillis();
        for (int i = 0; i < tscs.length; ++i) {
            tscs[i] = (TimeSeriesContainer) dss8.getInUnit(tscs[i].fullName, tscs[i].units);
            valueCount += tscs[i].numberValues;
        }
        dss8.close();
        t2 = System.currentTimeMillis();
        System.out.println(String.format("Time to read %d pathnames (%d values) from DSS8: %d", tscs.length, valueCount, t2-t1));
    }
}
