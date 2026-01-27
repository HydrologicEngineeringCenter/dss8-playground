package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import com.google.common.flogger.FluentLogger;
import mil.army.usace.hec.sqldss.core.Constants;
import mil.army.usace.hec.sqldss.core.EncodedDateTime;
import mil.army.usace.hec.sqldss.core.TimeSeries;
import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.util.Arrays;

import static mil.army.usace.hec.metadata.constants.NumericalConstants.UNDEFINED_DOUBLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeSeriesTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    TimeSeries.TsvData existing = new TimeSeries.TsvData();
    TimeSeries.TsvData incoming = new TimeSeries.TsvData();
    TimeSeries.TsvData merged = new TimeSeries.TsvData();

    @Test
    public void testMergeReplaceAllDisjoint() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[3];
        double[] incomingValues = new double[3];

        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 3) {
                existingValues[i] = 10 + i;
            }
            if (i > 5) {
                incomingValues[i-6] = 100 + i;
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 6, 9);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(times.length, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(times, merged.times);
        assertArrayEquals(new double[]{10,11,12,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,106,107,108}, merged.values);
    }

    @Test
    public void testMergeReplaceAllOverlap() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[9];
        double[] incomingValues = new double[3];

        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            existingValues[i] = 10 + i;
            if (i > 2 && i < 6) {
                incomingValues[i-3] = 100 + i;
            }
        }

        existing.times = times;
        existing.values = existingValues;
        existing.count = 9;
        incoming.times = Arrays.copyOfRange(times, 3, 6);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(times.length, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(times, merged.times);
        assertArrayEquals(new double[]{10,11,12,103,104,105,16,17,18}, merged.values);
    }

    @Test
    public void testMergeReplaceAllParialOverlap() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[6];
        double[] incomingValues = new double[6];

        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 6) {
                existingValues[i] = 10 + i;
            }
            if (i > 2) {
                incomingValues[i-3] = 100 + i;
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 6);
        existing.values = existingValues;
        existing.count = 6;
        incoming.times = Arrays.copyOfRange(times, 3, 9);
        incoming.values = incomingValues;
        incoming.count = 6;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(times.length, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(times, merged.times);
        assertArrayEquals(new double[]{10,11,12,103,104,105,106,107,108}, merged.values);
    }
}
