package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import com.google.common.flogger.FluentLogger;
import mil.army.usace.hec.sqldss.core.Constants;
import mil.army.usace.hec.sqldss.core.EncodedDateTime;
import mil.army.usace.hec.sqldss.core.TimeSeries;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static mil.army.usace.hec.metadata.constants.NumericalConstants.UNDEFINED_DOUBLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeSeriesMergeTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    TimeSeries.TsvData existing = new TimeSeries.TsvData();
    TimeSeries.TsvData incoming = new TimeSeries.TsvData();
    TimeSeries.TsvData merged = new TimeSeries.TsvData();

    @Test
    public void testRegularMergeReplaceAllNoOverlap() throws Exception {

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
    public void testRegularMergeReplaceAllOverlap() throws Exception {

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
    public void testRegularMergeReplaceAllPartialOverlap() throws Exception {

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

    @Test
    public void testRegularMergeDoNotReplaceNoOverlap() throws Exception {

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
                Constants.REGULAR_STORE_RULE.DO_NOT_REPLACE,
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
    public void testRegularMergeDoNotReplaceOverlap() throws Exception {

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
                Constants.REGULAR_STORE_RULE.DO_NOT_REPLACE,
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
        assertArrayEquals(existing.values, merged.values);
    }

    @Test
    public void testRegularMergeDoNotReplacePartialOverlap() throws Exception {

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
                Constants.REGULAR_STORE_RULE.DO_NOT_REPLACE,
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
        assertArrayEquals(new double[]{10,11,12,13,14,15,106,107,108}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceMissingValuesOnlyNoOverlap() throws Exception {

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
        incomingValues[1] = existingValues[1] = UNDEFINED_DOUBLE;

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 6, 9);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
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
        assertArrayEquals(new double[]{10,UNDEFINED_DOUBLE,12,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,106,UNDEFINED_DOUBLE,108}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceMissingValuesOnlyOverlap() throws Exception {

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
        existingValues[4] = UNDEFINED_DOUBLE;

        existing.times = times;
        existing.values = existingValues;
        existing.count = 9;
        incoming.times = Arrays.copyOfRange(times, 3, 6);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
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
        assertArrayEquals(new double[]{10,11,12,13,104,15,16,17,18}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceMissingValuesOnlyPartialOverlap() throws Exception {

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
        existingValues[4] = UNDEFINED_DOUBLE;

        existing.times = Arrays.copyOfRange(times, 0, 6);
        existing.values = existingValues;
        existing.count = 6;
        incoming.times = Arrays.copyOfRange(times, 3, 9);
        incoming.values = incomingValues;
        incoming.count = 6;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
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
        assertArrayEquals(new double[]{10,11,12,13,104,15,106,107,108}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceWithNonMissingNoOverlap() throws Exception {

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
        incomingValues[1] = existingValues[1] = UNDEFINED_DOUBLE;

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 6, 9);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
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
        assertArrayEquals(new double[]{10,UNDEFINED_DOUBLE,12,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,106,UNDEFINED_DOUBLE,108}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceWithNonMissingOverlap() throws Exception {

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
        existingValues[2] = UNDEFINED_DOUBLE;
        incomingValues[1] = UNDEFINED_DOUBLE;

        existing.times = times;
        existing.values = existingValues;
        existing.count = 9;
        incoming.times = Arrays.copyOfRange(times, 3, 6);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
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
        assertArrayEquals(new double[]{10,11,UNDEFINED_DOUBLE,103,14,105,16,17,18}, merged.values);
    }

    @Test
    public void testRegularMergeReplaceWithNonMissingPartialOverlap() throws Exception {

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
        existingValues[2] = UNDEFINED_DOUBLE;
        incomingValues[0] = incomingValues[2] = UNDEFINED_DOUBLE;

        existing.times = Arrays.copyOfRange(times, 0, 6);
        existing.values = existingValues;
        existing.count = 6;
        incoming.times = Arrays.copyOfRange(times, 3, 9);
        incoming.values = incomingValues;
        incoming.count = 6;

        TimeSeries.mergeTimeSeries(
                intervalMinutes,
                Constants.REGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
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
        assertArrayEquals(new double[]{10,11,UNDEFINED_DOUBLE,13,104,15,106,107,108}, merged.values);
    }

    @Test
    public void testIrregularReplaceAllNoOverlap() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[3];
        double[] incomingValues = new double[3];

        int expectedMergedCount = 6;
        long[] expectedMergedTimes = new long[expectedMergedCount];
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 3) {
                existingValues[i] = 10 + i;
                expectedMergedTimes[i] = times[i];
            }
            if (i > 5) {
                incomingValues[i-6] = 100 + i;
                expectedMergedTimes[i-3] = times[i];
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 6, 9);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,12,106,107,108}, merged.values);
    }

    @Test
    public void testIrregularReplaceAllOverlapAligned() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[count];
        double[] incomingValues = new double[3];

        int expectedMergedCount = count;
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            existingValues[i] = 10 + i;
            if (i > 2 && i < 6) {
                incomingValues[i-3] = 100 + i;
            }
        }
        long[] expectedMergedTimes = Arrays.copyOf(times, expectedMergedCount);

        existing.times = Arrays.copyOfRange(times, 0, 9);
        existing.values = existingValues;
        existing.count = existingValues.length;
        incoming.times = Arrays.copyOfRange(times, 3, 6);
        incoming.values = incomingValues;
        incoming.count = incomingValues.length;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,12,103,104,105,16,17,18}, merged.values);
    }

    @Test
    public void testIrregularReplaceAllOverlapNonAligned() throws Exception {

        int count = 12;

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L,
        };
        existing.values = new double[]{10,11,12,13,14,15,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
        };
        incoming.values = new double[]{103.25, 104.25, 105.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101050000L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L,
        };
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,13,103.25,14,104.25,15,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceAllPartialOverlapAligned() throws Exception {

        int count = 5;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[3];
        double[] incomingValues = new double[3];

        int expectedMergedCount = 5;
        long[] expectedMergedTimes = new long[expectedMergedCount];
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 3) {
                existingValues[i] = 10 + i;
                expectedMergedTimes[i] = times[i];
            }
            if (i > 1) {
                incomingValues[i-2] = 100 + i;
                expectedMergedTimes[i] = times[i];
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 2, 5);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,102,103, 104}, merged.values);
    }

    @Test
    public void testIrregularReplaceAllPartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, 11, 12};
        existing.count = 3;
        incoming.times = new long[]{20250101011500L,20250101021500L,20250101031500L};
        incoming.values = new double[]{101.25, 102.25, 103.25};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101011500L,
                20250101020000L,
                20250101021500L,
                20250101031500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {
                10,
                11,
                101.25,
                12,
                102.25,
                103.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_ALL,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularMergeNoOverlap() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[3];
        double[] incomingValues = new double[3];

        int expectedMergedCount = 6;
        long[] expectedMergedTimes = new long[expectedMergedCount];
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 3) {
                existingValues[i] = 10 + i;
                expectedMergedTimes[i] = times[i];
            }
            if (i > 5) {
                incomingValues[i-6] = 100 + i;
                expectedMergedTimes[i-3] = times[i];
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 6, 9);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.MERGE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,12,106,107,108}, merged.values);
    }

    @Test
    public void testIrregularMergeOverlapAligned() throws Exception {

        int count = 9;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[count];
        double[] incomingValues = new double[3];

        int expectedMergedCount = count;
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            existingValues[i] = 10 + i;
            if (i > 2 && i < 6) {
                incomingValues[i-3] = 100 + i;
            }
        }
        long[] expectedMergedTimes = Arrays.copyOf(times, expectedMergedCount);

        existing.times = Arrays.copyOfRange(times, 0, 9);
        existing.values = existingValues;
        existing.count = existingValues.length;
        incoming.times = Arrays.copyOfRange(times, 3, 6);
        incoming.values = incomingValues;
        incoming.count = incomingValues.length;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.MERGE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,12,103,104,105,16,17,18}, merged.values);
    }

    @Test
    public void testIrregularMergeOverlapNonAligned() throws Exception {

        int count = 12;

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L,
        };
        existing.values = new double[]{10,11,12,13,14,15,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
        };
        incoming.values = new double[]{103.25, 104.25, 105.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101050000L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L,
        };
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,13,103.25,14,104.25,15,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.MERGE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularMergePartialOverlapAligned() throws Exception {

        int count = 5;
        int intervalMinutes = 60;
        long[] times = new long[count];
        double[] existingValues = new double[3];
        double[] incomingValues = new double[3];

        int expectedMergedCount = 5;
        long[] expectedMergedTimes = new long[expectedMergedCount];
        for (int i = 0; i < count; ++i) {
            times[i] = EncodedDateTime.addMinutes(20250101000000L, i * intervalMinutes);
            if (i < 3) {
                existingValues[i] = 10 + i;
                expectedMergedTimes[i] = times[i];
            }
            if (i > 1) {
                incomingValues[i-2] = 100 + i;
                expectedMergedTimes[i] = times[i];
            }
        }

        existing.times = Arrays.copyOfRange(times, 0, 3);
        existing.values = existingValues;
        existing.count = 3;
        incoming.times = Arrays.copyOfRange(times, 2, 5);
        incoming.values = incomingValues;
        incoming.count = 3;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.MERGE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(new double[]{10,11,102,103, 104}, merged.values);
    }

    @Test
    public void testIrregularMergePartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, 11, 12};
        existing.count = 3;
        incoming.times = new long[]{20250101011500L,20250101021500L,20250101031500L};
        incoming.values = new double[]{101.25, 102.25, 103.25};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101011500L,
                20250101020000L,
                20250101021500L,
                20250101031500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {
                10,
                11,
                101.25,
                12,
                102.25,
                103.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.MERGE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceMissingValuesOnlyNoOverlap() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, UNDEFINED_DOUBLE, 12};
        existing.count = 3;
        incoming.times = new long[]{20250101060000L,20250101070000L,20250101080000L};
        incoming.values = new double[]{106, UNDEFINED_DOUBLE, 108};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {
                10,
                UNDEFINED_DOUBLE,
                12,
                106,
                UNDEFINED_DOUBLE,
                108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceMissingValuesOnlyOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L};
        incoming.values = new double[]{103, UNDEFINED_DOUBLE, 105};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,UNDEFINED_DOUBLE,105,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceMissingValuesOnlyOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101050000L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceMissingValuesOnlyPartialOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        incoming.values = new double[]{103, UNDEFINED_DOUBLE, 105, 106, 107, 108};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,UNDEFINED_DOUBLE,105,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceMissingValuesOnlyPartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25, 106.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,106.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceWithNonMissingNoOverlap() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, UNDEFINED_DOUBLE, 12};
        existing.count = existing.values.length;
        incoming.times = new long[]{20250101060000L,20250101070000L,20250101080000L};
        incoming.values = new double[]{106, UNDEFINED_DOUBLE, 108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {
                10,
                UNDEFINED_DOUBLE,
                12,
                106,
                UNDEFINED_DOUBLE,
                108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceWithNonMissingOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L};
        incoming.values = new double[]{103, UNDEFINED_DOUBLE, 105};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,103,UNDEFINED_DOUBLE,105,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceWithNonMissingOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101050000L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceWithNonMissingPartialOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        incoming.values = new double[]{103, UNDEFINED_DOUBLE, 105, 106, 107, 108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,103,UNDEFINED_DOUBLE,105,106,107,108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_WITH_NON_MISSING,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularReplaceWithNonMissingPartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25, 106.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,106.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.REPLACE_MISSING_VALUES_ONLY,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDoNotReplaceNoOverlap() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, UNDEFINED_DOUBLE, 12};
        existing.count = existing.values.length;
        incoming.times = new long[]{20250101060000L,20250101070000L,20250101080000L};
        incoming.values = new double[]{106, UNDEFINED_DOUBLE, 108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {
                10,
                UNDEFINED_DOUBLE,
                12,
                106,
                UNDEFINED_DOUBLE,
                108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DO_NOT_REPLACE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDoNotReplaceOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L};
        incoming.values = new double[]{103, UNDEFINED_DOUBLE, 105};
        incoming.count = 3;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = existing.values;

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DO_NOT_REPLACE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDoNotReplaceOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101050000L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DO_NOT_REPLACE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDoNotReplacePartialOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        incoming.values = new double[]{103,UNDEFINED_DOUBLE,105,106,107,108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,106,107,108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DO_NOT_REPLACE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDoNotReplacePartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L};
        existing.values = new double[]{10,11,12,13,UNDEFINED_DOUBLE};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        incoming.values = new double[]{103.25, UNDEFINED_DOUBLE, 105.25, 106.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101040000L,
                20250101041500L,
                20250101051500L,
                20250101061500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,13,103.25,UNDEFINED_DOUBLE,UNDEFINED_DOUBLE,105.25,106.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DO_NOT_REPLACE,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDeleteInsertNoOverlap() throws Exception {

        existing.times = new long[]{20250101000000L,20250101010000L,20250101020000L};
        existing.values = new double[]{10, 11, 12};
        existing.count = existing.values.length;
        incoming.times = new long[]{20250101060000L,20250101070000L,20250101080000L};
        incoming.values = new double[]{106, 107, 108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = {10,11,12,106,107,108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DELETE_INSERT,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDeleteInsertOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,14,15,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L};
        incoming.values = new double[]{103, 104, 105};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,103,104,105,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DELETE_INSERT,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDeleteInsertOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        existing.values = new double[]{10,11,12,13,14,15,16,17,18};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L};
        incoming.values = new double[]{103.25, 104.25, 105.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,13,103.25,104.25,105.25,16,17,18};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DELETE_INSERT,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDeleteInsertPartialOverlapAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L};
        existing.values = new double[]{10,11,12,13,14,15};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        incoming.values = new double[]{103,104,105,106,107,108};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L,
                20250101060000L,
                20250101070000L,
                20250101080000L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,103,104,105,106,107,108};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DELETE_INSERT,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }

    @Test
    public void testIrregularDeleteInsertPartialOverlapNonAligned() throws Exception {

        existing.times = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101040000L,
                20250101050000L};
        existing.values = new double[]{10,11,12,13,14,15};
        existing.count = existing.values.length;
        incoming.times = new long[]{
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101061500L,
                20250101071500L,
                20250101081500L};
        incoming.values = new double[]{103.25,104.25,105.25,106.25,107.25,108.25};
        incoming.count = incoming.values.length;
        long[] expectedMergedTimes = new long[]{
                20250101000000L,
                20250101010000L,
                20250101020000L,
                20250101030000L,
                20250101031500L,
                20250101041500L,
                20250101051500L,
                20250101061500L,
                20250101071500L,
                20250101081500L};
        int expectedMergedCount = expectedMergedTimes.length;
        double[] expectedMergedValues = new double[]{10,11,12,13,103.25,104.25,105.25,106.25,107.25,108.25};

        TimeSeries.mergeTimeSeries(
                Constants.IRREGULAR_STORE_RULE.DELETE_INSERT,
                incoming,
                existing,
                merged
        );

        assertEquals(expectedMergedCount, merged.count);
        if (merged.times.length > merged.count) {
            merged.times = Arrays.copyOfRange(merged.times, 0, merged.count);
            merged.values = Arrays.copyOfRange(merged.values, 0, merged.count);
            merged.qualities = Arrays.copyOfRange(merged.qualities, 0, merged.count);
        }
        assertArrayEquals(expectedMergedTimes, merged.times);
        assertArrayEquals(expectedMergedValues, merged.values);
    }
}
