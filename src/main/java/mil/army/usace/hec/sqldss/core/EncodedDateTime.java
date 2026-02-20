package mil.army.usace.hec.sqldss.core;

import hec.heclib.util.HecTime;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Utility class for working with encoded dates and times.
 *
 * Encoded date/times are in the format <code>...YYYYMMDDhhmmss</code> where
 * <ul>
 *     <li><code>...YYYY</code> specifies the year (+/-) using as many decimal digits as necessary</li>
 *     <li><code>MM</code> specifies the month of the year in range <code>01..12</code></li>
 *     <li><code>DD</code> specifies the day of the month in range <code>01..31</code></li>
 *     <li><code>hh</code> specifies the hour of the day in range <code>00..23</code></li>
 *     <li><code>hh</code> specifies the minute of the hour in range <code>00..59</code></li>
 *     <li><code>ss</code> specifies the second of the minute in range <code>00..59</code></li>
 * </ul>
 * Encoded dates are in the format <code>...YYYYMMDD</code> using the same definitions.
 */
public final class EncodedDateTime {

    /**
     * The factor used to convert between encoded date/times and encoded dates
     */
    public static final int DATE_TO_TIME_FACTOR = 1_000_000;

    /**
     * Prevent class instantiation
     */
    private EncodedDateTime() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Create an encoded date/time from an HecTime integer (assuming minute granularity)
     * @param dateTime The HecTime integer
     * @return the equivalent encoded date/time
     * @throws EncodedDateTimeException If a valid HecTime object cannot be created from the date/time integer using
     * minute granularity
     */
    public static long encodeDateTime(int dateTime) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.showTimeAsBeginningOfDay(true);
        t.set(dateTime);
        if (t.isDateDefined() && !t.isTimeDefined()) {
            t.setYearMonthDay(t.year(), t.month(), t.day(), 0);
        }
        if (!t.isDefined()) {
            throw new EncodedDateTimeException("Invalid date/time string: "+dateTime);
        }
        return encodeDateTime(t);
    }

    /**
     * Create an encoded date/time from a date/time string
     * @param dateTime The date/time string
     * @return the equivalent encoded date/time
     * @throws EncodedDateTimeException If a valid HecTime object cannot be created from the date/time string
     */
    public static long encodeDateTime(String dateTime) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.showTimeAsBeginningOfDay(true);
        t.set(dateTime);
        if (t.isDateDefined() && !t.isTimeDefined()) {
            t.setYearMonthDay(t.year(), t.month(), t.day(), 0);
        }
        if (!t.isDefined()) {
            throw new EncodedDateTimeException("Invalid date/time string: "+dateTime);
        }
        return encodeDateTime(t);
    }

    /**
     * Create an encoded date/time from an HecTime object
     * @param dateTime The HecTime object
     * @return the equivalent encoded date/time
     * @throws EncodedDateTimeException if the date/time object is undefined
     */
    public static long encodeDateTime(HecTime dateTime) throws EncodedDateTimeException {
        HecTime t = new HecTime(dateTime);
        t.showTimeAsBeginningOfDay(true);
        if (t.isDateDefined() && !t.isTimeDefined()) {
            t.setYearMonthDay(t.year(), t.month(), t.day(), 0);
        }
        if (!t.isDefined()) {
            throw new EncodedDateTimeException("HecTime object is undefined: "+t);
        }
        return encodeDateTime(new int[]{t.year(), t.month(), t.day(), t.hour(), t.minute(), t.second()});
    }

    /**
     * Create an encoded date/time from a integer array
     * @param values The integer array of length 0..6 in Y, M, D, h, m, s order
     * @return The equivalent encoded date/time
     * @throws EncodedDateTimeException if thrown by {@link #normalizeDateTime(int[])}
     */
    public static long encodeDateTime(int[] values) throws EncodedDateTimeException {
        normalizeDateTime(values);
        int len = values.length;
        int y = len > 0 ? values[0] : 0;
        int m = len > 1 ? values[1] : 1;
        int d = len > 2 ? values[2] : 1;
        int h = len > 3 ? values[3] : 0;
        int n = len > 4 ? values[4] : 0;
        int s = len > 5 ? values[5] : 0;
        return (((((y * 100L + m) * 100 + d) * 100 + h) * 100 + n) * 100 + s);
    }

    /**
     * Normalizes an integer array if date/time values. Normalized values are:
     * <ul>
     *     <li><code>values[0]</code> (year) is unconstrained</li>
     *     <li><code>values[1]</code> (month) is <code>1..12</code></li>
     *     <li><code>values[2]</code> (day) is <code>1..31</code> (actually max day for year/month)</li>
     *     <li><code>values[3]</code> (hour) is <code>0..23</code></li>
     *     <li><code>values[4]</code> (minute) is <code>0..59</code></li>
     *     <li><code>values[5]</code> (second) is <code>0..59</code></li>
     * </ul>
     * @param values The integer array of length 0..6 in Y, M, D, h, m, s order
     * @throws EncodedDateTimeException If thrown {@link #lastDay(int, int)}
     */
    static void normalizeDateTime(int @NotNull [] values) throws EncodedDateTimeException {
        int len = values.length;
        if (len > 5) {
            // seconds
            normalizePair(values, 5, 0, 60, false);
        }
        if (len > 4) {
            // minutes
            normalizePair(values, 4, 0, 60, false);
        }
        if (len > 3) {
            // hours
            normalizePair(values, 3, 0, 24, false);
        }
        if (len > 1) {
            // months
            normalizePair(values, 1, 1, 12, true);
        }
        if (len > 2) {
            // days
            int maxDay = lastDay(values[0], values[1]);
            while (values[2] > maxDay) {
                values[1] += 1;
                if (values[1] > 12) {
                    values[0] += 1;
                    values[1] -= 12;
                }
                values[2] -= maxDay;
                maxDay = lastDay(values[0], values[1]);
            }
            while (values[2] < 1) {
                values[1] -= 1;
                if (values[1] < 1) {
                    values[0] -= 1;
                    values[1] += 12;
                }
                maxDay = lastDay(values[0], values[1]);
                values[2] += maxDay;
            }
        }
    }

    /**
     * Normalize a pair of items in a integer list of date/time items
     * @param values The integer array of length 0..6 in Y, M, D, h, m, s order
     * @param offset The offset into <code>values</code> of the smaller unit of the pair
     * @param minVal The minimum value of the smaller unit of the pair
     * @param maxVal The maximum value of the smaller unit of the pair
     * @param maxInclusive, Whether the maximum value is inclusive
     */
    private static void normalizePair(int @NotNull [] values, int offset, int minVal, int maxVal, boolean maxInclusive) {
        while (values[offset] > (maxInclusive ? maxVal : maxVal - 1)) {
            values[offset - 1] += 1;
            values[offset] -= maxVal;
        }
        while (values[offset] < minVal) {
            values[offset - 1] -= 1;
            values[offset] += maxVal;
        }
    }

    /**
     * Returns the last day of the month for a specifed year and month
     * @param y The year
     * @param m The month
     * @return The last day of the month
     * @throws EncodedDateTimeException If month is not in the range 1..12
     */
    static int lastDay(int y, int m) throws EncodedDateTimeException {
        return switch (m) {
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            case 2 -> isLeap(y) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            default -> throw new EncodedDateTimeException("Invalid month: " + m);
        };
    }

    /**
     * Returns whether the specified year is a leap year in the proleptic Gregorian calendar
     * @param y The year
     * @return Whether the year is a leap year
     */
    static boolean isLeap(int y) {
        return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    }

    /**
     * Creates an HecTime object from an encoded date/time
     * @param encoded The encoded date/time
     * @return The equivalent HecTime object
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static HecTime toHecTime(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        HecTime t = new HecTime();
        t.setYearMonthDay(dt[0], dt[1], dt[2], dt[3] * 60 + dt[4] + dt[5] / 60);
        t.showTimeAsBeginningOfDay(true);
        return t;
    }

    /**
     * Creates a string representation of an encoded date/time. The string is in ISO-8601 format except the date and time
     * are separated by a ' ' characater instead of a 'T' character
     * @param encoded The encoded date/time
     * @return The equivalent string representation
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static String toString(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        return String.format(
                "%d-%02d-%02d %02d:%02d:%02d",
                dt[0], dt[1], dt[2], dt[3], dt[4], dt[5]
        );
    }

    /**
     * Creates a string representation of an encoded date/time in the specified HecTime style
     * @param encoded The encoded date/time
     * @param hecTimeStyle The HecTime date style. See <b>HecTime Date Format</b> in <a href="https://www.hec.usace.army.mil/confluence/dssdocs/dssvueum/scripting/hectime-class">this page</a>
     * @return The equivalent string representation
     * @throws EncodedDateTimeException If thrown by {@link #toHecTime(long)}
     */
    public static String toString(long encoded, int hecTimeStyle) throws EncodedDateTimeException {
        return toHecTime(encoded).dateAndTime(hecTimeStyle);
    }

    /**
     * Creates a ISO-8601 format string representation of an encoded date/time
     * @param encoded The encoded date/time
     * @return The equivalent string representation
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static String toIsoString(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        return String.format(
                "%d-%02d-%02dT%02d:%02d:%02d",
                dt[0], dt[1], dt[2], dt[3], dt[4], dt[5]
        );
    }

    /**
     * Creates an encoded date from an encoded date/time
     * @param encodedDateTime The encoded date/time
     * @return The encoded date
     */
    public static long toEncodedDate(long encodedDateTime) {
        return encodedDateTime / DATE_TO_TIME_FACTOR;
    }

    /**
     * Creates an encoded date/time from an encoded date. The hour, minute, and second will all be zero
     * @param encodedDate The encoded date
     * @return The encoded date/time
     */
    public static long toEncodedDateTime(long encodedDate) {
        return encodedDate * DATE_TO_TIME_FACTOR;
    }

    /**
     * Creates a 6-integer array from an encoded date/time
     * @param encoded The encoded date/time
     * @return The equivalent values array
     * @throws EncodedDateTimeException If any item int the array would be out of normalized range
     */
    public static int[] toValues(long encoded) throws EncodedDateTimeException {
        long absVal = Math.abs(encoded);
        int sign = encoded < 0 ? -1 : 1;
        int s = (int) (absVal % 100);
        absVal /= 100;
        int n = (int) (absVal % 100);
        absVal /= 100;
        int h = (int) (absVal % 100);
        absVal /= 100;
        int d = (int) (absVal % 100);
        absVal /= 100;
        int m = (int) (absVal % 100);
        absVal /= 100;
        int y = (int) absVal * sign;
        if (m < 1 || m > 12
                || d < 1 || d > lastDay(y, m)
                || h < 0 || h > 23
                || n < 0 || n > 59
                || s < 0 || s > 59
        ) {
            throw new EncodedDateTimeException("Invalid encoded date/time: "+encoded);
        }
        return new int[]{y, m, d, h, n, s};
    }

    /**
     * Create an array of encoded date/times beginning at a specified time, at specified intervals, and of a specified length
     * @param encodedStartTime The date/time of the first value in the array
     * @param count The length of the array
     * @param intervalMinutes The interval minutes between each value in the array
     * @return The created array
     * @throws EncodedDateTimeException If thrown by {@link #incrementEncodedDateTime(long, int, int)}
     */
    public static long @NotNull [] makeRegularEncodedDateTimeArray(long encodedStartTime, int count, int intervalMinutes) throws EncodedDateTimeException {
        long[] encodedDateTimes = new long[count];
        encodedDateTimes[0] = encodedStartTime;
        for (int i = 1; i < count; ++i) {
            encodedDateTimes[i] = incrementEncodedDateTime(encodedDateTimes[i - 1], intervalMinutes, 1);
        }
        return encodedDateTimes;
    }

    /**
     * Create an array of encoded date/times beginning at a specified time, at specified intervals, and ending on or before a specified time
     * @param encodedStartTime The date/time of the first value in the array
     * @param encodedEndTime The date/time the array must end on or before
     * @param intervalMinutes The interval minutes between each value in the array
     * @return The created array
     * @throws EncodedDateTimeException If thrown by {@link #incrementEncodedDateTime(long, int, int)}
     */
    public static long @NotNull [] makeRegularEncodedDateTimeArray(long encodedStartTime, long encodedEndTime, int intervalMinutes) throws EncodedDateTimeException {
        return makeRegularEncodedDateTimeArray(
                encodedStartTime,
                intervalsBetween(encodedStartTime, encodedEndTime, intervalMinutes) + 1,
                intervalMinutes);
    }

    /**
     * Computes the number of <b>complete</b> intervals between two encoded date/times
     * @param encodedStartTime The start date/time
     * @param encodedEndTime The end date/time
     * @param intervalMinutes The interval minutes of the intervals to count
     * @return The number of complete intervals between the start and end date/times
     * @throws EncodedDateTimeException If thrown by {@link #incrementEncodedDateTime(long, int, int)}
     */
    public static int intervalsBetween(long encodedStartTime, long encodedEndTime, int intervalMinutes) throws EncodedDateTimeException {
        if (encodedStartTime > encodedEndTime) {
            throw new EncodedDateTimeException(String.format("Start time (%d) must not be greater than end time (%d)", encodedStartTime, encodedEndTime));
        }
        int count = 0;
        long encodedTime;
        encodedTime = encodedStartTime;
        while (encodedTime <= encodedEndTime) {
            ++count;
            encodedTime = incrementEncodedDateTime(encodedTime, intervalMinutes, 1);
        }
        return count - 1;
    }

    /**
     * Computes the date/time of a specified number of intervals of a specified size after a specified start date/time
     * @param encoded The start date/time
     * @param intervalMinutes The interval minutes of the interval to use
     * @param count The number of intervals after the start date/time
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If the interval minutes is not a valid combination of known intervals
     */
    public static long incrementEncodedDateTime(long encoded, int intervalMinutes, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        int minutesRemaining = intervalMinutes;
        for (int i = 0; i < count; ++i) {
            int num = 0;
            if (intervalMinutes >= Constants.YEAR_MINUTES) {
                num = intervalMinutes / Constants.YEAR_MINUTES;
                values[0] += num;
                if (i == 0) {
                    minutesRemaining -= num * Constants.YEAR_MINUTES;
                }
            }
            else if (intervalMinutes >= Constants.MONTH_MINUTES) {
                num = intervalMinutes / Constants.MONTH_MINUTES;
                values[1] += num;
                if (i == 0) {
                    minutesRemaining -= num * Constants.MONTH_MINUTES;
                }
            }
            else if (intervalMinutes >= Constants.DAY_MINUTES) {
                num = intervalMinutes / Constants.DAY_MINUTES;
                values[2] += num;
                if (i == 0) {
                    minutesRemaining -= num * Constants.DAY_MINUTES;
                }
            }
            else {
                values[4] += intervalMinutes; // minutes
                if (i == 0) {
                    minutesRemaining -= intervalMinutes;
                }
            }
            if (i == 0) {
                if (minutesRemaining != 0) {
                    throw new EncodedDateTimeException("Invalid interval minutes: "+minutesRemaining);
                }
            }
        }
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of years after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of years after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addYears(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[0] += count;
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of months after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of months after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addMonths(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[1] += count;
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of days after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of days after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addDays(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[2] += count;
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of hours after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of hours after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addHours(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[3] += count;
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of minutes after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of minutes after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addMinutes(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[4] += count;
        return encodeDateTime(values);
    }

    /**
     * Creates an encoded date/time that is a specified number of seconds after a specified encoded date/time
     * @param encoded The encoded date/time to start with
     * @param count The number of seconds after the encoded date/time to compute (before if count &lt; 0)
     * @return The resulting encoded date/time
     * @throws EncodedDateTimeException If thrown by {@link #toValues(long)}
     */
    public static long addSeconds(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[5] += count;
        return encodeDateTime(values);
    }

    /**
     * Create an encoded date from an HecTime integer (assuming minute granularity)
     * @param date The HecTime integer
     * @return the equivalent encoded date
     * @throws EncodedDateTimeException If a valid HecTime object cannot be created from the date/time integer using
     * minute granularity
     */
    public static long encodeDate(int date) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.set(date);
        return encodeDate(t);
    }

    /**
     * Create an encoded date from a date string
     * @param date The date/time string
     * @return the equivalent encoded date
     * @throws EncodedDateTimeException If a valid HecTime object cannot be created from the date/time string
     */
    public static long encodeDate(String date) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.set(date);
        return encodeDate(t);
    }

    /**
     * Create an encoded date from an HecTime object
     * @param date The HecTime object
     * @return the equivalent encoded date
     * @throws EncodedDateTimeException if the date object is undefined
     */
    public static long encodeDate(HecTime date) throws EncodedDateTimeException {
        HecTime t = new HecTime(date);
        t.showTimeAsBeginningOfDay(true);
        return encodeDate(new int[]{t.year(), t.month(), t.day()});
    }

    /**
     * Create an encoded date from a integer array
     * @param values The integer array of length 0..3 in Y, M, D order
     * @return The equivalent encoded date
     * @throws EncodedDateTimeException if thrown by {@link #normalizeDateTime(int[])}
     */
    public static long encodeDate(int[] values) throws EncodedDateTimeException {
        normalizeDateTime(values);
        int len = values.length;
        int y = len > 0 ? values[0] : 0;
        int m = len > 1 ? values[1] : 1;
        int d = len > 2 ? values[2] : 1;
        return ((y * 100L + m) * 100 + d);
    }

    /**
     * Creates an encoded date/time at a specified target time zone from and encoded date/time in a specified source time zone
     * @param dateTime The encoded date/time in the source time zone
     * @param fromZone The source timme zone
     * @param toZone The target time zone
     * @return The encoded date/time in the target time zone
     * @throws EncodedDateTimeException If thrown by {@link #encodeDateTime(int[])}
     */
    public static long changeTimeZone(long dateTime, ZoneId fromZone, ZoneId toZone) throws EncodedDateTimeException {
        int[] values = toValues(dateTime);
        ZonedDateTime fromTime  = ZonedDateTime.of(
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                0,
                fromZone);
        ZonedDateTime toTime = fromTime.withZoneSameInstant(toZone);
        values[0] = toTime.getYear();
        values[1] = toTime.getMonthValue();
        values[2] = toTime.getDayOfMonth();
        values[3] = toTime.getHour();
        values[4] = toTime.getMinute();
        values[5] = toTime.getSecond();
        return encodeDateTime(values);
    }
}
