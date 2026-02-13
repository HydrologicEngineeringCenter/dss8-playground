package mil.army.usace.hec.sqldss.core;

import hec.heclib.util.HecTime;
import org.jetbrains.annotations.NotNull;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class EncodedDateTime {

    public static final int DATE_TO_TIME_FACTOR = 1_000_000;

    private EncodedDateTime() {
        throw new AssertionError("Cannot instantiate");
    }

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

    public static long encodeDateTime(HecTime t) throws EncodedDateTimeException {
        t = new HecTime(t);
        t.showTimeAsBeginningOfDay(true);
        if (t.isDateDefined() && !t.isTimeDefined()) {
            t.setYearMonthDay(t.year(), t.month(), t.day(), 0);
        }
        if (!t.isDefined()) {
            throw new EncodedDateTimeException("HecTime object is undefined: "+t);
        }
        return encodeDateTime(new int[]{t.year(), t.month(), t.day(), t.hour(), t.minute(), t.second()});
    }

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

    private static void normalizePair(int[] values, int offset, int minVal, int maxVal, boolean maxInclusive) {
        while (values[offset] > (maxInclusive ? maxVal : maxVal - 1)) {
            values[offset - 1] += 1;
            values[offset] -= maxVal;
        }
        while (values[offset] < minVal) {
            values[offset - 1] -= 1;
            values[offset] += maxVal;
        }
    }

    static int lastDay(int y, int m) throws EncodedDateTimeException {
        return switch (m) {
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            case 2 -> isLeap(y) ? 29 : 28;
            case 4, 6, 9, 11 -> 30;
            default -> throw new EncodedDateTimeException("Invalid month: " + m);
        };
    }

    static boolean isLeap(int y) {
        return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    }

    public static HecTime toHecTime(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        HecTime t = new HecTime();
        t.setYearMonthDay(dt[0], dt[1], dt[2], dt[3] * 60 + dt[4] + dt[5] / 60);
        t.showTimeAsBeginningOfDay(true);
        return t;
    }

    public static String toString(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        return String.format(
                "%d-%02d-%02d %02d:%02d:%02d",
                dt[0], dt[1], dt[2], dt[3], dt[4], dt[5]
        );
    }

    public static String toString(long encoded, int hecTimeStyle) throws EncodedDateTimeException {
        return toHecTime(encoded).dateAndTime(hecTimeStyle);
    }

    public static String toIsoString(long encoded) throws EncodedDateTimeException {
        int[] dt = toValues(encoded);
        return String.format(
                "%d-%02d-%02dT%02d:%02d:%02d",
                dt[0], dt[1], dt[2], dt[3], dt[4], dt[5]
        );
    }

    public static long toEncodedDate(long encodedDateTime) {
        return encodedDateTime / DATE_TO_TIME_FACTOR;
    }

    public static long toEncodedDateTime(long encodedDate) {
        return encodedDate * DATE_TO_TIME_FACTOR;
    }

    public static int[] toValues(long encoded) throws EncodedDateTimeException {
        long ae = Math.abs(encoded);
        int sign = encoded < 0 ? -1 : 1;
        int s = (int) (ae % 100);
        ae /= 100;
        int n = (int) (ae % 100);
        ae /= 100;
        int h = (int) (ae % 100);
        ae /= 100;
        int d = (int) (ae % 100);
        ae /= 100;
        int m = (int) (ae % 100);
        ae /= 100;
        int y = (int) ae * sign;
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

    public static long[] makeRegularEncodedDateTimeArray(long encodedStartTime, int count, int intervalMinutes) throws EncodedDateTimeException {
        long[] encodedDateTimes = new long[count];
        encodedDateTimes[0] = encodedStartTime;
        for (int i = 1; i < count; ++i) {
            encodedDateTimes[i] = incrementEncodedDateTime(encodedDateTimes[i - 1], intervalMinutes, 1);
        }
        return encodedDateTimes;
    }

    public static long[] makeRegularEncodedDateTimeArray(long encodedStartTime, long encodedEndTime, int intervalMinutes) throws EncodedDateTimeException {
        return makeRegularEncodedDateTimeArray(
                encodedStartTime,
                intervalsBetween(encodedStartTime, encodedEndTime, intervalMinutes) + 1,
                intervalMinutes);
    }

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

    public static long addYears(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[0] += count;
        return encodeDateTime(values);
    }

    public static long addMonths(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[1] += count;
        return encodeDateTime(values);
    }

    public static long addDays(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[2] += count;
        return encodeDateTime(values);
    }

    public static long addHours(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[3] += count;
        return encodeDateTime(values);
    }

    public static long addMinutes(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[4] += count;
        return encodeDateTime(values);
    }

    public static long addSeconds(long encoded, int count) throws EncodedDateTimeException {
        int[] values = toValues(encoded);
        values[5] += count;
        return encodeDateTime(values);
    }

    public static long encodeDate(int date) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.set(date);
        return encodeDate(t);
    }

    public static long encodeDate(String date) throws EncodedDateTimeException {
        HecTime t = new HecTime();
        t.set(date);
        return encodeDate(t);
    }

    public static long encodeDate(HecTime t) throws EncodedDateTimeException {
        t = new HecTime(t);
        t.showTimeAsBeginningOfDay(true);
        return encodeDate(new int[]{t.year(), t.month(), t.day()});
    }

    public static long encodeDate(int[] values) throws EncodedDateTimeException {
        normalizeDateTime(values);
        int len = values.length;
        int y = len > 0 ? values[0] : 0;
        int m = len > 1 ? values[1] : 1;
        int d = len > 2 ? values[2] : 1;
        return ((y * 100L + m) * 100 + d);
    }

    public static long changeTimeZone(long dateTIme, ZoneId fromZone, ZoneId toZone) throws EncodedDateTimeException {
        int[] values = toValues(dateTIme);
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
