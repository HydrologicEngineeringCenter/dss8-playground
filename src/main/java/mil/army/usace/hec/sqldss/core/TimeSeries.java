package mil.army.usace.hec.sqldss.core;

import com.google.common.flogger.FluentLogger;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static hec.lang.Const.UNDEFINED_DOUBLE;
import static mil.army.usace.hec.sqldss.core.Constants.*;
import static mil.army.usace.hec.sqldss.core.Constants.RECORD_TYPE.ITD;
import static mil.army.usace.hec.sqldss.core.Constants.RECORD_TYPE.RTD;
import static mil.army.usace.hec.sqldss.core.EncodedDateTime.changeTimeZone;

/**
 * Utility class to store and retrieve time series in SQLDSS
 */
public final class TimeSeries {

    /**
     * Prevent class instantiation
     */
    private TimeSeries() {
        throw new AssertionError("Cannot instantiate");
    }

    static FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     * Class to hold BLOB header information from time series records
     */
    static class TsvRecordHeader {
        RECORD_TYPE redordType;
        int version;
        int valueCount;
        boolean hasQuality;
        long firstTime;
        long[] times = null; // ITS only
    }

    /**
     * Class to hold information about time series records
     */
    static class TsvInfo {
        int valueCount;
        long firstTime;
        long lastTime;
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;
        long lastUpdate;
    }

    /**
     * Class to hold time series record information (used for merging data)
     */
    public static class TsvData {
        public long[] times = null;
        public double[] values = null;
        public int[] qualities = null;
        public int offset = 0;
        public int count = -1;
    }

    /**
     * Read the BLOB header information
     *
     * @param buf The buffer wrapping the BLOB in little-endian format
     * @return The header information
     * @throws SqlDssException If:
     *                       <ul>
     *                           <li>The record type is unknown or unexpected</li>
     *                           <li>The record type version is unexpected</li>
     *                       </ul>
     */
    @NotNull
    static TsvRecordHeader readHeader(@NotNull ByteBuffer buf) throws SqlDssException {
        int bufPosition;
        byte recordTypeCode;
        TsvRecordHeader header = new TsvRecordHeader();
        bufPosition = 0;
        recordTypeCode = buf.get(bufPosition);
        bufPosition += Byte.BYTES;
        try {
            header.redordType = RECORD_TYPE.fromCode(recordTypeCode);
        } catch (IllegalArgumentException e) {
            throw new SqlDssException(e);
        }
        switch (header.redordType) {
            case RTD:
                header.version = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (header.version != 1) {
                    throw new SqlDssException("Don't know how to decode RTS version " + header.version);
                }
                header.valueCount = buf.getInt(bufPosition);
                bufPosition += Integer.BYTES;
                header.hasQuality = buf.get(bufPosition) != 0;
                bufPosition += Byte.BYTES;
                header.firstTime = buf.getLong(bufPosition);
                bufPosition += Long.BYTES;
                buf.position(bufPosition);
                break;
            case ITD:
                throw new SqlDssException("Cannot yet decode ITS records");
            default:
                throw new SqlDssException(String.format(
                        "Expected data type of %d (%s) or %d (%s), got %d",
                        RTD.getCode(), RTD.name(), ITD.getCode(), ITD.name(), recordTypeCode));
        }
        return header;
    }

    /**
     * Retrieve a specified time series for a time window
     *
     * @param name        The time series name
     * @param startTime   The start of the time window
     * @param endTime     The end of the time window
     * @param trimMissing Whether to trim blocks of missing values from the beginning and end of the retrieved data
     * @param sqldss      The SQLDSS object to use
     * @return The retrieved time series
     * @throws SqlDssException          If thrown by {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws SQLException             If thrown by {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws IOException              If thrown by {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     */
    @NotNull
    public static TimeSeriesContainer retrieveTimeSeriesValues(@NotNull String name, long startTime, long endTime, boolean trimMissing,
                                                               @NotNull SqlDss sqldss) throws SqlDssException, SQLException,
            EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        String parameter = name.split("\\|", 3)[1];
        String unit = sqldss.getEffectiveRetrieveUnit(parameter);
        return retrieveTimeSeriesValues(name, startTime, endTime, trimMissing, unit, sqldss);
    }

    /**
     * Retrieve a specified time series for a time window in a specified unit
     *
     * @param name        The time series name
     * @param startTime   The start of the time window
     * @param endTime     The end of the time window
     * @param trimMissing Whether to trim blocks of missing values from the beginning and end of the retrieved data
     * @param unit        The unit to retrieve the time series in
     * @param sqldss      The SQLDSS object to use
     * @return The retrieved time series
     * @throws SqlDssException          If thrown by {@link #retrieveRegularTimeSeriesValues(String, long, long, String, SqlDss)} or {@link #retrieveIrregularTimeSeriesValues(String, long, long, String, SqlDss)}
     * @throws SQLException             If thrown by {@link #retrieveRegularTimeSeriesValues(String, long, long, String, SqlDss)} or {@link #retrieveIrregularTimeSeriesValues(String, long, long, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link #retrieveRegularTimeSeriesValues(String, long, long, String, SqlDss)} or {@link #retrieveIrregularTimeSeriesValues(String, long, long, String, SqlDss)}
     * @throws IOException              If thrown by {@link #retrieveRegularTimeSeriesValues(String, long, long, String, SqlDss)} or {@link #retrieveIrregularTimeSeriesValues(String, long, long, String, SqlDss)}
     */
    @NotNull
    public static TimeSeriesContainer retrieveTimeSeriesValues(String name, long startTime, long endTime, boolean trimMissing,
                                                               String unit, SqlDss sqldss) throws SqlDssException, SQLException,
            EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        if (isIrregular(name)) {
            tsc = retrieveIrregularTimeSeriesValues(name, startTime, endTime, unit, sqldss);
        } else {
            tsc = retrieveRegularTimeSeriesValues(name, startTime, endTime, unit, sqldss);
        }
        if (trimMissing) {
            trimTimeSeriesContainer(tsc);
        }
        return tsc;
    }

    /**
     * Retrieve a specified time series for its full time extents
     *
     * @param name        The time series name
     * @param trimMissing Whether to trim blocks of missing values from the beginning and end of the retrieved data
     * @param sqldss      The SQLDSS object to use
     * @return The retrieved time series
     * @throws SqlDssException            If thrown by {@link #retrieveAllTimeSeriesValues(String, boolean, String, SqlDss)}
     * @throws SQLException             If thrown by {@link #retrieveAllTimeSeriesValues(String, boolean, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link #retrieveAllTimeSeriesValues(String, boolean, String, SqlDss)}
     * @throws IOException              If thrown by {@link #retrieveAllTimeSeriesValues(String, boolean, String, SqlDss)}
     */
    public static @NotNull TimeSeriesContainer retrieveAllTimeSeriesValues(@NotNull String name, boolean trimMissing, @NotNull SqlDss sqldss) throws SqlDssException,
            SQLException, EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        String parameter = name.split("\\|", 3)[1];
        String unit = sqldss.getEffectiveRetrieveUnit(parameter);
        return retrieveAllTimeSeriesValues(name, trimMissing, unit, sqldss);
    }

    /**
     * Retrieve a specified time series for its full time extents in a specified unit
     *
     * @param name        The time series name
     * @param trimMissing Whether to trim blocks of missing values from the beginning and end of the retrieved data
     * @param unit        The unit to retrieve the time series in
     * @param sqldss      The SQLDSS object to use
     * @return The retrieved time series
     * @throws SqlDssException            If time series has no data or thrown by {@link #getTimeSeriesExtents(String, Long[], Connection)} or {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws SQLException             If thrown by {@link #getTimeSeriesExtents(String, Long[], Connection)} or {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link #getTimeSeriesExtents(String, Long[], Connection)} or {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws IOException              If thrown by {@link #getTimeSeriesExtents(String, Long[], Connection)} or {@link #retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     */
    @NotNull
    public static TimeSeriesContainer retrieveAllTimeSeriesValues(String name, boolean trimMissing, String unit, @NotNull SqlDss sqldss) throws SqlDssException,
            SQLException, EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        Long[] extents = new Long[2];
        getTimeSeriesExtents(name, extents, sqldss.getConnection());
        if (extents[0] == null) {
            throw new SqlDssException("No data for time series");
        }
        return retrieveTimeSeriesValues(name, extents[0], extents[1], true, unit, sqldss);
    }

    /**
     * Retrieve the first and last block start dates and the interval name for a time series
     *
     * @param key          The database key for the time series
     * @param blockExtents An array of at least two to hold the block start dates
     * @param intervalName An arrya of at least one to hold the interval name
     * @param conn         The JDBC connection
     * @throws SQLException  If SQL error
     * @throws SqlDssException If arrays are null or too small, or unexpected error getting last block start
     */
    static void getFirstLastBlockAndInterval(long key, Long[] blockExtents, String[] intervalName, Connection conn) throws SQLException, SqlDssException {
        if (blockExtents == null || blockExtents.length < 2) {
            throw new SqlDssException("Parameter 'blockExtents' must be of length 2");
        }
        if (intervalName == null || intervalName.length < 1) {
            throw new SqlDssException("Parameter 'intervalName' must be of length 1");
        }
        String sql = "select interval, min(block_start_date), max(block_start_date) from tsv where time_series = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                intervalName[0] = rs.getString("interval");
                blockExtents[0] = rs.getLong("max(block_start_date)");
                if (rs.wasNull()) {
                    return;
                }
                blockExtents[1] = rs.getLong("min(block_start_date");
                if (rs.wasNull()) {
                    throw new SqlDssException("Error getting first/last block dates");
                }
            }
        }
    }

    /**
     * Retrieve the last value time from a record header
     *
     * @param header          The (regular or irregular) time series record header
     * @param intervalMinutes The interval minutes of the time series interval (necessary for regular time series)
     * @return The last value time in the header
     * @throws EncodedDateTimeException If thrown by {@link EncodedDateTime#incrementEncodedDateTime(long, int, int)}
     */
    static long getLastTimeFromHeader(@NotNull TsvRecordHeader header, int intervalMinutes) throws EncodedDateTimeException {
        if (header.times == null) {
            // RTS
            return EncodedDateTime.incrementEncodedDateTime(
                    header.firstTime,
                    header.valueCount * intervalMinutes - 1,
                    1);
        } else {
            // ITS
            if (header.times.length > 1) {
                return header.times[header.times.length - 1] - 1;
            } else {
                return header.firstTime;
            }
        }
    }

    /**
     * Retrieve the time extents for a time series
     *
     * @param name    The time series name to retrieve the extents for
     * @param extents An array of length at least two to hold the extents
     * @param conn    The JDBC connection
     * @throws SqlDssException            If:
     *                                  <ul>
     *                                      <li><code>extents</code> is null or too small</li>
     *                                      <li>time series <code>name</code> is not found</li>
     *                                      <li>thrown by {@link #getTimeSeriesSpecKey(String, Connection)} or {@link #getFirstLastBlockAndInterval(long, Long[], String[], Connection)}</li>
     *                                  </ul>
     * @throws SQLException             If SQL error
     * @throws EncodedDateTimeException If thrown by {@link #getLastTimeFromHeader(TsvRecordHeader, int)}
     */
    public static void getTimeSeriesExtents(String name, Long[] extents, Connection conn) throws SqlDssException,
            SQLException, EncodedDateTimeException {
        if (extents == null || extents.length < 2) {
            throw new SqlDssException("Parameter 'extents' must be of length 2 or more");
        }
        extents[0] = extents[1] = null;
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            throw new SqlDssException("No such time series: " + name);
        }
        Long[] blockExtentsArr = new Long[2];
        String[] intervalNameArr = new String[1];
        getFirstLastBlockAndInterval(key, blockExtentsArr, intervalNameArr, conn);
        byte[] blob;
        String sql = "select data from tsv where key = ? and block_start_date = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, key);
            for (int i = 0; i < 2; ++i) {
                ps.setLong(2, blockExtentsArr[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    blob = rs.getBytes("data");
                }
                ByteBuffer buf = ByteBuffer.wrap(blob);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                TsvRecordHeader header = readHeader(buf);
                int intervalMinutes = Interval.getIntervalMinutes(intervalNameArr[0]);
                if (i == 0) {
                    extents[0] = header.firstTime;
                } else {
                    extents[1] = getLastTimeFromHeader(header, intervalMinutes);
                }
            }
        }
    }


    /**
     * Retrieves an irregular time series for a time window and in a specified unit
     *
     * @param name      The time series to retrieve
     * @param startTime The start of the time window
     * @param endTime   The end of the time window
     * @param unit      The unit to retrieve the values in
     * @param sqldss    The SQLDSS object
     * @return The irregular time series
     * @throws SqlDssException            If:
     *                                  <ul>
     *                                      <li>time series <code>name</code> is not found</li>
     *                                      <li>time series <code>name</code> is deleted</li>
     *                                  </ul>
     * @throws SQLException             If SQL error
     * @throws EncodedDateTimeException In thrown by EncodedDateTime method
     */
    @NotNull
    static TimeSeriesContainer retrieveIrregularTimeSeriesValues(@NotNull String name, long startTime, long endTime,
                                                                 String unit, @NotNull SqlDss sqldss) throws SqlDssException, SQLException,
            EncodedDateTimeException, IOException {
        throw new SqlDssException("Cannot yet retrieve irregular time series");
    }

    /**
     * Retrieves a regular time series for a time window and in a specified unit
     *
     * @param name      The time series to retrieve
     * @param startTime The start of the time window
     * @param endTime   The end of the time window
     * @param unit      The unit to retrieve the values in
     * @param sqldss    The SQLDSS object
     * @return The regular time series
     * @throws SqlDssException            If:
     *                                  <ul>
     *                                      <li>time series <code>name</code> is not found</li>
     *                                      <li>time series <code>name</code> is deleted</li>
     *                                      <li>there is a problem with the interval offset for time series <code>name</code>
     *                                      <ul>
     *                                          <li>the time series interval offset is not set or is invalid</li>
     *                                          <li>time computed interval offset in one or more records differs from the time series interval offset</li>
     *                                      </ul>
     *                                      </li>
     *                                      <li>the record type or record type version of one or more records is unexpected</li>
     *                                  </ul>
     * @throws SQLException             If SQL error
     * @throws EncodedDateTimeException In thrown by EncodedDateTime method
     */
    @NotNull
    static TimeSeriesContainer retrieveRegularTimeSeriesValues(@NotNull String name, long startTime, long endTime,
                                                               String unit, @NotNull SqlDss sqldss) throws SqlDssException, SQLException,
            EncodedDateTimeException {
        Connection conn = sqldss.getConnection();
        HecTime startHecTime = EncodedDateTime.toHecTime(startTime);
        HecTime endHecTime = EncodedDateTime.toHecTime(endTime);
        TimeSeriesContainer tsc = new TimeSeriesContainer();
        String[] nameParts = name.split("\\|", -1);
        String locationName = nameParts[0];
        String context = "";
        if (locationName.indexOf(':') != -1) {
            String[] parts = locationName.split(":", 2);
            context = parts[0];
            locationName = parts[1];
        }
        String baseLocationName = locationName;
        String subLocationName = "";
        if (locationName.indexOf('-') != -1) {
            String[] parts = locationName.split("-", 2);
            baseLocationName = parts[0];
            subLocationName = parts[1];
        }
        String parameterName = nameParts[1];
        String baseParameterName = parameterName;
        String subParameterName = "";
        if (parameterName.indexOf('-') != -1) {
            String[] parts = parameterName.split("-", 2);
            baseParameterName = parts[0];
            subParameterName = parts[1];
        }
        String paramTypeName = nameParts[2];
        String intervalName = nameParts[3];
        String versionName = nameParts[5];
        tsc.setFullName(String.format(name));
        tsc.watershed = context;
        tsc.location = baseLocationName;
        tsc.subLocation = subLocationName;
        tsc.parameter = baseParameterName;
        tsc.subParameter = subParameterName;
        tsc.version = versionName;
        tsc.type = paramTypeName;
        // get the units
        try (PreparedStatement ps = conn.prepareStatement(
                "select default_si_unit from base_parameter where name = ?"
        )) {
            ps.setString(1, baseParameterName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                tsc.units = rs.getString("default_si_unit");
            }
        }

        // get the time series spec key
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            throw new SqlDssException("No such time series: " + name);
        }
        // get the interval and offset
        String existingOffsetStr = null;
        int intervalMinutes;
        int existingOffsetMinutes;
        try (PreparedStatement ps = conn.prepareStatement("select deleted, interval, interval_offset from time_series where " +
                "key = ?")) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getLong("deleted") == 1) {
                    throw new SqlDssException("No such time series: " + name);
                }
                intervalName = rs.getString("interval");
                existingOffsetStr = rs.getString("interval_offset");
            }
        }
        if (existingOffsetStr == null || existingOffsetStr.isEmpty()) {
            throw new SqlDssException("Interval offset is not set for time series!");
        }
        intervalMinutes = Interval.getIntervalMinutes(intervalName);
        if (intervalMinutes == 0) {
            throw new SqlDssException("Error getting interval minutes for " + name);
        }
        tsc.interval = intervalMinutes;
        existingOffsetMinutes = Duration.iso8601ToMinutes(existingOffsetStr);
        // determine blocks
        long[] encodedBlockDates = getBlockStartDates(startHecTime, endHecTime, intervalName);
        byte[] blob;
        int[][] timeArrays = new int[encodedBlockDates.length - 1][];
        double[][] valueArrays = new double[encodedBlockDates.length - 1][];
        int[][] qualityArrays = new int[encodedBlockDates.length - 1][];
        // get each block
        for (int i = 0; i < encodedBlockDates.length - 1; ++i) {

            try (PreparedStatement ps = conn.prepareStatement(String.format(SQL_SELECT_TS_BLOCK, key))) {
                ps.setLong(1, encodedBlockDates[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    blob = rs.getLong("deleted") == 1 ? null : rs.getBytes("data");
                }
            }
            if (blob == null) {
                // create missing data for the record
                long encodedFirstTime = EncodedDateTime.toEncodedDateTime(encodedBlockDates[i]);
                long encodedLastTime = EncodedDateTime.addMinutes(
                        EncodedDateTime.toEncodedDateTime(encodedBlockDates[i + 1]),
                        -intervalMinutes);
                int blockValueCount = EncodedDateTime.intervalsBetween(
                        encodedFirstTime,
                        encodedLastTime,
                        intervalMinutes
                ) + 1;
                int firstValueOffset = -1;
                int lastValueOffset = -1;
                if (startTime <= encodedFirstTime) {
                    firstValueOffset = 0;
                } else {
                    firstValueOffset = EncodedDateTime.intervalsBetween(
                            encodedFirstTime,
                            startTime,
                            intervalMinutes);
                }
                if (endTime >= encodedLastTime) {
                    lastValueOffset = blockValueCount - 1;
                } else {
                    lastValueOffset = blockValueCount - EncodedDateTime.intervalsBetween(
                            endTime,
                            encodedLastTime,
                            intervalMinutes) - 1;
                }
                int valueCount = lastValueOffset - firstValueOffset + 1;
                timeArrays[i] = new int[valueCount];
                valueArrays[i] = new double[valueCount];
                qualityArrays[i] = new int[valueCount];
                HecTime t = EncodedDateTime.toHecTime(encodedFirstTime);
                if (intervalMinutes < MONTH_MINUTES) {
                    int minutes = t.value();
                    for (int j = 0; j < valueCount; ++j) {
                        timeArrays[i][j] = minutes;
                        minutes += intervalMinutes;
                    }
                } else {
                    for (int j = 0; j < valueCount; ++j) {
                        timeArrays[i][j] = t.value();
                        t.increment(1, intervalMinutes);
                    }
                }
                Arrays.fill(valueArrays[i], UNDEFINED_DOUBLE);
            } else {
                // read the record
                ByteBuffer buf = ByteBuffer.wrap(blob);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                int bufPosition = 0;
                byte dataType = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (dataType != RTD.getCode()) {
                    throw new SqlDssException(String.format(
                            "Expected data type of %d (%s), got %d",
                            RTD.getCode(),
                            RTD.name(),
                            dataType));
                }
                byte dataTypeVersion = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (dataTypeVersion != 1) {
                    throw new SqlDssException("Don't know how to decode RTD version " + dataTypeVersion);
                }
                int blockValueCount = buf.getInt(bufPosition);
                bufPosition += Integer.BYTES;
                byte hasQuality = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                long encodedFirstTime = buf.getLong(bufPosition);
                bufPosition += Long.BYTES;
                long encodedLastTime = EncodedDateTime.incrementEncodedDateTime(
                        encodedFirstTime,
                        intervalMinutes,
                        blockValueCount - 1);
                int firstValueOffset = -1;
                int lastValueOffset = -1;
                if (startTime <= encodedFirstTime) {
                    firstValueOffset = 0;
                } else {
                    firstValueOffset = EncodedDateTime.intervalsBetween(
                            encodedFirstTime,
                            startTime,
                            intervalMinutes);
                }
                if (endTime >= encodedLastTime) {
                    lastValueOffset = blockValueCount - 1;
                } else {
                    lastValueOffset = blockValueCount - EncodedDateTime.intervalsBetween(
                            endTime,
                            encodedLastTime,
                            intervalMinutes) - 1;
                }
                int valueCount = lastValueOffset - firstValueOffset + 1;
                if (firstValueOffset > 0) {
                    encodedFirstTime = EncodedDateTime.incrementEncodedDateTime(encodedFirstTime, intervalMinutes, firstValueOffset);
                }
                HecTime firstTime = EncodedDateTime.toHecTime(encodedFirstTime);
                HecTime intervalTime = new HecTime(firstTime);
                intervalTime.adjustToIntervalOffset(intervalMinutes, 0);
                if (intervalTime.greaterThan(firstTime)) {
                    intervalTime.subtractMinutes(intervalMinutes);
                }
                int thisOffset = (int) ((firstTime.getTimeInMillis() - intervalTime.getTimeInMillis()) / 60000);
                if (thisOffset != existingOffsetMinutes) {
                    throw new SqlDssException(String.format(
                            "Interval offset for block starting at %d (%d) doesn't match offset for time series (%d)",
                            encodedBlockDates[i], thisOffset, existingOffsetMinutes));
                }
                double[] values = new double[valueCount];
                bufPosition += firstValueOffset * Double.BYTES;
                for (int j = 0; j < valueCount; ++j) {
                    values[j] = buf.getDouble(bufPosition);
                    bufPosition += Double.BYTES;
                }
                int[] qualities = new int[valueCount];
                if (hasQuality != 0) {
                    bufPosition += (valueCount - lastValueOffset - 1) * Double.BYTES;
                    for (int j = 0; j < valueCount; ++j) {
                        qualities[j] = buf.getInt(bufPosition);
                        bufPosition += Integer.BYTES;
                    }
                }
                timeArrays[i] = new int[valueCount];
                valueArrays[i] = new double[valueCount];
                qualityArrays[i] = new int[valueCount];
                if (intervalMinutes < MONTH_MINUTES) {
                    int minutes = firstTime.value();
                    for (int j = 0; j < valueCount; ++j) {
                        timeArrays[i][j] = minutes;
                        minutes += intervalMinutes;
                        valueArrays[i][j] = values[j];
                        qualityArrays[i][j] = qualities[j];
                    }
                } else {
                    HecTime t = new HecTime(firstTime);
                    for (int j = 0; j < valueCount; ++j) {
                        timeArrays[i][j] = t.value();
                        t.increment(1, intervalMinutes);
                        valueArrays[i][j] = values[j];
                        qualityArrays[i][j] = qualities[j];
                    }
                }
            }
        }
        // concatenate arrays
        int size = 0;
        for (int[] a : timeArrays) size += a.length;
        int[] times = new int[size];
        double[] values = new double[size];
        int[] qualities = new int[size];
        int count = 0;
        for (int i = 0; i < timeArrays.length; ++i) {
            if (i > 0) {
                if (intervalMinutes < 30 * DAY_MINUTES) {
                    int nextTime = times[count - 1] + intervalMinutes;
                    for (; nextTime < timeArrays[i][0]; nextTime += intervalMinutes, ++count) {
                        if (count == size) {
                            size *= 2;
                            times = Arrays.copyOf(times, size);
                            values = Arrays.copyOf(values, size);
                            qualities = Arrays.copyOf(qualities, size);
                        }
                        times[count] = nextTime;
                        values[count] = UNDEFINED_DOUBLE;
                        qualities[count] = 0;
                    }
                } else {
                    HecTime nextTime = new HecTime();
                    for (nextTime.set(times[count - 1]); nextTime.value() < timeArrays[i][0]; nextTime.increment(1,
                            intervalMinutes), ++count) {
                        if (count == size) {
                            size *= 2;
                            times = Arrays.copyOf(times, size);
                            values = Arrays.copyOf(values, size);
                            qualities = Arrays.copyOf(qualities, size);
                        }
                        times[count] = nextTime.value();
                        values[count] = UNDEFINED_DOUBLE;
                        qualities[count] = 0;
                    }
                }
            }
            for (int j = 0; j < timeArrays[i].length; ++j, ++count) {
                if (count == size) {
                    size *= 2;
                    times = Arrays.copyOf(times, size);
                    values = Arrays.copyOf(values, size);
                    qualities = Arrays.copyOf(qualities, size);
                }
                times[count] = timeArrays[i][j];
                values[count] = valueArrays[i][j];
                qualities[count] = qualityArrays[i][j];
            }
        }
        // populate TimeSeriesContainer
        if (count == size) {
            tsc.times = times;
            tsc.setValues(values);
            tsc.setQuality(qualities);
        } else {
            tsc.times = Arrays.copyOf(times, count);
            tsc.setValues(Arrays.copyOf(values, count));
            tsc.setQuality(Arrays.copyOf(qualities, count));
        }
        tsc.numberValues = count;
        tsc.setStartTime(startHecTime);
        tsc.setEndTime(endHecTime);
        if (unit != null && !unit.equals(tsc.units)) {
            Unit.convertUnits(tsc, unit, conn);
        }
        return tsc;
    }

    /**
     * Store time series to the database using a specified store ruls
     * @param tsc The time series to store
     * @param storeRule The store rule to use
     * @param sqldss The SqlDss object
     * @throws SqlDssException If thrown by {@link Interval#getBlockSizeMinutes },
     *      {@link #storeIrregularTimeSeriesValues(TimeSeriesContainer, IRREGULAR_STORE_RULE, Connection)}, or
     *      {@link #storeRegularTimeSeriesValues(TimeSeriesContainer, REGULAR_STORE_RULE, Connection)}
     * @throws SQLException If thrown by {@link #storeIrregularTimeSeriesValues(TimeSeriesContainer, IRREGULAR_STORE_RULE, Connection)} or
     *      {@link #storeRegularTimeSeriesValues(TimeSeriesContainer, REGULAR_STORE_RULE, Connection)}
     * @throws EncodedDateTimeException If thrown by {@link #storeIrregularTimeSeriesValues(TimeSeriesContainer, IRREGULAR_STORE_RULE, Connection)} or
     *      {@link #storeRegularTimeSeriesValues(TimeSeriesContainer, REGULAR_STORE_RULE, Connection)}
     */
    public static void storeTimeSeriesValues(@NotNull TimeSeriesContainer tsc, String storeRule, SqlDss sqldss) throws SqlDssException, SQLException, EncodedDateTimeException {
        String name = tsc.fullName;
        String[] parts = name.split("\\|", -1);
        String intervalName = parts[3];
        int intervalMinutes = Interval.getIntervalMinutes(intervalName);
        if (intervalMinutes == 0) {
            IRREGULAR_STORE_RULE sr = IRREGULAR_STORE_RULE.valueOf(storeRule.toUpperCase());
            storeIrregularTimeSeriesValues(tsc, sr, sqldss.getConnection());
        } else {
            REGULAR_STORE_RULE sr = REGULAR_STORE_RULE.valueOf(storeRule.toUpperCase());
            storeRegularTimeSeriesValues(tsc, sr, sqldss.getConnection());
        }
    }

    /**
     * Return whether a time series name represents an irregular time series
     * @param name The time series name
     * @return Whether the name represents an irregular time series
     * @throws SqlDssException If thrown by {@link Interval#getIntervalMinutes(String)}
     */
    static boolean isIrregular(@NotNull String name) throws SqlDssException {
        String[] parts = name.split("\\|", -1);
        String intervalName = parts[3];
        int minutes = Interval.getIntervalMinutes(intervalName);
        return minutes == 0;
    }

    /**
     * Populates a {@link TimeSeries.TsvInfo} object
     * @param blockInfo The TsvInfo object to populate
     * @param count The value count
     * @param firstTime The first value time in the record
     * @param lastTime The last value time in the record
     * @param values The time series values
     * @param qualities The time series qualities, if any
     * @param valueOffset The offset into the values and qualities of the first value for the record
     */
    static void populateTsvInfo(
            @NotNull TsvInfo blockInfo,
            int count,
            long firstTime,
            long lastTime,
            double @NotNull [] values,
            int[] qualities,
            int valueOffset
    ) {
        blockInfo.valueCount = count;
        blockInfo.firstTime = firstTime;
        blockInfo.lastTime = lastTime;
        blockInfo.minValue = Double.MAX_VALUE;
        blockInfo.maxValue = Double.MIN_VALUE;
        if (qualities == null) {
            for (int i = valueOffset; i < valueOffset + count; ++i) {
                if (values[i] != UNDEFINED_DOUBLE) {
                    if (values[i] < blockInfo.minValue) {
                        blockInfo.minValue = values[i];
                    }
                    if (values[i] > blockInfo.maxValue) {
                        blockInfo.maxValue = values[i];
                    }
                }
            }
        } else {
            for (int i = valueOffset; i < valueOffset + count; ++i) {
                if (values[i] != UNDEFINED_DOUBLE
                        && (qualities[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                        && (qualities[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE) {
                    if (values[i] < blockInfo.minValue) {
                        blockInfo.minValue = values[i];
                    }
                    if (values[i] > blockInfo.maxValue) {
                        blockInfo.maxValue = values[i];
                    }
                }
            }
        }
        blockInfo.lastUpdate = System.currentTimeMillis();
    }

    /**
     * Store irregular interval time series values
     * @param tsc The time series to store
     * @param storeRule The store rule to use
     * @param conn The JDBC connection
     * @throws SqlDssException If problem with time series name, etc...
     * @throws SQLException If SQL error
     * @throws EncodedDateTimeException Tf thrown by an {@link EncodedDateTime} method
     */
    static void storeIrregularTimeSeriesValues(
            @NotNull TimeSeriesContainer tsc,
            IRREGULAR_STORE_RULE storeRule,
            Connection conn
    ) throws SqlDssException, SQLException, EncodedDateTimeException {
        throw new SqlDssException("Cannot yet store irregular time series");
    }

    /**
     * Store regular interval time series values
     * @param tsc The time series to store
     * @param storeRule The store rule to use
     * @param conn The JDBC connection
     * @throws SqlDssException If problem with time series name, interval, etc...
     * @throws SQLException If SQL error
     * @throws EncodedDateTimeException Tf thrown by an {@link EncodedDateTime} method
     */
    static void storeRegularTimeSeriesValues(
            @NotNull TimeSeriesContainer tsc,
            REGULAR_STORE_RULE storeRule,
            Connection conn
    ) throws SqlDssException, SQLException, EncodedDateTimeException {
        // parse the name
        String[] parts = tsc.fullName.split("\\|", -1);
        String intervalName = parts[3];
        // verify the interval
        int intervalMinutes = tsc.getTimeIntervalSeconds() / 60;
        if (intervalMinutes < MONTH_MINUTES) {
            for (int i = 1; i < tsc.numberValues; ++i) {
                if (tsc.times[i] - tsc.times[i - 1] != intervalMinutes) {
                    throw new SqlDssException("Time series is not regular interval");
                }
            }
        }
        else {
            HecTime t = new HecTime(tsc.getStartTime());
            for (int i = 0; ; t.increment(1, intervalMinutes), ++i) {
                if (t.value() != tsc.times[i]) {
                    throw new SqlDssException("Time series is not regular interval");
                }
            }
        }
        // get time zones for conversion
        ZoneId fromZone = null;
        ZoneId toZone = null;
        if (tsc.timeZoneID != null && !tsc.timeZoneID.equals("UTC")) {
            fromZone = ZoneId.of(tsc.timeZoneID);
            toZone = ZoneId.of("UTC");
        }
        // get the unit conversion
        double[] unitConvFactor = new double[1];
        double[] unitConvOffset = new double[1];
        String[] unitConvFunction = new String[1];
        Unit.getUnitConverisonForStoring(
                tsc.units,
                tsc.parameter,
                unitConvFactor,
                unitConvOffset,
                unitConvFunction,
                conn);
        boolean mustConvert = (
                unitConvFunction[0] != null && !unitConvFunction[0].isEmpty()) ||
                !(unitConvFactor[0] == 1.0 && unitConvOffset[0] == 0.);
        // store the time series spec or get the key if already exists
        long key = putTimeSeriesSpec(tsc.fullName, conn);
        // verify interval offset against database
        String existingOffsetStr = null;
        HecTime t = new HecTime(tsc.getStartTime());
        t.adjustToIntervalOffset(intervalMinutes, 0);
        int tscOffsetMinutes = (int) ((t.getTimeInMillis() - tsc.getStartTime().getTimeInMillis()) / 60000L);
        int existingOffsetMinutes = -1;
        try (PreparedStatement ps = conn.prepareStatement("select interval_offset from time_series where key = ?")) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                existingOffsetStr = rs.getString("interval_offset");
            }
        }
        if (existingOffsetStr != null && !existingOffsetStr.isEmpty()) {
            existingOffsetMinutes = Duration.iso8601ToMinutes(existingOffsetStr);
            if (tscOffsetMinutes != existingOffsetMinutes) {
                throw new SqlDssException("Expected interval offset of " + existingOffsetMinutes + ", got " + tscOffsetMinutes);
            }
        }
        // prepare arrays
        long[] encodedTimes = new long[tsc.numberValues];
        for (int i = 0; i < tsc.numberValues; ++i) {
            encodedTimes[i] = EncodedDateTime.encodeDateTime(tsc.times[i]);
            if (fromZone != null) {
                encodedTimes[i] = changeTimeZone(encodedTimes[i], fromZone, toZone);
            }
        }
        double[] values = Arrays.copyOf(tsc.values, tsc.numberValues);
        int[] qualities = null;
        if (tsc.quality != null && Arrays.stream(tsc.quality).anyMatch(q -> q != 0)) {
            qualities = Arrays.copyOf(tsc.quality, tsc.numberValues);
        }
        // determine blocks
        long[] encodedBlockDates = getBlockStartDates(tsc.startHecTime, tsc.endHecTime, intervalName);
        long[] encodedBlockTimes = Arrays.stream(encodedBlockDates).map(EncodedDateTime::toEncodedDateTime).toArray();
        int[] blockStarts = new int[encodedBlockTimes.length];
        int[] blockCounts = new int[encodedBlockTimes.length];
        blockStarts[0] = 0;
        blockCounts[0] = 0;
        for (int i = 0, j = 0; i < tsc.numberValues; ++i) {
            if (encodedTimes[i] >= encodedBlockTimes[j + 1]) {
                blockStarts[++j] = i;
                blockCounts[j] = 0;
            }
            blockCounts[j]++;
        }
        // store the time series values
        boolean deleted;
        byte[] blob;
        byte format = (byte) RTD.getCode();
        byte version = 1;
        byte hasQuality = 0;
        for (int i = 0; i < encodedBlockDates.length - 1; ++i) {
            // retrieve any existing blob for the start date
            try (PreparedStatement ps = conn.prepareStatement(String.format(SQL_SELECT_TS_BLOCK, key))) {
                ps.setLong(1, encodedBlockDates[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    deleted = rs.getLong("deleted") == 1;
                    blob = rs.getBytes("data");
                }
            }
            long firstTime = EncodedDateTime.encodeDateTime(tsc.times[blockStarts[i]]);
            TsvInfo blockInfo = new TsvInfo();
            if (deleted || blob == null) {
                //------------------------------------//
                // record is deleted or doesn't exist //
                //------------------------------------//
                // create the blob
                int size = Byte.BYTES                       // data type
                        + Byte.BYTES                            // data type version
                        + Integer.BYTES                         // value count
                        + Byte.BYTES                            // has quality?
                        + Long.BYTES                            // date/time of first value
                        + blockCounts[i] * Double.BYTES;        // values
                if (tsc.quality != null) {
                    for (int j = 0; j < tsc.numberValues; ++j) {
                        if (tsc.quality[j] != 0) {
                            size += blockCounts[i] * Integer.BYTES; // quality codes
                            hasQuality = 1;
                            break;
                        }
                    }
                }
                populateTsvInfo(
                        blockInfo,
                        blockCounts[i],
                        firstTime,
                        EncodedDateTime.incrementEncodedDateTime(firstTime, intervalMinutes, blockCounts[i] - 1),
                        tsc.values,
                        tsc.quality,
                        blockStarts[i]
                );
                ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                buf.put(format);
                buf.put(version);
                buf.putInt(blockCounts[i]);
                buf.put(hasQuality);
                buf.putLong(firstTime);
                for (int j = blockStarts[i]; j < blockStarts[i] + blockCounts[i]; ++j) {
                    double value = tsc.values[j];
                    int quality = hasQuality == 1 ? tsc.quality[j] : 0;
                    if (mustConvert
                            && value != UNDEFINED_DOUBLE
                            && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                            && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE) {
                        value = Unit.performConversion(value, unitConvFactor[0], unitConvOffset[0], unitConvFunction[0]);
                    }
                    buf.putDouble(value);
                }
                if (hasQuality != 0) {
                    for (int j = blockStarts[i]; j < blockStarts[i] + blockCounts[i]; ++j) {
                        buf.putInt(tsc.quality[j]);
                    }
                }
                blob = buf.array();
                if (deleted) {
                    // overwrite the existing the blob
                    try (PreparedStatement ps = conn.prepareStatement(
                            "update tsv set deleted = 0, data=? where time_series=? and block_start_date=?"
                    )) {
                        ps.setBytes(1, blob);
                        ps.setLong(2, key);
                        ps.setLong(3, encodedBlockDates[i]);
                        ps.executeUpdate();
                    }
                    // overwrite the existing the block info
                    try (PreparedStatement ps = conn.prepareStatement(
                            """
                                    update tsv_info
                                       set value_count = ?,
                                           first_time  = ?,
                                           last_time   = ?,
                                           min_value   = ?,
                                           max_value   = ?,
                                           last_update = ?
                                     where time_series = ?
                                       and block_start_date = ?"""
                    )) {
                        ps.setLong(1, blockInfo.valueCount);
                        ps.setLong(2, blockInfo.firstTime);
                        ps.setLong(3, blockInfo.lastTime);
                        ps.setDouble(4, blockInfo.minValue);
                        ps.setDouble(5, blockInfo.maxValue);
                        ps.setLong(6, blockInfo.lastUpdate);
                        ps.setLong(7, key);
                        ps.setLong(8, encodedBlockDates[i]);
                        ps.executeUpdate();
                    }
                }
                else {
                    // insert the blob
                    try (PreparedStatement ps = conn.prepareStatement("""
                            insert
                              into tsv
                                   (time_series,
                                    block_start_date,
                                    deleted,
                                    data
                                   )
                            values (?, ?, 0, ?)""" 
                    )) {
                        ps.setLong(1, key);
                        ps.setLong(2, encodedBlockDates[i]);
                        ps.setBytes(3, blob);
                        ps.executeUpdate();
                    }
                    // insert the block info
                    try (PreparedStatement ps = conn.prepareStatement("""
                            insert
                             into tsv_info
                                  (time_series,
                                   block_start_date,
                                   value_count,
                                   first_time,
                                   last_time,
                                   min_value,
                                   max_value,
                                   last_update
                                  )
                           values (?, ?, ?, ? , ?, ?, ?, ?)"""
                    )) {
                        ps.setLong(1, key);
                        ps.setLong(2, encodedBlockDates[i]);
                        ps.setLong(3, blockInfo.valueCount);
                        ps.setLong(4, blockInfo.firstTime);
                        ps.setLong(5, blockInfo.lastTime);
                        ps.setDouble(6, blockInfo.minValue);
                        ps.setDouble(7, blockInfo.maxValue);
                        ps.setLong(8, blockInfo.lastUpdate);
                        ps.executeUpdate();
                    }
                }
            }
            else {
                //-------------------//
                // record does exist //
                //-------------------//
                // retrieve the existing data
                HecTime firstIncomingTime = new HecTime();
                firstIncomingTime.set(tsc.times[blockStarts[i]]);
                HecTime intervalTime = new HecTime(firstIncomingTime);
                if (intervalTime.greaterThan(firstIncomingTime)) {
                    intervalTime.subtractMinutes(intervalMinutes);
                }
                int incomingOffset =
                        (int) ((intervalTime.getTimeInMillis() - firstIncomingTime.getTimeInMillis()) / 60000);
                ByteBuffer buf = ByteBuffer.wrap(blob);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                int bufPosition = 0;
                byte dataType = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (dataType != RTD.getCode()) {
                    throw new SqlDssException(String.format(
                            "Expected data type of %d (%s), got %d",
                            RTD.getCode(),
                            RTD.name(),
                            dataType));
                }
                byte dataTypeVersion = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (dataTypeVersion != 1) {
                    throw new SqlDssException("Don't know how to decode RTD version " + dataTypeVersion);
                }
                int valueCount = buf.getInt(bufPosition);
                bufPosition += Integer.BYTES;
                hasQuality = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                long encodedFirstTime = buf.getLong(bufPosition);
                bufPosition += Long.BYTES;
                HecTime firstExistingTime = EncodedDateTime.toHecTime(encodedFirstTime);
                intervalTime = new HecTime(firstExistingTime);
                intervalTime.adjustToIntervalOffset(intervalMinutes, 0);
                if (intervalTime.greaterThan(firstExistingTime)) {
                    intervalTime.subtractMinutes(intervalMinutes);
                }
                int existingOffset =
                        (int) ((firstExistingTime.getTimeInMillis() - intervalTime.getTimeInMillis()) / 60000);
                if (existingOffset != incomingOffset) {
                    throw new SqlDssException(String.format(
                            "Incoming interval offset (%d) differs from existing interval offset (%d)",
                            incomingOffset,
                            existingOffset
                    ));
                }
                double[] existingValues = new double[valueCount];
                for (int j = 0; j < valueCount; ++j) {
                    existingValues[j] = buf.getDouble(bufPosition);
                    bufPosition += Double.BYTES;
                }
                int[] existingQualities = new int[valueCount];
                if (hasQuality != 0) {
                    for (int j = 0; j < valueCount; ++j) {
                        existingQualities[j] = buf.getInt(bufPosition);
                        bufPosition += Integer.BYTES;
                    }
                }
                long[] existingEncodedTimes = EncodedDateTime.makeRegularEncodedDateTimeArray(encodedFirstTime,
                        valueCount, intervalMinutes);
                // merge the data according to the store rule
                TsvData incoming = new TsvData();
                incoming.times = encodedTimes;
                incoming.values = values;
                incoming.qualities = qualities;
                incoming.offset = blockStarts[i];
                incoming.count = blockCounts[i];
                TsvData existing = new TsvData();
                existing.times = existingEncodedTimes;
                existing.values = existingValues;
                existing.qualities = existingQualities;
                existing.offset = 0;
                existing.count = existingValues.length;
                TsvData merged = new TsvData();
                if (mustConvert) {
                    if (incoming.qualities == null) {
                        for (int j = incoming.offset; j < incoming.offset + incoming.count; ++j) {
                            if (incoming.values[j] != UNDEFINED_DOUBLE) {
                                incoming.values[j] = Unit.performConversion(
                                        incoming.values[j], unitConvFactor[0],
                                        unitConvOffset[0],
                                        unitConvFunction[0]);
                            }
                        }
                    }
                    else {
                        for (int j = incoming.offset; j < incoming.offset + incoming.count; ++j) {
                            if (incoming.values[j] != UNDEFINED_DOUBLE
                                    && (incoming.qualities[j] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                                    && (incoming.qualities[j] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE) {
                                incoming.values[j] = Unit.performConversion(
                                        incoming.values[j], unitConvFactor[0],
                                        unitConvOffset[0],
                                        unitConvFunction[0]);
                            }
                        }
                    }
                }
                mergeTimeSeries(
                        intervalMinutes,
                        storeRule,
                        incoming,
                        existing,
                        merged
                );
                // create a new blob from the merged data
                int count = merged.count;
                hasQuality = 0;
                if (merged.qualities != null) {
                    for (int j = 0; j < merged.count; ++j) {
                        if (merged.qualities[j] != 0) {
                            hasQuality = 1;
                            break;
                        }
                    }
                }
                populateTsvInfo(
                        blockInfo,
                        count,
                        merged.times[0],
                        merged.times[count - 1],
                        merged.values,
                        merged.qualities,
                        0
                );
                int size = Byte.BYTES        // data type
                        + Byte.BYTES             // data type version
                        + Integer.BYTES          // value count
                        + Byte.BYTES             // has quality?
                        + Long.BYTES             // date/time of first value
                        + count * Double.BYTES;  // values
                if (hasQuality == 1) {
                    size += count * Integer.BYTES;
                }
                buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                buf.put(format);
                buf.put(version);
                buf.putInt(count);
                buf.put(hasQuality);
                buf.putLong(merged.times[0]);
                for (int j = 0; j < count; ++j) {
                    buf.putDouble(merged.values[j]);
                }
                if (hasQuality == 1) {
                    for (int j = 0; j < count; ++j) {
                        buf.putInt(merged.qualities[j]);
                    }
                }
                blob = buf.array();
                // overwrite the existing the blob
                try (PreparedStatement ps = conn.prepareStatement(
                        "update tsv set data=? where time_series=? and block_start_date=?"
                )) {
                    ps.setBytes(1, blob);
                    ps.setLong(2, key);
                    ps.setLong(3, encodedBlockDates[i]);
                    ps.executeUpdate();
                }
                // overwrite the existing the block info
                try (PreparedStatement ps = conn.prepareStatement(
                        """
                                update tsv_info
                                   set value_count = ?,
                                       first_time  = ?,
                                       last_time   = ?,
                                       min_value   = ?,
                                       max_value   = ?,
                                       last_update = ?
                                 where time_series = ?
                                   and block_start_date = ?"""
                )) {
                    ps.setLong(1, blockInfo.valueCount);
                    ps.setLong(2, blockInfo.firstTime);
                    ps.setLong(3, blockInfo.lastTime);
                    ps.setDouble(4, blockInfo.minValue);
                    ps.setDouble(5, blockInfo.maxValue);
                    ps.setLong(6, blockInfo.lastUpdate);
                    ps.setLong(7, key);
                    ps.setLong(8, encodedBlockDates[i]);
                    ps.executeUpdate();
                }
            }
            if (existingOffsetMinutes == -1) {
                try (PreparedStatement ps = conn.prepareStatement("update time_series set interval_offset = ? where " +
                        "key = ?")) {
                    ps.setString(1, Duration.minutesToIso8601(tscOffsetMinutes));
                    ps.setLong(2, key);
                    ps.executeUpdate();
                    existingOffsetMinutes = tscOffsetMinutes;
                }
            }
        }
        if (encodedBlockDates.length > 1) {
            String sql = "update time_series set deleted = 0 where key = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setLong(1, key);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Compute the block start date for a specified date/time and interval
     * @param valueTime The date/time to compute the block start date for
     * @param intervalName The interval name to compute the block start date for
     * @return The block start date
     * @throws SqlDssException If the interval name is not a recognized block size
     * @throws EncodedDateTimeException If thrown by {@link EncodedDateTime#toValues(long)}
     */
    static long getBlockStartDate(long valueTime, String intervalName) throws SqlDssException, EncodedDateTimeException {
        int[] blockStartVals = EncodedDateTime.toValues(valueTime);
        blockStartVals[3] = blockStartVals[4] = blockStartVals[5] = 0;
        int blockSizeMinutes = Interval.getBlockSizeMinutes(intervalName);
        switch (blockSizeMinutes) {
            case CENTURY_MINUTES:
                blockStartVals[0] -= blockStartVals[0] % 100;
                blockStartVals[1] = blockStartVals[2] = 1;
                break;
            case DECADE_MINUTES:
                blockStartVals[0] -= blockStartVals[0] % 10;
                blockStartVals[1] = blockStartVals[2] = 1;
                break;
            case YEAR_MINUTES:
                blockStartVals[1] = blockStartVals[2] = 1;
                break;
            case MONTH_MINUTES:
                blockStartVals[2] = 1;
                break;
            case DAY_MINUTES:
                break;
            default:
                throw new SqlDssException("Unexpected block minutes: " + blockSizeMinutes);
        }
        return EncodedDateTime.encodeDate(blockStartVals);
    }

    /**
     * Generates an array of block start dates for a time window and a specified interval. The first date will be on or
     * before the start of the time window, and the last date will be after the end of the time window (start date of the
     * block following the data)
     * @param startTime The start of the time window
     * @param endTime The end of the time window
     * @param intervalName The interval name to generate the array for
     * @return The generated list of block start dates
     * @throws SqlDssException If thrown by {@link #getBlockStartDate(long, String)} or by {@link Interval#getBlockSizeMinutes(String)}
     * @throws EncodedDateTimeException If thrown by an {@link EncodedDateTime} method
     */
    static long @NotNull [] getBlockStartDates(HecTime startTime, HecTime endTime, String intervalName) throws SqlDssException,
            EncodedDateTimeException {
        long encodedStartTime = EncodedDateTime.encodeDateTime(startTime);
        long encodedEndTime = EncodedDateTime.encodeDateTime(endTime);
        long blockStartTime = EncodedDateTime.toEncodedDateTime(getBlockStartDate(encodedStartTime, intervalName));
        int blockMinutes = Interval.getBlockSizeMinutes(intervalName);
        List<Long> blockStartTimeList = new ArrayList<>();
        while (blockStartTime <= encodedEndTime) {
            blockStartTimeList.add(blockStartTime);
            blockStartTime = EncodedDateTime.incrementEncodedDateTime(blockStartTime, blockMinutes, 1);
        }
        blockStartTimeList.add(blockStartTime);
        long[] blockStartDates = new long[blockStartTimeList.size()];
        for (int i = 0; i < blockStartDates.length; ++i) {
            blockStartDates[i] = EncodedDateTime.toEncodedDate(blockStartTimeList.get(i));
        }
        return blockStartDates;
    }

    /**
     * Retrieves the database key for a specified time series name
     * @param name The time series name
     * @param conn The JDBC connection
     * @return The database key
     * @throws SqlDssException If the time series name is invalid
     * @throws SQLException If SQL Error
     */
    public static long getTimeSeriesSpecKey(@NotNull String name, Connection conn) throws SqlDssException, SQLException {
        // name like "[ctx:]base-sub_loc|base-sub_param|param_type|intvl|dur|version"
        long key = -1;
        String[] parts = name.split("\\|");
        if (parts.length != 6) {
            throw new SqlDssException("Invalid time series name: " + name);
        }
        long locKey = Location.getLocationKey(parts[0], conn);
        if (locKey < 0) {
            return key;
        }
        long paramKey;
        paramKey = Parameter.getParameterKey(parts[1], conn);
        if (paramKey < 0) {
            return key;
        }
        String intvlName = Interval.getInterval(parts[3]);
        String durName = Duration.getDuration(parts[4], conn);
        boolean nullKey;
        try (PreparedStatement ps = conn.prepareStatement(
                """
                        select key
                          from time_series
                         where deleted = 0
                           and location = ?
                           and parameter = ?
                           and parameter_type = ?
                           and interval = ?
                           and duration = ?
                           and version = ?"""
        )) {
            ps.setLong(1, locKey);
            ps.setLong(2, paramKey);
            ps.setString(3, parts[2]);
            ps.setString(4, intvlName);
            ps.setString(5, durName);
            ps.setString(6, parts[5]);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                key = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            key = -1;
        }

        return key;
    }

    /**
     * Stores a time series name in the database and returns is database key. If the time series name already exists,
     * the existing key is returned
     * @param name The time series name
     * @param conn The JDBC connection
     * @return The database key
     * @throws SqlDssException If the time series name is invalid
     * @throws SQLException If SQL error
     */
    static long putTimeSeriesSpec(String name, Connection conn) throws SqlDssException, SQLException {
        // name like "[ctx:]base-sub_loc|base-sub_param|param_type|intvl|dur|version"
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            String[] parts = name.split("\\|"); // if not 6 parts, will throw in getTimeSeriesSpecKey()
            long locKey = Location.putLocation(parts[0], conn);
            long paramKey = Parameter.putParameter(parts[1], conn);
            String intvlName = Interval.getInterval(parts[3]);
            String durName = Duration.getDuration(parts[4], conn);
            boolean nullKey;
            try (PreparedStatement ps = conn.prepareStatement("""
                    insert
                      into time_series
                           (deleted,
                            location,
                            parameter,
                            parameter_type,
                            interval,
                            duration,
                            version
                           )
                    values (0, ?, ?, ?, ?, ?, ?)"""
            )) {
                ps.setLong(1, locKey);
                ps.setLong(2, paramKey);
                ps.setString(3, parts[2]);
                ps.setString(4, intvlName);
                ps.setString(5, durName);
                ps.setString(6, parts[5]);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    SQL_SELECT_LAST_INSERT_ROWID
            )) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    key = rs.getLong(LAST_INSERT_ROWID);
                    nullKey = rs.wasNull();
                }
            }
            if (nullKey) {
                key = -1;
            }
        }
        return key;
    }

    /**
     * Generates a catalog of time series in the database that have values
     * @param nameRegex A regular expression of the time series names to match. May be null to match every name
     * @param condensed Whether to generate a condensed catalog
     *                  <dl>
     *                      <dt>Condensed Catalog</dt>
     *                      <dd>Each time series record whose name matches is included, with the (encoded) block start
     *                          date of each record appended to the time series name, delimited by a pipe (<code>'|'</code>)
     *                          character<br>
     *                          <pre>
     *                              SWT:Olive|Flow|INST-VAL|1Hour|0|Obs|20250601
     *                              SWT:Olive|Flow|INST-VAL|1Hour|0|Obs|20250701
     *                          </pre>
     *                      </dd>
     *                      <dt>Non-condensed Catalog</dt>
     *                      <dd>Each time series name that matches and that has data is included once, with its first
     *                          and last value time appended to the name, delimited by a pipe (<code>'|'</code>) character
     *                          <pre>
     *                              SWT:Olive|Flow|INST-VAL|1Hour|0|Obs|20250612130000 - 20260705080000
     *                          </pre>
     *                      </dd>
     *                  </dl>
     * @param flags A (possibly null or empty) string containing any permutation of the following:
     *              <dl>
     *                  <dt>N</dt>
     *                  <dd>Include non-deleted records</dd>
     *                  <dt>D</dt>
     *                  <dd>Include deleted records</dd>
     *              </dl>
     *              If null or empty, the effect is the same as "N" (only non-deleted records are cataloged)
     * @param sqldss The SqlDss object
     * @return An array of time series catalog names
     * @throws SqlDssException If <code>flags</code> is invalid
     * @throws SQLException If SQL error
     */
    public static String @NotNull [] catalogTimeSeries(String nameRegex, boolean condensed, String flags, SqlDss sqldss) throws SqlDssException, SQLException {
        flags = flags == null || flags.isEmpty() ? "N" : flags.toUpperCase();
        if (!flags.matches("[DN]*")) {
            throw new SqlDssException("Invalid flags string: "+flags);
        }
        String sql = getTsCatalogSql(flags);
        List<String> names = new ArrayList<>();
        Connection conn = sqldss.getConnection();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long key = rs.getLong("key");
                    String context = rs.getString("context");
                    String baseLocation = rs.getString("base_location");
                    String subLocation = rs.getString("sub_location");
                    String baseParameter = rs.getString("base_parameter");
                    String subParameter = rs.getString("sub_parameter");
                    String parameterType = rs.getString("parameter_type");
                    String interval = rs.getString("interval");
                    String duration = rs.getString("duration");
                    String version = rs.getString("version");

                    StringBuilder name = new StringBuilder();
                    if (context != null && !context.isEmpty()) {
                        name.append(context).append(':');
                    }
                    name.append(baseLocation);
                    if (subLocation != null && !subLocation.isEmpty()) {
                        name.append('-').append(subLocation);
                    }
                    name.append('|').append(baseParameter);
                    if (subParameter != null && !subParameter.isEmpty()) {
                        name.append('-').append(subParameter);
                    }
                    name.append('|').append(parameterType);
                    name.append('|').append(interval);
                    name.append('|').append(duration);
                    name.append('|').append(version);

                    if (nameRegex != null && !nameRegex.isEmpty() && !name.toString().matches(nameRegex)) {
                        continue;
                    }
                    if (condensed) {
                        try (PreparedStatement ps3 = conn.prepareStatement(getTsCondensedCatalogSql(flags))) {
                            ps3.setLong(1, key);
                            try (ResultSet rs3 = ps3.executeQuery()) {
                                if (rs3.next()) {
                                    long firstTime = rs3.getLong("first_time");
                                    if (!rs3.wasNull()) {
                                        long lastTime = rs3.getLong("last_time");
                                        names.add(String.format("%s|%d - %d", name.toString(), firstTime, lastTime));
                                    }
                                }
                            }
                        }
                    }
                    else {
                        try (PreparedStatement ps2 = conn.prepareStatement(getTsRecordCatalogSql(flags))) {
                            ps2.setLong(1, key);
                            try (ResultSet rs2 = ps2.executeQuery()) {
                                while (rs2.next()) {
                                    long blockStartDate = rs2.getLong("block_start_date");
                                    names.add(String.format("%s|%d", name.toString(), blockStartDate));
                                }
                            }
                        }
                    }
                }
            }
        }
        String[] namesArray = names.toArray(new String[0]);
        Arrays.sort(namesArray);
        return namesArray;
    }

    /**
     * Generate the SQL used to catalog time series names based on flags
     * @param flags A string containing any permutation of the following:
     *              <dl>
     *                  <dt>N</dt>
     *                  <dl>Include non-deleted records</dl>
     *                  <dt>D</dt>
     *                  <dl>Include deleted records</dl>
     *              </dl>
     *              If null or empty, the effect is the same as "N" (only non-deleted records are cataloged)
     * @return The SQL statement
     */
    private static @Nullable String getTsCatalogSql(@NotNull String flags) {
        boolean matchNormal = flags.indexOf('N') != -1;
        boolean matchDeleted = flags.indexOf('D') != -1;
        String sql = null;
        if (matchNormal) {
            if (matchDeleted) {
                sql = """
                        select ts.key,
                               bl.context,
                               bl.name as base_location,
                               l.sub_location,
                               p.base_parameter,
                               p.sub_parameter,
                               parameter_type,
                               interval,
                               duration,
                               version
                          from time_series ts,
                               location l,
                               base_location bl,
                               parameter p
                         where l.key = ts.location
                           and bl.key = l.base_location
                           and p.key = ts.parameter""";
            } else {
                sql = """
                        select ts.key,
                               bl.context,
                               bl.name as base_location,
                               l.sub_location,
                               p.base_parameter,
                               p.sub_parameter,
                               parameter_type,
                               interval,
                               duration,
                               version
                          from time_series ts,
                               location l,
                               base_location bl,
                               parameter p
                         where l.key = ts.location
                           and bl.key = l.base_location
                           and p.key = ts.parameter
                           and ts.deleted = 0""";
            }
        }
        else if (matchDeleted) {
            sql = """
                        select ts.key,
                               bl.context,
                               bl.name as base_location,
                               l.sub_location,
                               p.base_parameter,
                               p.sub_parameter,
                               parameter_type,
                               interval,
                               duration,
                               version
                          from time_series ts,
                               location l,
                               base_location bl,
                               parameter p
                         where l.key = ts.location
                           and bl.key = l.base_location
                           and p.key = ts.parameter
                           and (ts.deleted = 1
                                or exists (select *
                                             from tsv
                                            where time_series = ts.key
                                              and deleted = 1
                                          )
                               )""";
        }
        return sql;
    }

    /**
     * Generate the SQL used to catalog time series records based on flags
     * @param flags A string containing any permutation of the following:
     *              <dl>
     *                  <dt>N</dt>
     *                  <dl>Include non-deleted records</dl>
     *                  <dt>D</dt>
     *                  <dl>Include deleted records</dl>
     *              </dl>
     *              If null or empty, the effect is the same as "N" (only non-deleted records are cataloged)
     * @return The SQL statement
     */
    private static @Nullable String getTsRecordCatalogSql (@NotNull String flags){
        boolean matchNormal = flags.indexOf('N') != -1;
        boolean matchDeleted = flags.indexOf('D') != -1;
        String sql = null;
        if (matchNormal) {
            if (matchDeleted) {
                sql = """
                        select block_start_date
                          from tsv
                         where time_series = ?
                         order by 1""";
            } else {
                sql = """
                        select block_start_date
                          from tsv
                         where time_series = ?
                           and deleted = 0
                         order by 1""";
            }
        }
        else if (matchDeleted) {
            sql = """
                        select block_start_date
                          from tsv
                         where time_series = ?
                           and deleted = 1
                         order by 1""";
        }
        return sql;
    }

    /**
     * Generate the SQL used to catalog time series extents for condensed catalog based on flags
     * @param flags A string containing any permutation of the following:
     *              <dl>
     *                  <dt>N</dt>
     *                  <dl>Include non-deleted records</dl>
     *                  <dt>D</dt>
     *                  <dl>Include deleted records</dl>
     *              </dl>
     *              If null or empty, the effect is the same as "N" (only non-deleted records are cataloged)
     * @return The SQL statement
     */
    private static @Nullable String getTsCondensedCatalogSql (@NotNull String flags){
        boolean matchNormal = flags.indexOf('N') != -1;
        boolean matchDeleted = flags.indexOf('D') != -1;
        String sql = null;
        if (matchNormal) {
            if (matchDeleted) {
                sql = """
                        select min(first_time) as first_time,
                               max(last_time) as last_time
                          from tsv_info
                         where time_series = ?""";
            } else {
                sql = """
                        with records as (select *
                                           from tsv
                                          where time_series = ?
                                            and deleted = 0
                                        )
                        select min(i.first_time) as first_time,
                               max(i.last_time) as last_time
                          from tsv_info i,
                               records r
                         where i.time_series = r.time_series
                           and i.block_start_date = r.block_start_date""";
            }
        }
        else if (matchDeleted) {
            sql = """
                        with records as (select *
                                           from tsv
                                          where time_series = ?
                                            and deleted = 1
                                        )
                        select min(i.first_time) as first_time,
                               max(i.last_time) as last_time
                          from tsv_info i,
                               records r
                         where i.time_series = r.time_series
                           and i.block_start_date = r.block_start_date""";
        }
        return sql;
    }

    /**
     * Mark a single time series record as deleted
     * @param recordSpec The time series record to delete (uncondensed catalog name for time series record)
     * @param sqldss The SqlDss object
     * @throws SqlDssException If thrown by {@link #deleteOrUndeleteTimeSeriesRecord(String, boolean, Connection)}
     * @throws SQLException If SQL error
     */
    public static void deleteTimeSeriesRecord(String recordSpec, @NotNull SqlDss sqldss) throws SqlDssException, SQLException {
        deleteOrUndeleteTimeSeriesRecord(recordSpec, true, sqldss.getConnection());
    }

    /**
     * Mark a multiple time series records as deleted
     * @param recordSpecs The time series records to delete (uncondensed catalog names for each time series record)
     * @param sqldss The SqlDss object
     * @throws SqlDssException If thrown by {@link #deleteOrUndeleteTimeSeriesRecords(String[], boolean, Connection)}
     * @throws SQLException If SQL error
     */
    public static void deleteTimeSeriesRecords(String[] recordSpecs, @NotNull SqlDss sqldss)
            throws SqlDssException, SQLException {
        deleteOrUndeleteTimeSeriesRecords(recordSpecs, true, sqldss.getConnection());
    }

    /**
     * Mark a single time series record as undeleted
     * @param recordSpec The time series record to delete (uncondensed catalog name for time series record)
     * @param sqldss The SqlDss object
     * @throws SqlDssException If thrown by {@link #deleteOrUndeleteTimeSeriesRecord(String, boolean, Connection)}
     * @throws SQLException If SQL error
     */
    public static void undeleteTimeSeriesRecord(String recordSpec, @NotNull SqlDss sqldss) throws SqlDssException, SQLException {
        deleteOrUndeleteTimeSeriesRecord(recordSpec, false, sqldss.getConnection());
    }

    /**
     * Mark a multiple time series records as undeleted
     * @param recordSpecs The time series records to delete (uncondensed catalog names for each time series record)
     * @param sqldss The SqlDss object
     * @throws SqlDssException If thrown by {@link #deleteOrUndeleteTimeSeriesRecords(String[], boolean, Connection)}
     * @throws SQLException If SQL error
     */
    public static void undeleteTimeSeriesRecords(String[] recordSpecs, @NotNull SqlDss sqldss)
            throws SqlDssException, SQLException {
        deleteOrUndeleteTimeSeriesRecords(recordSpecs, false, sqldss.getConnection());
    }

    /**
     * Mark multiple time series records as deleted or undeleted
     * @param recordSpecs The time series records to mark (uncondensed catalog names for each time series record)
     * @param deleteRecords Whether to mark as deleted or undeleted
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #deleteOrUndeleteTimeSeriesRecord(String, boolean, Connection)}
     */
    public static void deleteOrUndeleteTimeSeriesRecords(String[] recordSpecs, boolean deleteRecords, @NotNull Connection conn)
            throws SQLException, SqlDssException {
        boolean isAutoCommit = conn.getAutoCommit();
        if (isAutoCommit) {
            conn.setAutoCommit(false);
        }
        try {
            for (String recordSpec: recordSpecs) {
                deleteOrUndeleteTimeSeriesRecord(recordSpec, deleteRecords, conn);
            }
        }
        finally {
            if (isAutoCommit) {
                conn.commit();
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Mark a single time series record as deleted or undeleted
     * @param recordSpec The time series record to mark (uncondensed catalog name for time series record)
     * @param deleteRecord Whether to mark as deleted or undeleted
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If: <ul>
     *                               <li><code>recordSpec</code> is invalid</li>
     *                               <li>no such time series record exists</li>
     *                               <li>problem updated deleted flag on record</li>
     *                           </ul>
     */
    public static void deleteOrUndeleteTimeSeriesRecord(@NotNull String recordSpec, boolean deleteRecord, Connection conn)
            throws  SQLException, SqlDssException {
        String[] parts = recordSpec.split("\\|", -1);
        if (parts.length != 7) {
            throw new SqlDssException(String.format("Invalid record specification: %s", recordSpec));
        }
        String name = String.join("|", Arrays.copyOfRange(parts, 0, 6));
        long recordStartDate = Long.parseLong(parts[6]);
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            throw new SqlDssException(String.format("No such %stime series: %s", deleteRecord ? "" : "deleted ", name));
        }
        String sqlSelect = "select deleted from tsv where time_series = ? and block_start_date = ?";
        String sqlUpdate = String.format(
                "update tsv set deleted = %d where time_series = ? and block_start_date = ?",
                deleteRecord ? 1 : 0);
        long deleted;
        try (PreparedStatement psSelect = conn.prepareStatement(sqlSelect)) {
            psSelect.setLong(1, key);
            psSelect.setLong(2, recordStartDate);
            try (ResultSet rs = psSelect.executeQuery()) {
                rs.next();
                deleted = rs.getLong("deleted");
                if (rs.wasNull() || deleted == (deleteRecord ? 1 : 0)) {
                    throw new SqlDssException(String.format(
                            "No such %stime series: %s|%d",
                            deleteRecord ? "" : "deleted ",
                            name,
                            recordStartDate));
                }
            }
        }
        try (PreparedStatement psUpdate = conn.prepareStatement(sqlUpdate)) {
            psUpdate.setLong(1, key);
            psUpdate.setLong(2, recordStartDate);
            int updateCount = psUpdate.executeUpdate();
            if (updateCount != 1) {
                throw new SqlDssException(String.format(
                        "Failed to update %stime series: %s|%d",
                        deleteRecord ? "" : "deleted ",
                        name,
                        recordStartDate));
            }
        }
    }

    /**
     * Modifies a {@link TimeSeriesContainer} object in-place, removing blocks of contiguous missing or rejected values
     * from the beginning and end
     * @param tsc The TimeSeriesContainer to modify in-place
     */
    public static void trimTimeSeriesContainer(@NotNull TimeSeriesContainer tsc) {
        int firstNonMissing = -1;
        int lastNonMissing = -1;
        for (int i = 0; i < tsc.numberValues; ++i) {
            if (tsc.values[i] != UNDEFINED_DOUBLE
                    && (tsc.quality == null
                    || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                    || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE)) {
                firstNonMissing = i;
                break;
            }
        }
        if (firstNonMissing == -1) {
            // empty
            tsc.times = new int[0];
            tsc.values = new double[0];
            if (tsc.quality != null) {
                tsc.quality = new int[0];
            }
            tsc.numberValues = 0;
            tsc.setStartTime(new HecTime());
            tsc.setEndTime(new HecTime());
        }
        else {
            for (int i = tsc.numberValues - 1; i >= firstNonMissing; --i) {
                if (tsc.values[i] != UNDEFINED_DOUBLE
                        && (tsc.quality == null
                        || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                        || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE)) {
                    lastNonMissing = i;
                    break;
                }
            }
            if (firstNonMissing > 0 || lastNonMissing < tsc.numberValues - 1) {
                tsc.numberValues = lastNonMissing - firstNonMissing + 1;
                tsc.times = Arrays.copyOfRange(tsc.times, firstNonMissing, lastNonMissing + 1);
                tsc.values = Arrays.copyOfRange(tsc.values, firstNonMissing, lastNonMissing + 1);
                if (tsc.quality != null) {
                    tsc.quality = Arrays.copyOfRange(tsc.quality, firstNonMissing, lastNonMissing + 1);
                }
                tsc.startHecTime.set(tsc.times[0]);
                tsc.endHecTime.set(tsc.times[tsc.numberValues - 1]);
            }
        }
    }

    /**
     * Merges regular interval time series data according to a store rule
     * @param intervalMinutes The interval minutes of the time series data
     * @param storeRule The store rule to use
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws SqlDssException If <code>storeRule</code> is unexpected. Shouldn't be possible unless a new store rule
     * is added and not included here
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link #mergeReplaceAll(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeDoNotReplace(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeReplaceMissingValuesOnly(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeReplaceWithNonMissing(int, TsvData, TsvData, TsvData)}</li>
     * </ul>
     */
    public static void mergeTimeSeries(
            int intervalMinutes,
            @NotNull REGULAR_STORE_RULE storeRule,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws SqlDssException, EncodedDateTimeException {
        switch (storeRule) {
            case REPLACE_ALL:
            case REPLACE_ALL_CREATE:
            case REPLACE_ALL_DELETE:
                mergeReplaceAll(intervalMinutes, incoming, existing, merged);
                break;
            case DO_NOT_REPLACE:
                mergeDoNotReplace(intervalMinutes, incoming, existing, merged);
                break;
            case REPLACE_MISSING_VALUES_ONLY:
                mergeReplaceMissingValuesOnly(intervalMinutes, incoming, existing, merged);
                break;
            case REPLACE_WITH_NON_MISSING:
                mergeReplaceWithNonMissing(intervalMinutes, incoming, existing, merged);
                break;
            default:
                throw new SqlDssException("Unexpected regular store rule: " + storeRule.name());
        }
    }

    /**
     * Merges irregular interval time series data according to a store rule
     * @param storeRule The store rule to use
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws SqlDssException If <code>storeRule</code> is unexpected. Shouldn't be possible unless a new store rule
     * is added and not included here
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link #mergeReplaceAll(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeDoNotReplace(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeReplaceMissingValuesOnly(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeReplaceWithNonMissing(int, TsvData, TsvData, TsvData)}</li>
     *     <li>{@link #mergeDeleteInsert(TsvData, TsvData, TsvData)}</li>
     * </ul>
     */
    public static void mergeTimeSeries(
            @NotNull IRREGULAR_STORE_RULE storeRule,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws SqlDssException, EncodedDateTimeException {
        int intervalMinutes = 0;
        switch (storeRule) {
            case REPLACE_ALL:
            case MERGE:
                mergeReplaceAll(intervalMinutes, incoming, existing, merged);
                break;
            case DO_NOT_REPLACE:
                mergeDoNotReplace(intervalMinutes, incoming, existing, merged);
                break;
            case REPLACE_MISSING_VALUES_ONLY:
                mergeReplaceMissingValuesOnly(intervalMinutes, incoming, existing, merged);
                break;
            case REPLACE_WITH_NON_MISSING:
                mergeReplaceWithNonMissing(intervalMinutes, incoming, existing, merged);
                break;
            case DELETE_INSERT:
                mergeDeleteInsert(incoming, existing, merged);
                break;
            default:
                throw new SqlDssException("Unexpected regular store rule: " + storeRule.name());
        }
    }

    /**
     * Merges regular or irregular interval time series using the REPLACE_ALL store rule
     * @param intervalMinutes The interval minutes of the interval for regular time series, zero for irregular
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link EncodedDateTime#intervalsBetween(long, long, int)}</li>
     *     <li>{@link EncodedDateTime#makeRegularEncodedDateTimeArray(long, int, int)}</li>
     * </ul>
     */
    static void mergeReplaceAll(
            int intervalMinutes,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws EncodedDateTimeException {
        int count;
        if (intervalMinutes == 0) {
            count = incoming.count + existing.count;
            merged.times = new long[count];
        }
        else {
            long first = Math.min(incoming.times[incoming.offset], existing.times[existing.offset]);
            long last = Math.max(incoming.times[incoming.offset + incoming.count - 1], existing.times[existing.offset + existing.count - 1]);
            count = EncodedDateTime.intervalsBetween(first, last, intervalMinutes) + 2;
            merged.times = EncodedDateTime.makeRegularEncodedDateTimeArray(first, count, intervalMinutes);
        }
        merged.values = new double[count];
        merged.qualities = new int[count];

        int i = incoming.offset;
        int e = existing.offset;
        int m = 0;
        //--------------------------------------------------//
        // copy within limits of both incoming and existing //
        //--------------------------------------------------//
        outer:
        while (i < incoming.offset + incoming.count && e < existing.offset + existing.count) {
            if (incoming.times[i] <= existing.times[e]) {
                while (incoming.times[i] <= existing.times[e]) {
                    merged.times[m] = incoming.times[i];
                    merged.values[m] = incoming.values[i];
                    merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
                    ++m;
                    if (++i + incoming.offset >= incoming.count) {
                        if (incoming.times[i-1] >= existing.times[e]) {
                            ++e;
                        }
                        break outer;
                    }
                }
                if (existing.times[e] > merged.times[m-1]) {
                    continue;
                }
                else if (++e + existing.offset >= existing.count) {
                    break;
                }
            }
            if (existing.times[e] < incoming.times[i]) {
                while (existing.times[e] < incoming.times[i]) {
                    merged.times[m] = existing.times[e];
                    merged.values[m] = existing.values[e];
                    merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    ++m;
                    if (++e + existing.offset >= existing.count) {
                        if (existing.times[e-1] >= incoming.times[i]) {
                            ++i;
                        }
                        break outer;
                    }
                }
                if (incoming.times[i] > merged.times[m-1]) {
                    continue;
                }
                if (++i + incoming.offset >= incoming.count) {
                    break;
                }
            }
        }
        if (intervalMinutes > 0) {
            //-----------------------------------------------//
            // fill any blanks between incoming and existing //
            //-----------------------------------------------//
            if (i < incoming.offset + incoming.count) {
                while (merged.times[m] < incoming.times[i]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
            if (e < existing.offset + existing.count) {
                while (merged.times[m] < existing.times[e]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
        }
        //------------------------------------------------//
        // fill remainder with whichever wasn't exhausted //
        //------------------------------------------------//
        while (i < incoming.offset + incoming.count) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
            ++i;
            ++m;
        }
        while (e < existing.offset + existing.count) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++e;
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
    }

    /**
     * Merges regular or irregular interval time series using the DO_NOT_REPLACE store rule
     * @param intervalMinutes The interval minutes of the interval for regular time series, zero for irregular
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link EncodedDateTime#intervalsBetween(long, long, int)}</li>
     *     <li>{@link EncodedDateTime#makeRegularEncodedDateTimeArray(long, int, int)}</li>
     * </ul>
     */
    static void mergeDoNotReplace(
            int intervalMinutes,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws EncodedDateTimeException {
        int count;
        if (intervalMinutes == 0) {
            count = incoming.count + existing.count;
            merged.times = new long[count];
        }
        else {
            long first = Math.min(incoming.times[incoming.offset], existing.times[existing.offset]);
            long last = Math.max(incoming.times[incoming.offset + incoming.count - 1], existing.times[existing.offset + existing.count - 1]);
            count = EncodedDateTime.intervalsBetween(first, last, intervalMinutes) + 2;
            merged.times = EncodedDateTime.makeRegularEncodedDateTimeArray(first, count, intervalMinutes);
        }
        merged.values = new double[count];
        merged.qualities = new int[count];

        int i = incoming.offset;
        int e = existing.offset;
        int m = 0;
        //--------------------------------------------------//
        // copy within limits of both incoming and existing //
        //--------------------------------------------------//
        outer:
        while (i < incoming.offset + incoming.count && e < existing.offset + existing.count) {
            if (incoming.times[i] < existing.times[e]) {
                while (incoming.times[i] < existing.times[e]) {
                    merged.times[m] = incoming.times[i];
                    merged.values[m] = incoming.values[i];
                    merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
                    ++m;
                    if (++i + incoming.offset >= incoming.count) {
                        if (existing.times[e] <= merged.times[m-1]) {
                            ++e;
                        }
                        break outer;
                    }
                }
                if (existing.times[e] > merged.times[m-1]) {
                    continue;
                }
                if (++e + existing.offset >= existing.count) {
                    break;
                }
            }
            if (existing.times[e] <= incoming.times[i]) {
                while (existing.times[e] <= incoming.times[i]) {
                    merged.times[m] = existing.times[e];
                    merged.values[m] = existing.values[e];
                    merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    ++m;
                    if (++e + existing.offset >= existing.count) {
                        if (existing.times[e-1] >= incoming.times[i]) {
                            ++i;
                        }
                        break outer;
                    }
                }
                if (incoming.times[i] > merged.times[m-1]) {
                    continue;
                }
                if (++i + incoming.offset >= incoming.count) {
                    break;
                }
            }
        }
        if (intervalMinutes > 0) {
            //-----------------------------------------------//
            // fill any blanks between incoming and existing //
            //-----------------------------------------------//
            if (i < incoming.offset + incoming.count) {
                while (merged.times[m] < incoming.times[i]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
            if (e < existing.offset + existing.count) {
                while (merged.times[m] < existing.times[e]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
        }
        //------------------------------------------------//
        // fill remainder with whichever wasn't exhausted //
        //------------------------------------------------//
        while (i < incoming.offset + incoming.count) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
            ++i;
            ++m;
        }
        while (e < existing.offset + existing.count) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++e;
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
    }

    /**
     * Merges regular or irregular interval time series using the REPLACE_MISSING_VALUES_ONLY store rule
     * @param intervalMinutes The interval minutes of the interval for regular time series, zero for irregular
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link EncodedDateTime#intervalsBetween(long, long, int)}</li>
     *     <li>{@link EncodedDateTime#makeRegularEncodedDateTimeArray(long, int, int)}</li>
     * </ul>
     */
    static void mergeReplaceMissingValuesOnly(
            int intervalMinutes,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws EncodedDateTimeException {
        int count;
        if (intervalMinutes == 0) {
            count = incoming.count + existing.count;
            merged.times = new long[count];
        }
        else {
            long first = Math.min(incoming.times[incoming.offset], existing.times[existing.offset]);
            long last = Math.max(incoming.times[incoming.offset + incoming.count - 1], existing.times[existing.offset + existing.count - 1]);
            count = EncodedDateTime.intervalsBetween(first, last, intervalMinutes) + 2;
            merged.times = EncodedDateTime.makeRegularEncodedDateTimeArray(first, count, intervalMinutes);
        }
        merged.values = new double[count];
        merged.qualities = new int[count];

        int i = incoming.offset;
        int e = existing.offset;
        int m = 0;
        //--------------------------------------------------//
        // copy within limits of both incoming and existing //
        //--------------------------------------------------//
        outer:
        while (i < incoming.offset + incoming.count && e < existing.offset + existing.count) {
            if (incoming.times[i] <= existing.times[e]) {
                while (incoming.times[i] <= existing.times[e]) {
                    if (
                            existing.times[e] != incoming.times[i] ||
                                    existing.values[e] == UNDEFINED_DOUBLE
                                            && (
                                            existing.qualities == null
                                                    || (
                                                    ((existing.qualities[e] & QUALITY_SCREENED_VALIDITY_MASK) == QUALITY_MISSING_VALUE)
                                                            || ((existing.qualities[e] & QUALITY_SCREENED_VALIDITY_MASK) == QUALITY_REJECTED_VALUE)
                                            )
                                    )
                    ) {
                        merged.times[m] = incoming.times[i];
                        merged.values[m] = incoming.values[i];
                        merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
                    }
                    else {
                        merged.times[m] = existing.times[e];
                        merged.values[m] = existing.values[e];
                        merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    }
                    ++m;
                    if (++i + incoming.offset >= incoming.count) {
                        if (existing.times[e] <= merged.times[m-1]) {
                            ++e;
                        }
                        break outer;
                    }
                }
                if (existing.times[e] > merged.times[m-1]) {
                    continue;
                }
                if (++e + existing.offset >= existing.count) {
                    break;
                }
            }
            if (existing.times[e] < incoming.times[i]) {
                while (existing.times[e] < incoming.times[i]) {
                    merged.times[m] = existing.times[e];
                    merged.values[m] = existing.values[e];
                    merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    ++m;
                    if (++e + existing.offset >= existing.count) {
                        if (existing.times[e-1] >= incoming.times[i]) {
                            ++i;
                        }
                        break outer;
                    }
                }
                if (incoming.times[i] > merged.times[m-1]) {
                    continue;
                }
                if (++i + incoming.offset >= incoming.count) {
                    break;
                }
            }
        }
        if (intervalMinutes > 0) {
            //-----------------------------------------------//
            // fill any blanks between incoming and existing //
            //-----------------------------------------------//
            if (i < incoming.offset + incoming.count) {
                while (merged.times[m] < incoming.times[i]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
            if (e < existing.offset + existing.count) {
                while (merged.times[m] < existing.times[e]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
        }
        //------------------------------------------------//
        // fill remainder with whichever wasn't exhausted //
        //------------------------------------------------//
        while (i < incoming.offset + incoming.count) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
            ++i;
            ++m;
        }
        while (e < existing.offset + existing.count) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++e;
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
    }

    /**
     * Merges regular or irregular interval time series using the REPLACE_WITH_NON_MISSING store rule
     * @param intervalMinutes The interval minutes of the interval for regular time series, zero for irregular
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link EncodedDateTime#intervalsBetween(long, long, int)}</li>
     *     <li>{@link EncodedDateTime#makeRegularEncodedDateTimeArray(long, int, int)}</li>
     * </ul>
     */
    static void mergeReplaceWithNonMissing(
            int intervalMinutes,
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) throws EncodedDateTimeException {
        int count;
        if (intervalMinutes == 0) {
            count = incoming.count + existing.count;
            merged.times = new long[count];
        }
        else {
            long first = Math.min(incoming.times[incoming.offset], existing.times[existing.offset]);
            long last = Math.max(incoming.times[incoming.offset + incoming.count - 1], existing.times[existing.offset + existing.count - 1]);
            count = EncodedDateTime.intervalsBetween(first, last, intervalMinutes) + 2;
            merged.times = EncodedDateTime.makeRegularEncodedDateTimeArray(first, count, intervalMinutes);
        }
        merged.values = new double[count];
        merged.qualities = new int[count];

        int i = incoming.offset;
        int e = existing.offset;
        int m = 0;
        //--------------------------------------------------//
        // copy within limits of both incoming and existing //
        //--------------------------------------------------//
        outer:
        while (i < incoming.offset + incoming.count && e < existing.offset + existing.count) {
            if (incoming.times[i] <= existing.times[e]) {
                while (incoming.times[i] <= existing.times[e]) {
                    if (
                            existing.times[e] != incoming.times[i] ||
                                    incoming.values[i] != UNDEFINED_DOUBLE
                                    || (
                                    incoming.qualities != null
                                            && (
                                            ((incoming.qualities[i] & QUALITY_SCREENED_VALIDITY_MASK) == QUALITY_MISSING_VALUE)
                                                    || ((incoming.qualities[i] & QUALITY_SCREENED_VALIDITY_MASK) == QUALITY_REJECTED_VALUE)
                                    )
                            )
                    ) {
                        merged.times[m] = incoming.times[i];
                        merged.values[m] = incoming.values[i];
                        merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
                    }
                    else {
                        merged.times[m] = existing.times[e];
                        merged.values[m] = existing.values[e];
                        merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    }
                    ++m;
                    if (++i + incoming.offset >= incoming.count) {
                        if (existing.times[e] <= merged.times[m-1]) {
                            ++e;
                        }
                        break outer;
                    }
                }
                if (existing.times[e] > merged.times[m-1]) {
                    continue;
                }
                if (++e + existing.offset >= existing.count) {
                    break;
                }
            }
            if (existing.times[e] < incoming.times[i]) {
                while (existing.times[e] < incoming.times[i]) {
                    merged.times[m] = existing.times[e];
                    merged.values[m] = existing.values[e];
                    merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
                    ++m;
                    if (++e + existing.offset >= existing.count) {
                        if (existing.times[e-1] >= incoming.times[i]) {
                            ++i;
                        }
                        break outer;
                    }
                }
                if (incoming.times[i] > merged.times[m-1]) {
                    continue;
                }
                if (++i + incoming.offset >= incoming.count) {
                    break;
                }
            }
        }
        if (intervalMinutes > 0) {
            //-----------------------------------------------//
            // fill any blanks between incoming and existing //
            //-----------------------------------------------//
            if (i < incoming.offset + incoming.count) {
                while (merged.times[m] < incoming.times[i]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
            if (e < existing.offset + existing.count) {
                while (merged.times[m] < existing.times[e]) {
                    merged.values[m] = UNDEFINED_DOUBLE;
                    merged.qualities[m] = 0;
                    ++m;
                }
            }
        }
        //------------------------------------------------//
        // fill remainder with whichever wasn't exhausted //
        //------------------------------------------------//
        while (i < incoming.offset + incoming.count) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
            ++i;
            ++m;
        }
        while (e < existing.offset + existing.count) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++e;
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
    }

    /**
     * Merges irregular interval time series using the DELETE_INSERT store rule
     * @param incoming The "new" or "incoming" data to be merged
     * @param existing The "old" or "existing" data to be merged
     * @param merged The result of the merge operation
     */
    static void mergeDeleteInsert(
            @NotNull TsvData incoming,
            @NotNull TsvData existing,
            @NotNull TsvData merged
    ) {
        int count = incoming.count + existing.count;
        merged.times = new long[count];
        merged.values = new double[count];
        merged.qualities = new int[count];

        long deleteWindowStart = incoming.times[incoming.offset];
        long deleteWindowEnd = incoming.times[incoming.offset+incoming.count-1];
        int e = existing.offset;
        int m = 0;
        
        for (; e < existing.offset+existing.count && existing.times[e] < deleteWindowStart; ++e) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++m;
        }
        for(int i = incoming.offset; i < incoming.offset+incoming.count; ++i) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities == null ? 0 : incoming.qualities[i];
            ++m;
        }
        for (; e < existing.offset+existing.count && existing.times[e] <= deleteWindowEnd; ++e) {
        }
        for (; e < existing.offset+existing.count; ++e) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities == null ? 0 : existing.qualities[e];
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
    }
}
