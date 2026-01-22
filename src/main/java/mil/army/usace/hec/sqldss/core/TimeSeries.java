package mil.army.usace.hec.sqldss.core;

import com.google.common.flogger.FluentLogger;
import hec.heclib.dss.DSSPathname;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static hec.lang.Const.UNDEFINED_DOUBLE;
import static mil.army.usace.hec.sqldss.core.Constants.DATA_TYPE.ITS;
import static mil.army.usace.hec.sqldss.core.Constants.DATA_TYPE.RTS;

public final class TimeSeries {
    public static final String SQL_SELECT_TS_BLOCK = "select data from tsv where time_series = %d and " +
            "block_start_date =?";
    public static final int QUALITY_MISSING_VALUE = 5;
    public static final int QUALITY_REJECTED_VALUE = 17;
    public static final int QUALITY_SCREENED_VALIDITY_MASK = 31;

    private TimeSeries() {
        throw new AssertionError("Cannot instantiate");
    }

    static FluentLogger logger = FluentLogger.forEnclosingClass();

    static class TsvRecordHeader {
        Constants.DATA_TYPE dataType;
        int version;
        int valueCount;
        boolean hasQuality;
        long firstTime;
        long[] times = null; // ITS only
    }

    static class TsvInfo {
        int valueCount;
        long firstTime;
        long lastTime;
        double minValue = Double.MAX_VALUE;
        double maxValue = Double.MIN_VALUE;
        long lastUpdate;
    }

    static class TsvData {
        long[] times = null;
        double[] values = null;
        int[] qualities = null;
        int offset = 0;
        int count = -1;
    }

    @NotNull
    static TsvRecordHeader readHeader(ByteBuffer buf) throws CoreException {
        int bufPosition;
        byte dataTypeCode;
        TsvRecordHeader header = new TsvRecordHeader();
        bufPosition = 0;
        dataTypeCode = buf.get(bufPosition);
        bufPosition += Byte.BYTES;
        try {
            header.dataType = Constants.DATA_TYPE.fromCode(dataTypeCode);
        }
        catch (IllegalArgumentException e) {
            throw new CoreException(e);
        }
        switch (header.dataType) {
            case RTS:
                header.version = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (header.version != 1) {
                    throw new CoreException("Don't know how to decode RTS version " + header.version);
                }
                header.valueCount = buf.getInt(bufPosition);
                bufPosition += Integer.BYTES;
                header.hasQuality = buf.get(bufPosition) != 0;
                bufPosition += Byte.BYTES;
                header.firstTime = buf.getLong(bufPosition);
                bufPosition += Long.BYTES;
                buf.position(bufPosition);
                break;
            case ITS:
                throw new CoreException("Cannot yet decode ITS records");
            default:
                throw new CoreException(String.format(
                        "Expected data type of %d (%s) or %d (%s), got %d",
                        RTS.getCode(), RTS.name(), ITS.getCode(), ITS.name(), dataTypeCode));
        }
        return header;
    }

    @NotNull
    public static TimeSeriesContainer getTimeSeriesValues(String name, HecTime startTime, HecTime endTime, boolean trimMissing,
                                                          Connection conn) throws CoreException, SQLException,
            EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        if (isIrregular(name)) {
            throw new CoreException("Cannot yet retrieve irregular time series");
        }
        else {
            tsc = getRegularTimeSeriesValues(name, startTime, endTime, conn);
        }
        if (trimMissing) {
            trimTimeSeriesContainer(tsc);
        }
        return tsc;
    }

    @NotNull
    public static TimeSeriesContainer getAllTimeSeriesValues(String name, boolean trimMissing, Connection conn) throws CoreException,
            SQLException, EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc;
        if (isIrregular(name)) {
            throw new CoreException("Cannot yet retrieve irregular time series");
        }
        else {
            tsc = getAllRegularTimeSeriesValues(name, conn);
        }
        if (trimMissing) {
            trimTimeSeriesContainer(tsc);
        }
        return tsc;
    }

    static void getFirstLastBlockAndInterval(long key, Long[] blockExtents, String[] intervalName, Connection conn) throws SQLException, CoreException {
        if (blockExtents == null || blockExtents.length < 2) {
            throw new CoreException("Parameter 'blockExtents' must be of length 2");
        }
        if (intervalName == null || intervalName.length < 1) {
            throw new CoreException("Parameter 'intervalName' must be of length 1");
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
                    throw new CoreException("Error getting first/last block dates");
                }
            }
        }
    }

    static long getLastTimeFromHeader(@NotNull TsvRecordHeader header, int intervalMinutes) throws EncodedDateTimeException {
        if (header.times == null) {
            // RTS
            return EncodedDateTime.incrementEncodedDateTime(
                    header.firstTime,
                    header.valueCount * intervalMinutes - 1,
                    1);
        }
        else {
            // ITS
            if (header.times.length > 1) {
                return header.times[header.times.length-1] - 1;
            }
            else {
                return header.firstTime;
            }
        }
    }

    public static void getTimeSeriesExtents(String name, Long[] extents, Connection conn) throws CoreException,
            SQLException, EncodedDateTimeException, IOException {
        if (extents == null || extents.length < 2) {
            throw new CoreException("Parameter 'extents' must be of length 2 or more");
        }
        extents[0] = extents[1] = null;
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            throw new CoreException("No such time series: " + name);
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
                }
                else {
                    extents[1] = getLastTimeFromHeader(header, intervalMinutes);
                }
            }
        }
    }

    @NotNull
    static TimeSeriesContainer getAllRegularTimeSeriesValues(String name, Connection conn) throws CoreException,
            SQLException, EncodedDateTimeException, IOException {
        Long[] extents = new Long[2];
        getTimeSeriesExtents(name, extents, conn);
        if (extents[0] == null) {
            throw new CoreException("No data for time series");
        }
        return getRegularTimeSeriesValues(name, EncodedDateTime.toHecTime(extents[0]),
                EncodedDateTime.toHecTime(extents[1]), conn);
    }

    @NotNull
    static TimeSeriesContainer getRegularTimeSeriesValues(@NotNull String name, HecTime startTime, HecTime endTime,
                                                          Connection conn) throws CoreException, SQLException,
            EncodedDateTimeException, IOException {
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
            throw new CoreException("No such time series: " + name);
        }
        // get the interval and offset
        String intevalName = null;
        String existingOffsetStr = null;
        int intervalMinutes;
        int existingOffsetMinutes;
        try (PreparedStatement ps = conn.prepareStatement("select interval, interval_offset from time_series where " +
                "key = ?")) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                intervalName = rs.getString("interval");
                existingOffsetStr = rs.getString("interval_offset");
            }
        }
        if (existingOffsetStr == null || existingOffsetStr.isEmpty()) {
            throw new CoreException("Interval offset is not set for time series!");
        }
        intervalMinutes = Interval.getIntervalMinutes(intervalName);
        if (intervalMinutes == 0) {
            throw new CoreException("Error getting interval minutes for " + name);
        }
        tsc.interval = intervalMinutes;
        existingOffsetMinutes = Duration.iso8601ToMinutes(existingOffsetStr);
        // determine blocks
        long[] encodedBlockDates = getBlockStartDates(startTime, endTime, intervalName);
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
                    blob = rs.getBytes("data");
                }
            }
            if (blob == null) {
                throw new CoreException("Missing expected record at "+encodedBlockDates[i]);
            }
            ByteBuffer buf = ByteBuffer.wrap(blob);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            int bufPosition = 0;
            byte dataType = buf.get(bufPosition);
            bufPosition += Byte.BYTES;
            if (dataType != 9) { // RTS
                throw new CoreException("Expected data type of 9 (RTS), got " + dataType);
            }
            byte dataTypeVersion = buf.get(bufPosition);
            bufPosition += Byte.BYTES;
            if (dataTypeVersion != 1) {
                throw new CoreException("Don't know how to decode RTS version " + dataTypeVersion);
            }
            int valueCount = buf.getInt(bufPosition);
            bufPosition += Integer.BYTES;
            byte hasQuality = buf.get(bufPosition);
            bufPosition += Byte.BYTES;
            long encodedFirstTime = buf.getLong(bufPosition);
            bufPosition += Long.BYTES;
            HecTime firstTime = EncodedDateTime.toHecTime(encodedFirstTime);
            HecTime intervalTime = new HecTime(firstTime);
            intervalTime.adjustToIntervalOffset(intervalMinutes, 0);
            if (intervalTime.greaterThan(firstTime)) {
                intervalTime.subtractMinutes(intervalMinutes);
            }
            int thisOffset = (int) ((firstTime.getTimeInMillis() - intervalTime.getTimeInMillis()) / 60000);
            if (thisOffset != existingOffsetMinutes) {
                throw new CoreException(String.format(
                        "Interval offset for block starting at %d (%d) doesn't match offset for time series (%d)",
                        encodedBlockDates[i], thisOffset, existingOffsetMinutes));
            }
            double[] values = new double[valueCount];
            for (int j = 0; j < valueCount; ++j) {
                values[j] = buf.getDouble(bufPosition);
                bufPosition += Double.BYTES;
            }
            int[] qualities = new int[valueCount];
            if (hasQuality != 0) {
                for (int j = 0; j < valueCount; ++j) {
                    qualities[j] = buf.getInt(bufPosition);
                    bufPosition += Integer.BYTES;
                }
            }
            else {
                Arrays.fill(qualities, 0);
            }
            firstTime.addMinutes(thisOffset);
            timeArrays[i] = new int[valueCount];
            valueArrays[i] = new double[valueCount];
            qualityArrays[i] = new int[valueCount];
            if (intervalMinutes < 30 * 1440) {
                int minutes = firstTime.value();
                for (int j = 0; j < valueCount; ++j) {
                    timeArrays[i][j] = minutes;
                    minutes += intervalMinutes;
                    valueArrays[i][j] = values[j];
                    qualityArrays[i][j] = qualities[j];
                }
            }
            else {
                HecTime t = new HecTime(firstTime);
                for (int j = 0; j < valueCount; ++j) {
                    timeArrays[i][j] = t.value();
                    t.increment(1, intervalMinutes);
                    valueArrays[i][j] = values[j];
                    qualityArrays[i][j] = qualities[j];
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
                if (intervalMinutes < 30 * 1440) {
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
                }
                else {
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
        }
        else {
            tsc.times = Arrays.copyOf(times, count);
            tsc.setValues(Arrays.copyOf(values, count));
            tsc.setQuality(Arrays.copyOf(qualities, count));
        }
        tsc.numberValues = count;
        tsc.setStartTime(startTime);
        tsc.setEndTime(endTime);
        return tsc;
    }

    private static int getIntervalMinutesForIntervalName(Connection conn, long key) throws SQLException {
        int intervalMinutes;
        try (PreparedStatement ps = conn.prepareStatement(
                "select i.minutes         " +
                        "  from time_series t,    " +
                        "       interval i        " +
                        " where t.key = ?         " +
                        "   and i.key = t.interval"
        )) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                intervalMinutes = (int) rs.getLong("minutes");
            }
        }
        return intervalMinutes;
    }

    public static void putTimeSeriesValues(@NotNull TimeSeriesContainer tsc, String storeRule, Connection conn) throws CoreException, SQLException, EncodedDateTimeException, IOException {
        String name = tsc.fullName;
        String[] parts = name.split("\\|", -1);
        String intervalName = parts[3];
        int intervalMinutes = Interval.getIntervalMinutes(intervalName);
        try (Connection tmp = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            try (Statement st = tmp.createStatement()) {
                st.execute(
                        "create table existing_tsv(             " +
                                "        date_time integer primary key, " +
                                "        value real,                    " +
                                "        quality integer);              "
                );
                st.execute(
                        "create table incoming_tsv(             " +
                                "        date_time integer primary key, " +
                                "        value real,                    " +
                                "        quality integer);              "
                );
            }
            if (intervalMinutes == 0) {
                throw new CoreException("Cannot yet store irregular time series");
            }
            else {
                Constants.REGULAR_STORE_RULE sr = Constants.REGULAR_STORE_RULE.valueOf(storeRule.toUpperCase());
                putRegularTimeSeriesValues(tsc, sr, conn, tmp);
            }
        }
    }

    static boolean isIrregular(@NotNull String name) throws CoreException {
        String[] parts = name.split("\\|", -1);
        String intervalName = parts[3];
        int minutes = Interval.getIntervalMinutes(intervalName);
        return minutes == 0;
    }

    static void setBlockInfo(
            @NotNull TsvInfo blockInfo,
            int count,
            long firstTime,
            long lastTime,
            @NotNull double[] values,
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
        }
        else {
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
    static void putRegularTimeSeriesValues(
            @NotNull TimeSeriesContainer tsc,
            Constants.REGULAR_STORE_RULE storeRule,
            Connection conn,
            Connection tmp
    ) throws CoreException, SQLException, EncodedDateTimeException, IOException {
        // parse the name
        String[] parts = tsc.fullName.split("\\|", -1);
        String intervalName = parts[3];
        // verify the interval
        int intervalMinutes = tsc.getTimeIntervalSeconds() / 60;
        if (intervalMinutes < 43200) {
            for (int i = 1; i < tsc.numberValues; ++i) {
                if (tsc.times[i] - tsc.times[i - 1] != intervalMinutes) {
                    throw new CoreException("Time series is not regular interval");
                }
            }
        }
        else {
            HecTime t = new HecTime(tsc.getStartTime());
            for (int i = 0; ; t.increment(1, intervalMinutes), ++i) {
                if (t.value() != tsc.times[i]) {
                    throw new CoreException("Time series is not regular interval");
                }
            }
        }
        // handle times
        if (tsc.timeZoneID != null && !tsc.timeZoneID.equals("UTC")) {
            throw new CoreException("Cannot yet handle non-UTC times");
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
                throw new CoreException("Expected interval offset of " + existingOffsetMinutes + ", got " + tscOffsetMinutes);
            }
        }
        // prepare arrays
        long[] encodedTimes = new long[tsc.numberValues];
        for (int i = 0; i < tsc.numberValues; ++i) {
            encodedTimes[i] = EncodedDateTime.encodeDateTime(tsc.times[i]);
        }
        double[] values = Arrays.copyOf(tsc.values, tsc.numberValues);
        int[] qualities = null;
        if (tsc.quality != null && Arrays.stream(tsc.quality).anyMatch(q -> q != 0)) {
            qualities = Arrays.copyOf(tsc.quality, tsc.numberValues);
        }
        else {
            qualities = new int[tsc.numberValues];
            Arrays.fill(qualities, 0);
        }
        // determine blocks
        long[] encodedBlockDates = getBlockStartDates(tsc.startHecTime, tsc.endHecTime, intervalName);
        long[] encodedBlockTimes = Arrays.stream(encodedBlockDates).map(d -> d * 1000000).toArray();
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
        byte[] blob;
        byte format = 9; // RTS
        byte version = 1;
        byte hasQuality = 0;
        for (int i = 0; i < encodedBlockDates.length - 1; ++i) {
            // retrieve any existing blob for the start date
            try (PreparedStatement ps = conn.prepareStatement(String.format(SQL_SELECT_TS_BLOCK, key))) {
                ps.setLong(1, encodedBlockDates[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    blob = rs.getBytes("data");
                }
            }
            long firstTime = EncodedDateTime.encodeDateTime(tsc.times[blockStarts[i]]);
            TsvInfo blockInfo = new TsvInfo();
            if (blob == null) {
                //----------------------//
                // record doesn't exist //
                //----------------------//
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
                setBlockInfo(
                        blockInfo,
                        blockCounts[i],
                        firstTime,
                        EncodedDateTime.incrementEncodedDateTime(firstTime, intervalMinutes, blockCounts[i]-1),
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
                // insert the blob
                try (PreparedStatement ps = conn.prepareStatement(
                        "insert " +
                                "into tsv " +
                                "    (time_series, " +
                                "     block_start_date, " +
                                "     data " +
                                "    ) " +
                                "values (?, ?, ?) "
                )) {
                    ps.setLong(1, key);
                    ps.setLong(2, encodedBlockDates[i]);
                    ps.setBytes(3, blob);
                    ps.executeUpdate();
                }
                // insert the block info
                try (PreparedStatement ps = conn.prepareStatement(
                        "insert " +
                                "into tsv_info " +
                                "    (time_series, " +
                                "     block_start_date, " +
                                "     value_count, " +
                                "     first_time, " +
                                "     last_time, " +
                                "     min_value, " +
                                "     max_value, " +
                                "     last_update " +
                                "    ) " +
                                "values (?, ?, ?, ? , ?, ?, ?, ?)"
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
            else {
                //-------------------//
                // record does exist //
                //-------------------//
                try (Statement st = tmp.createStatement()) {
                    st.executeUpdate(
                            "delete from incoming_tsv;" +
                                    "delete from existing_tsv;");
                }
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
                if (dataType != 9) { // RTS
                    throw new CoreException("Expected data type of 9 (RTS), got " + dataType);
                }
                byte dataTypeVersion = buf.get(bufPosition);
                bufPosition += Byte.BYTES;
                if (dataTypeVersion != 1) {
                    throw new CoreException("Don't know how to decode RTS version " + dataTypeVersion);
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
                    throw new CoreException(String.format(
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
                else {
                    Arrays.fill(existingQualities, 0);
                }
                // put existing data into tmp table
                long[] existingEncodedTimes = EncodedDateTime.makeRegularEncodedDateTimeArray(encodedFirstTime,
                        valueCount, intervalMinutes);
                try (PreparedStatement ps = tmp.prepareStatement("insert into existing_tsv (date_time, value, " +
                        "quality) values (?, ?, ?)")) {
                    for (int j = 0; j < valueCount; ++j) {
                        ps.setLong(1, existingEncodedTimes[j]);
                        ps.setDouble(2, existingValues[j]);
                        ps.setInt(3, existingQualities[j]);
                        ps.executeUpdate();
                    }
                }
                int count;
                // put incoming data into tmp table
                long[] incomingEncodedTimes =
                        EncodedDateTime.makeRegularEncodedDateTimeArray(EncodedDateTime.encodeDateTime(tsc.times[blockStarts[i]]),
                                blockCounts[i], intervalMinutes);
                try (PreparedStatement ps = tmp.prepareStatement("insert into incoming_tsv (date_time, value, " +
                        "quality) values (?, ?, ?)")) {
                    for (int j = blockStarts[i]; j < blockStarts[i] + blockCounts[i]; ++j) {
                        ps.setLong(1, incomingEncodedTimes[j-blockStarts[i]]);
                        ps.setDouble(2, tsc.values[j]);
                        ps.setInt(3, tsc.quality == null ? 0 : tsc.quality[j]);
                        ps.executeUpdate();
                    }
                }
                try (PreparedStatement ps = tmp.prepareStatement("select min(date_time) from existing_tsv")) {
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        encodedFirstTime = rs.getLong("min(date_time)");
                    }
                }
                // merge the data according to the store rule
                switch (storeRule) {
                    case REPLACE_ALL:
                    case REPLACE_ALL_CREATE:
                    case REPLACE_ALL_DELETE:
//                        try (PreparedStatement ps = tmp.prepareStatement("delete from existing_tsv where date_time " +
//                                "between ? and ?")) {
//                            ps.setLong(1, incomingEncodedTimes[0]);
//                            ps.setLong(2, incomingEncodedTimes[incomingEncodedTimes.length - 1]);
//                            ps.executeUpdate();
//                        }
//                        try (PreparedStatement ps = tmp.prepareStatement(
//                                "insert              " +
//                                        "  into existing_tsv " +
//                                        "       (date_time,  " +
//                                        "        value,      " +
//                                        "        quality     " +
//                                        "       )            " +
//                                        "select date_time,   " +
//                                        "       value,       " +
//                                        "       quality      " +
//                                        "  from incoming_tsv "
//                        )) {
//                            ps.executeUpdate();
//                        }
                        break;
                    case DO_NOT_REPLACE:
                        try (PreparedStatement ps = tmp.prepareStatement(
                                "insert                                                      " +
                                        "  into existing_tsv                                         " +
                                        "       (date_time,                                          " +
                                        "        value,                                              " +
                                        "        quality                                             " +
                                        "       )                                                    " +
                                        "select date_time,                                           " +
                                        "       value,                                               " +
                                        "       quality                                              " +
                                        "  from incoming_tsv                                         " +
                                        " where date_time not in (select date_time from existing_tsv)"
                        )) {
                            ps.executeUpdate();
                        }
                        break;
                    case REPLACE_MISSING_VALUES_ONLY:
                        try (PreparedStatement ps = tmp.prepareStatement(
                                "delete                              " +
                                        "  from existing_tsv                 " +
                                        " where date_time between ? and ?    " +
                                        "   and (value=? or (quality & 7)=5) "
                        )) {
                            ps.setLong(1, incomingEncodedTimes[0]);
                            ps.setLong(2, incomingEncodedTimes[incomingEncodedTimes.length - 1]);
                            ps.setDouble(3, UNDEFINED_DOUBLE);
                            ps.executeUpdate();
                        }
                        try (PreparedStatement ps = tmp.prepareStatement(
                                "insert                                                     " +
                                        "  into existing_tsv                                        " +
                                        "       (date_time,                                         " +
                                        "        value,                                             " +
                                        "        quality                                            " +
                                        "       )                                                   " +
                                        "select date_time,                                          " +
                                        "       value,                                              " +
                                        "       quality                                             " +
                                        "  from incoming_tsv                                        " +
                                        "where date_time not in (select date_time from existing_tsv)"
                        )) {
                            ps.executeUpdate();
                        }
                        break;
                    case REPLACE_WITH_NON_MISSING:
                        try (PreparedStatement ps = tmp.prepareStatement(
                                "with non_missing_times as                                    " +
                                        "(select date_time                                            " +
                                        "   from incoming_tsv                                         " +
                                        "  where value <> ? and (quality & 7) <> 5                    " +
                                        ")                                                            " +
                                        "delete                                                       " +
                                        "  from existing_tsv                                          " +
                                        " where date_time in (select date_time from non_missing_times)"
                        )) {
                            ps.setDouble(1, UNDEFINED_DOUBLE);
                            ps.executeUpdate();
                        }
                        try (PreparedStatement ps = tmp.prepareStatement(
                                "with non_missing_times as                                    " +
                                        "(select date_time                                            " +
                                        "   from incoming_tsv                                         " +
                                        "  where value <> ? and (quality & 7) <> 5                    " +
                                        ")                                                            " +
                                        "insert                                                       " +
                                        "  into existing_tsv                                          " +
                                        "       (date_time,                                           " +
                                        "        value,                                               " +
                                        "        quality                                              " +
                                        "       )                                                     " +
                                        "select date_time,                                            " +
                                        "       value,                                                " +
                                        "       quality                                               " +
                                        "  from incoming_tsv                                          " +
                                        " where date_time in (select date_time from non_missing_times)"
                        )) {
                            ps.setDouble(1, UNDEFINED_DOUBLE);
                            ps.executeUpdate();
                        }
                        break;
                    default:
                        throw new CoreException("Unexpected store rule: " + storeRule.name());
                }
                switch (storeRule) {
                    case REPLACE_ALL:
                    case REPLACE_ALL_CREATE:
                    case REPLACE_ALL_DELETE:
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
                        TsvData merged = new TsvData();
                        mergeReplaceAll(
                                incoming,
                                existing,
                                merged
                        );
                        // create a new blob from the merged data
                        count = merged.count;
                        hasQuality = 0;
                        if (merged.qualities != null) {
                            for (int j = 0; j < merged.count; ++j) {
                                if (merged.qualities[j] != 0) {
                                    hasQuality = 1;
                                    break;
                                }
                            }
                        }
                        setBlockInfo(
                                blockInfo,
                                count,
                                merged.times[0],
                                merged.times[count-1],
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
                            double value = merged.values[j];
                            int quality = hasQuality == 1 ? merged.qualities[j] : 0;
                            if (mustConvert
                                    && value != UNDEFINED_DOUBLE
                                    && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                                    && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE) {
                                value = Unit.performConversion(value, unitConvFactor[0], unitConvOffset[0], unitConvFunction[0]);
                            }
                            buf.putDouble(value);
                        }
                        if (hasQuality == 1) {
                            for (int j = 0; j < count; ++j) {
                                buf.putInt(merged.qualities[j]);
                            }
                        }
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
                                "update tsv_info\n" +
                                        "   set value_count = ?,\n" +
                                        "       first_time  = ?,\n" +
                                        "       last_time   = ?,\n" +
                                        "       min_value   = ?,\n" +
                                        "       max_value   = ?,\n" +
                                        "       last_update = ?\n" +
                                        " where time_series = ?\n" +
                                        "   and block_start_date = ?"
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
                        break;
                    default:
                        // fill in any gaps in the merged data
                        try (PreparedStatement ps = tmp.prepareStatement("select count(*) from existing_tsv")) {
                            try (ResultSet rs = ps.executeQuery()) {
                                rs.next();
                                count = rs.getInt("count(*)");
                            }
                        }
                        if (count == 0) {
                            // delete this record
                            try (PreparedStatement ps = conn.prepareStatement("delete from tsv where times_series=? and " +
                                    "block_start_date=?")) {
                                ps.setLong(1, key);
                                ps.setLong(2, encodedBlockTimes[i]);
                                ps.executeUpdate();
                            }
                        }
                        else {
                            // create a new blob from the merged data
                            existingEncodedTimes = new long[count];
                            // check for gaps within the block
                            List<Integer> gaps = new ArrayList<>();
                            try (PreparedStatement ps = tmp.prepareStatement("select date_time from existing_tsv order by " +
                                    "date_time")) {
                                try (ResultSet rs = ps.executeQuery()) {
                                    int lastGap = -2;
                                    long expectedEncodedTime;
                                    for (int j = 0; rs.next(); ++j) {
                                        existingEncodedTimes[j] = rs.getLong(1);
                                        if (j > 0) {
                                            expectedEncodedTime =
                                                    EncodedDateTime.incrementEncodedDateTime(
                                                            existingEncodedTimes[j - 1],
                                                            intervalMinutes,
                                                            1);
                                            if (j > lastGap + 2 && existingEncodedTimes[j] > expectedEncodedTime) {
                                                lastGap = j - 1;
                                                gaps.add(lastGap);
                                            }
                                        }
                                    }
                                }
                            }
                            // fill in any gaps within the block
                            if (!gaps.isEmpty()) {
                                // add the missing values
                                try (PreparedStatement ps = tmp.prepareStatement("insert into existing_tsv (date_time, value," +
                                        " quality) values (?, ?, ?)")) {
                                    for (Integer gap : gaps) {
                                        int g = gap;
                                        long[] fillTimes =
                                                EncodedDateTime.makeRegularEncodedDateTimeArray(existingEncodedTimes[g],
                                                        existingEncodedTimes[g + 1], intervalMinutes);
                                        for (int j = 1; j < fillTimes.length; ++j) {
                                            if (fillTimes[j] < existingEncodedTimes[g + 1]) {
                                                ps.setLong(1, fillTimes[j]);
                                                ps.setDouble(2, UNDEFINED_DOUBLE);
                                                ps.setInt(3, 0);
                                                ps.executeUpdate();
                                            }
                                        }
                                    }
                                }
                                // re-query for count
                                try (PreparedStatement ps = tmp.prepareStatement("select count(*) from existing_tsv")) {
                                    try (ResultSet rs = ps.executeQuery()) {
                                        rs.next();
                                        count = rs.getInt("count(*)");
                                    }
                                }
                            }
                            // get the merged values and qualities
                            try (PreparedStatement ps = tmp.prepareStatement("select min(date_time) from existing_tsv")) {
                                try (ResultSet rs = ps.executeQuery()) {
                                    rs.next();
                                    encodedFirstTime = rs.getLong("min(date_time)");
                                }
                            }
                            blockInfo.valueCount = count;
                            blockInfo.firstTime = encodedFirstTime;
                            blockInfo.lastTime = EncodedDateTime.incrementEncodedDateTime(
                                    encodedFirstTime, intervalMinutes,
                                    count-1);
                            values = new double[count];
                            qualities = new int[count];
                            try (PreparedStatement ps = tmp.prepareStatement("select value, quality from existing_tsv order " +
                                    "by date_time")) {
                                try (ResultSet rs = ps.executeQuery()) {
                                    for (int j = 0; rs.next(); ++j) {
                                        values[j] = rs.getDouble("value");
                                        qualities[j] = rs.getInt("quality");
                                    }
                                }
                            }
                            // create a new blob from the merged data
                            setBlockInfo(
                                    blockInfo,
                                    count,
                                    encodedFirstTime,
                                    EncodedDateTime.incrementEncodedDateTime(encodedFirstTime, intervalMinutes, count-1),
                                    values,
                                    qualities,
                                    0
                            );
                            hasQuality = 0;
                            for (int j = 0; j < qualities.length; ++j) {
                                if (qualities[j] != 0) {
                                    hasQuality = 1;
                                    break;
                                }
                            }
                            size = Byte.BYTES        // data type
                                    + Byte.BYTES             // data type version
                                    + Integer.BYTES          // value count
                                    + Byte.BYTES             // has quality?
                                    + Long.BYTES             // date/time of first value
                                    + count * Double.BYTES   // values
                                    + count * Integer.BYTES; // quality codes
                            buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                            buf.put(format);
                            buf.put(version);
                            buf.putInt(count);
                            buf.put(hasQuality);
                            buf.putLong(encodedFirstTime);
                            for (int j = 0; j < count; ++j) {
                                double value = values[j];
                                int quality = hasQuality == 1 ? qualities[j] : 0;
                                if (mustConvert
                                        && value != UNDEFINED_DOUBLE
                                        && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                                        && (quality & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE) {
                                    value = Unit.performConversion(value, unitConvFactor[0], unitConvOffset[0], unitConvFunction[0]);
                                }
                                buf.putDouble(value);
                            }
                            if (hasQuality == 1) {
                                for (int j = 0; j < count; ++j) {
                                    buf.putInt(qualities[j]);
                                }
                            }
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
                                    "update tsv_info\n" +
                                            "   set value_count = ?,\n" +
                                            "       first_time  = ?,\n" +
                                            "       last_time   = ?,\n" +
                                            "       min_value   = ?,\n" +
                                            "       max_value   = ?,\n" +
                                            "       last_update = ?\n" +
                                            " where time_series = ?\n" +
                                            "   and block_start_date = ?"
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
    }

    @NotNull
    static long[] getBlockStartDates(HecTime startTime, HecTime endTime, String intervalName) throws CoreException,
            EncodedDateTimeException {
        long[] blockStartDates;
        int[] julFirstBlock = {0, 0, 0};
        int[] julLastBlock = {0, 0, 0};
        int[] intvlCode = {0};
        int blockMinutes;
        if (DSSPathname.getTSBlockInfo(intervalName, startTime.julian(), julFirstBlock, intvlCode, null, null) != 0) {
            throw new CoreException("Error getting block start for " + startTime + " and " + intervalName);
        }
        if (DSSPathname.getTSBlockInfo(intervalName, endTime.julian(), julLastBlock, intvlCode, null, null) != 0) {
            throw new CoreException("Error getting block start for " + startTime + " and " + intervalName);
        }
        switch (intvlCode[0]) {
            case 1:
                blockMinutes = 1440;
                break;
            case 2:
                blockMinutes = 30 * 1440;
                break;
            case 3:
                blockMinutes = 365 * 1440;
                break;
            case 4:
                blockMinutes = 10 * 365 * 1440;
                break;
            case 5:
                blockMinutes = 100 * 365 * 1440;
                break;
            default:
                throw new CoreException("Unexpected block interval code: " + intvlCode[0]);
        }
        HecTime blockStart = new HecTime();
        blockStart.showTimeAsBeginningOfDay(true);
        blockStart.setYearMonthDay(julFirstBlock[0], julFirstBlock[1], julFirstBlock[2], 0);
        HecTime lastBlockStart = new HecTime();
        lastBlockStart.showTimeAsBeginningOfDay(true);
        lastBlockStart.setYearMonthDay(julLastBlock[0], julLastBlock[1], julLastBlock[2], 0);
        lastBlockStart.increment(1, blockMinutes);
        ArrayList<HecTime> hecTimes = new ArrayList<>();
        while (blockStart.lessThan(lastBlockStart)) {
            hecTimes.add(new HecTime(blockStart));
            blockStart.increment(1, blockMinutes);
        }
        hecTimes.add(new HecTime(blockStart));
        blockStartDates = new long[hecTimes.size()];
        for (int i = 0; i < blockStartDates.length; ++i) {
            blockStartDates[i] = EncodedDateTime.encodeDate(hecTimes.get(i));
        }
        return blockStartDates;
    }

    static String getBlockSizeForInterval(String name, Connection conn) throws SQLException {
        String blockSize;
        try (PreparedStatement ps = conn.prepareStatement(
                "select block_size from interval where name = ?"
        )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                blockSize = rs.getString("block_size");
            }
        }
        return blockSize;
    }

    @NotNull
    public static String getTimeSeriesName(@NotNull TimeSeriesContainer tsc) throws CoreException {
        String[] parts = tsc.fullName.split("/", -1);
        if (parts.length != 8) {
            throw new CoreException("Can't create name from " + tsc.fullName);
        }
        String location = parts[1].isEmpty() ? parts[2] : parts[1] + ":" + parts[2];
        String parameter = parts[3];
        String interval = parts[5];
        String version = parts[6];
        String paramType = tsc.type;
        String duration = paramType.startsWith("INST-") ? "0" : interval;
        return String.format("%s|%s|%s|%s|%s|%s", location, parameter, paramType, interval, duration, version);
    }

    public static long getTimeSeriesSpecKey(@NotNull String name, Connection conn) throws CoreException, SQLException {
        // name like "[ctx:]base-sub_loc|base-sub_param|param_type|intvl|dur|version"
        long key = -1;
        String[] parts = name.split("\\|");
        if (parts.length != 6) {
            throw new CoreException("Invalid time series name: " + name);
        }
        long locKey = Location.getLocationKey(parts[0], conn);
        if (locKey < 0) {
            return key;
        }
        long paramKey = Parameter.getParameterKey(parts[1], conn);
        if (paramKey < 0) {
            return key;
        }
        String intvlName = Interval.getInterval(parts[3]);
        String durName = Duration.getDuration(parts[4], conn);
        boolean nullKey;
        try (PreparedStatement ps = conn.prepareStatement(
                "select key " +
                        "from time_series " +
                        "where location = ? " +
                        "and parameter = ? " +
                        "and parameter_type = ? " +
                        "and interval = ? " +
                        "and duration = ? " +
                        "and version = ? "
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

    static long putTimeSeriesSpec(String name, Connection conn) throws CoreException, SQLException {
        // name like "[ctx:]base-sub_loc|base-sub_param|param_type|intvl|dur|version"
        long key = getTimeSeriesSpecKey(name, conn);
        if (key < 0) {
            String[] parts = name.split("\\|"); // if not 6 parts, with throw in getTimeSeriesSpecKey()
            long locKey = Location.putLocation(parts[0], conn);
            long paramKey = Parameter.putParameter(parts[1], conn);
            String intvlName = Interval.getInterval(parts[3]);
            String durName = Duration.getDuration(parts[4], conn);
            boolean nullKey;
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert " +
                            "into time_series " +
                            "(location, " +
                            "parameter, " +
                            "parameter_type, " +
                            "interval, " +
                            "duration, " +
                            "version " +
                            ") " +
                            "values (?, ?, ?, ?, ?, ?) "
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
                    Constants.SQL_SELECT_LAST_INSERT_ROWID
            )) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    key = rs.getLong(Constants.LAST_INSERT_ROWID);
                    nullKey = rs.wasNull();
                }
            }
            if (nullKey) {
                key = -1;
            }
        }
        return key;
    }

    public static void trimTimeSeriesContainer(@NotNull TimeSeriesContainer tsc) {
        int firstNonMissing = -1;
        int lastNonMissing = -1;
        for(int i = 0; i < tsc.numberValues; ++i) {
            if (tsc.values[i] != UNDEFINED_DOUBLE
                    && (tsc.quality == null
                    || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                    || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE))
            {
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
            for (int i = tsc.numberValues-1; i >= firstNonMissing; --i) {
                if (tsc.values[i] != UNDEFINED_DOUBLE
                        && (tsc.quality == null
                        || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE
                        || (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_REJECTED_VALUE))
                {
                    lastNonMissing = i;
                    break;
                }
            }
            if (firstNonMissing > 0 || lastNonMissing < tsc.numberValues - 1) {
                tsc.numberValues = lastNonMissing - firstNonMissing + 1;
                tsc.times = Arrays.copyOfRange(tsc.times, firstNonMissing, lastNonMissing+1);
                tsc.values = Arrays.copyOfRange(tsc.values, firstNonMissing, lastNonMissing+1);
                if (tsc.quality != null) {
                    tsc.quality = Arrays.copyOfRange(tsc.quality, firstNonMissing, lastNonMissing+1);
                }
                tsc.startHecTime.set(tsc.times[0]);
                tsc.endHecTime.set(tsc.times[tsc.numberValues-1]);
            }
        }
    }

    static void mergeReplaceAll(
            TsvData incoming,
            TsvData existing,
            TsvData merged
    ) {
        int count = 0;
        count += incoming.count == -1 ? incoming.times.length : incoming.count;
        count += existing.count == -1 ? existing.times.length : existing.count;
        merged.times = new long[count];
        merged.values = new double[count];
        merged.qualities = new int[count];

        int i = incoming.offset;
        int e = existing.offset;
        int m = 0;
        //--------------------------------------------------//
        // copy within limits of both incoming and existing //
        //--------------------------------------------------//
        while (i < incoming.offset + incoming.count && e < existing.offset + existing.count) {
            if (incoming.times[i] <= existing.times[e]) {
                while (incoming.times[i] <= existing.times[e]) {
                    merged.times[m] = incoming.times[i];
                    merged.values[m] = incoming.values[i];
                    merged.qualities[m] = incoming.qualities[i];
                    ++i;
                    ++m;
                }
                ++e;
            }
            if (existing.times[e] < incoming.times[i]) {
                while (existing.times[e] < incoming.times[i]) {
                    merged.times[m] = existing.times[i];
                    merged.values[m] = existing.values[i];
                    merged.qualities[m] = existing.qualities[i];
                    ++e;
                    ++m;
                }
                ++i;
            }
        }
        //------------------------------------------------//
        // fill remainder with whichever wasn't exhausted //
        //------------------------------------------------//
        while (i < incoming.offset + incoming.count) {
            merged.times[m] = incoming.times[i];
            merged.values[m] = incoming.values[i];
            merged.qualities[m] = incoming.qualities[i];
            ++i;
            ++m;
        }
        while (e < existing.offset + existing.count) {
            merged.times[m] = existing.times[e];
            merged.values[m] = existing.values[e];
            merged.qualities[m] = existing.qualities[e];
            ++e;
            ++m;
        }
        merged.offset = 0;
        merged.count = m;
        long ts2 = System.currentTimeMillis();
    }
}
