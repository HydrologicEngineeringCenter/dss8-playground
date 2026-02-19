package mil.army.usace.hec.sqldss.api.dss7;

import hec.heclib.dss.CondensedReference;
import hec.heclib.dss.DataReference;
import hec.heclib.util.HecTime;
import hec.heclib.util.stringContainer;
import hec.hecmath.HecMath;
import hec.hecmath.HecMathException;
import hec.hecmath.TimeSeriesMath;
import hec.io.DSSIdentifier;
import hec.io.DataContainer;
import hec.io.DataContainerTransformer;
import hec.io.TimeSeriesContainer;
import hec.lang.annotation.Scriptable;
import mil.army.usace.hec.sqldss.api.ApiException;
import mil.army.usace.hec.sqldss.core.*;
import mil.army.usace.hec.sqldss.core.SqlDssException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Class to implement hec.heclib.dss.HecDss API on SQLDSS
 */
public class HecDss implements AutoCloseable {

    SqlDss sqldss = null;

    /**
     * Constructor
     * @param fileName The SQLDSS file name
     * @param startTime The start of the default time window
     * @param endTime The end of the default time window
     * @param mustExist Whether the file must previously exist
     * @throws SqlDssException If thrown by {@link SqlDss#SqlDss(String, String, String, boolean)}
     * @throws SQLException If thrown by {@link SqlDss#SqlDss(String, String, String, boolean)}
     * @throws IOException If thrown by {@link SqlDss#SqlDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException If thrown by {@link SqlDss#SqlDss(String, String, String, boolean)}
     */
    public HecDss(String fileName, String startTime, String endTime, boolean mustExist) throws SqlDssException,
            SQLException, IOException, EncodedDateTimeException {
        sqldss = new SqlDss(fileName, startTime, endTime, mustExist);
    }

    /**
     * Factory method with file name only
     * @param dssFileName The SQLDSS file name
     * @return A new SQLDSS instance connected to the specified file name
     * @throws SqlDssException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws SQLException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws IOException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException If thrown by {@link #HecDss(String, String, String, boolean)}
     */
    @NotNull
    @Contract("_ -> new")
    @Scriptable
    public static HecDss open(String dssFileName) throws SqlDssException, SQLException, IOException,
            EncodedDateTimeException {
        return new HecDss(dssFileName, null, null, false);
    }

    /**
     * Factory method with file name and default time window
     * @param dssFileName The SQLDSS file name
     * @param timeWindow The time window string
     * @return A new SQLDSS instance connected to the specified file name
     * @throws SqlDssException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws SQLException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws IOException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException If thrown by {@link #HecDss(String, String, String, boolean)}
     */
    @NotNull
    @Contract("_, _ -> new")
    @Scriptable
    public static HecDss open(String dssFileName, String timeWindow) throws SqlDssException, SQLException, IOException,
            EncodedDateTimeException, ApiException {
        HecTime startTime = new HecTime();
        HecTime endTime = new HecTime();
        if (0 != HecTime.getTimeWindow(timeWindow, startTime, endTime)) {
            throw new ApiException("Invalid time window: " + timeWindow);
        }
        return new HecDss(dssFileName, startTime.dateAndTime(-13), endTime.dateAndTime(-13), false);
    }

    /**
     * Factory method with file name and default time window
     * @param dssFileName The SQLDSS file name
     * @param startTime The start of the default time window
     * @param endTime The end of the default time window
     * @return A new SQLDSS instance connected to the specified file name
     * @throws SqlDssException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws SQLException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws IOException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException If thrown by {@link #HecDss(String, String, String, boolean)}
     */
    @NotNull
    @Contract("_, _, _ -> new")
    @Scriptable
    public static HecDss open(String dssFileName, String startTime, String endTime) throws SqlDssException,
            SQLException, IOException, EncodedDateTimeException {
        return new HecDss(dssFileName, startTime, endTime, false);
    }

    /**
     * Factory method with file name and previous existence flag
     * @param dssFileName The SQLDSS file name
     * @param mustExist Whether the file must previously exist
     * @return A new SQLDSS instance connected to the specified file name
     * @throws SqlDssException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws SQLException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws IOException If thrown by {@link #HecDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException If thrown by {@link #HecDss(String, String, String, boolean)}
     */
    @NotNull
    @Contract("_, _ -> new")
    @Scriptable
    public static HecDss open(String dssFileName, boolean mustExist) throws SqlDssException, SQLException, IOException,
            EncodedDateTimeException {
        return new HecDss(dssFileName, null, null, mustExist);
    }

    /**@Scriptable
     * Generate a normalized file name ending with ".dss"
     * @param inputFilename The relative or absolute path of a DSS file, with or without the extension
     * @param outputFilename The (possibly absolute) path of the DSS file with the extension
     * @param alwaysGetCanonical Specifies whether to retrieve the absolute path for the DSS file even if the file doesn't exist
     * @return Whether the DSS file exists
     * @throws IOException
     */
    public static boolean getDssFilename(String inputFilename, stringContainer outputFilename,
                                         boolean alwaysGetCanonical) throws IOException {
        return hec.heclib.dss.HecDss.getDssFilename(inputFilename, outputFilename, alwaysGetCanonical);
    }

    /**
     * @return Whether the DSS file is open (i.e., has not been closed)
     */
    public boolean isOpened() {
        return sqldss.isOpen();
    }

    /**
     * Retrieve data in the default retrieval unit for the parameter of the data. For time series, the time window
     * retrieved is the default time window, if not null; otherwise the time window is determined by the D pathname part,
     * if present
     * @param pathname The pathname of the data to retrieve
     * @return The retrieved data
     * @throws ApiException If thrown in {@link #get(String, String)}
     * @throws EncodedDateTimeException If thrown in {@link #get(String, String)}
     * @throws SqlDssException If thrown in {@link #get(String, String)}
     * @throws SQLException If thrown in {@link #get(String, String)}
     * @throws IOException If thrown in {@link #get(String, String)}
     */
    public DataContainer get(String pathname) throws ApiException, EncodedDateTimeException, SqlDssException,
            SQLException, IOException {
        return get(pathname, null);
    }

    /**
     * Retrieve data in a specified unit. For time series, the time window retrieved is the default time window, if not
     * null; otherwise the time window is determined by the D pathname part, if present
     * @param pathname The pathname of the data to retrieve
     * @param unit The unit to retrieve the data in
     * @return The retrieved data
     * @throws ApiException If <ul>
     *     <li>the underlying SqlDss object has been closed</li>
     *     <li>thrown by {@link ApiUtil#isTimeSeriesApiName(String)} or {@link ApiUtil#toSqlDssName(String)}</li>
     *     <li>the default time window is null and the time series pathname has an empty D part</li>
     *     <li>no such time series exists</li>
     * </ul>
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link EncodedDateTime#encodeDateTime(String)}</li>
     *     <li>{@link EncodedDateTime#incrementEncodedDateTime(long, int, int)}</li>
     *     <li>{@link EncodedDateTime#addMinutes(long, int)}</li>
     *     <li>{@link SqlDss#retrieveTimeSeries(String, Long, Long, String, Boolean)}</li>
     * </ul>
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link Interval#getBlockSizeMinutes(String)}</li>
     *     <li>{@link Interval#getIntervalMinutes(String)}</li>
     *     <li>{@link TimeSeries#getTimeSeriesSpecKey(String, Connection)}</li>
     *     <li>{@link SqlDss#retrieveTimeSeries(String, Long, Long, String, Boolean)}</li>
     * </ul>
     * @throws SQLException If SQL error
     * @throws IOException If thrown by {@link SqlDss#retrieveTimeSeries(String, Long, Long, String, Boolean)}
     */
    public DataContainer get(String pathname, String unit) throws ApiException, EncodedDateTimeException, SqlDssException,
            SQLException, IOException {
        if (!sqldss.isOpen()) {
            throw new ApiException("File has been closed: " + sqldss.getFileName());
        }
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            TimeSeriesContainer tsc;
            Long startTime = sqldss.getStartTime();
            Long endTime = sqldss.getEndTime();
            if (startTime == null) {
                String[] parts = pathname.split("/", -1);
                if (parts[4].isEmpty()) {
                    throw new ApiException("No implicit or explicit time window, and no D pathname part");
                }
                startTime = EncodedDateTime.encodeDateTime(parts[4]);
                endTime = EncodedDateTime.incrementEncodedDateTime(
                        startTime,
                        Interval.getBlockSizeMinutes(parts[5]),
                        1);
                startTime = EncodedDateTime.addMinutes(startTime, Interval.getIntervalMinutes(parts[5]));
            }
            String sqlDssName = null;
            boolean exists = false;
            for (String pt: Constants.PARAMETER_TYPES) {
                sqlDssName = ApiUtil.toSqlDssName(pathname, pt);
                if (TimeSeries.getTimeSeriesSpecKey(sqlDssName, sqldss.getConnection()) > 0) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                throw new ApiException("No such time series: " + pathname);
            }
            tsc = sqldss.retrieveTimeSeries(
                    sqlDssName,
                    startTime,
                    endTime,
                    unit,
                    null
            );
            return tsc;
        }
       else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    /**
     * Retrieves data for a specified time window
     * @param pathname The pathname of the data to retrieve
     * @param startTime The start of the time window (overrides any default time window)
     * @param endTime The end of the time window (overrides any default time window()
     * @return The retrieved data
     * @throws Exception If: <ul>
     *     <li>thrown by {@link ApiUtil#isTimeSeriesApiName(String)}</li>
     *     <li>thrown by {@link SqlDss#getEffectiveRetrieveUnit(String)}</li>
     *     <li>thrown by {@link #get(String, String, String, String)}</li>
     *     <li><code>pathname</code> is not recognized as a valid record type</li>
     * </ul>
     */
    public DataContainer get(String pathname, String startTime, String endTime) throws Exception {
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            String parameter = pathname.split("/", -1)[3];
            String unit = sqldss.getEffectiveRetrieveUnit(parameter);
            return get(pathname, unit, startTime, endTime);
        }
        else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    /**
     * Retrieves data for a specified time window in a specfied unit
     * @param pathname The pathname of the data to retrieve
     * @param unit The unit to retrieve the data in
     * @param startTime The start of the time window (overrides any default time window)
     * @param endTime The end of the time window (overrides any default time window()
     * @return The retrieved data
     * @throws Exception If: <ul>
     *     <li>the underlying SqlDss object has been closed</li>
     *     <li>thrown by {@link ApiUtil#isTimeSeriesApiName(String)}</li>
     *     <li>the specified time window is null and the time series pathname has an empty D part</li>
     *     <li>thrown by {@link EncodedDateTime#encodeDate(int)}</li>
     *     <li>thrown by {@link Interval#getBlockSizeMinutes(String)}</li>
     *     <li>thrown by {@link EncodedDateTime#toHecTime(long)}</li>
     *     <li>thrown by {@link ApiUtil#toApiName(String)}</li>
     *     <li><code>pathname</code> is not recognized as a valid record type</li>
     * </ul>
     */
    public DataContainer get(String pathname, String unit, String startTime, String endTime) throws Exception {
        if (!sqldss.isOpen()) {
            throw new ApiException("File has been closed: " + sqldss.getFileName());
        }
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            HecTime startHecTime;
            HecTime endHecTime;
            TimeSeriesContainer tsc;
            if (startTime == null) {
                String[] parts = pathname.split("/", -1);
                if (parts[4].isEmpty()) {
                    throw new ApiException("No implicit or explicit time window, and no D pathname part");
                }
                long encodedStartTime = EncodedDateTime.encodeDate(parts[4]);
                long encodedEndTime = EncodedDateTime.incrementEncodedDateTime(
                        encodedStartTime,
                        Interval.getBlockSizeMinutes(parts[5]) - 1,
                        1);
                startHecTime = EncodedDateTime.toHecTime(encodedStartTime);
                endHecTime = EncodedDateTime.toHecTime(encodedEndTime);
            }
           else {
                startHecTime = new HecTime();
                endHecTime = new HecTime();
                startHecTime.set(startTime);
                endHecTime.set(endTime);
            }
            tsc = sqldss.retrieveTimeSeries(
                    ApiUtil.toApiName(pathname),
                    EncodedDateTime.encodeDateTime(startHecTime),
                    EncodedDateTime.encodeDateTime(endHecTime),
                    unit,
                    null
            );
            return tsc;
        }
       else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    public DataContainer get(String pathname, boolean readEntireSet) throws ApiException, SqlDssException, SQLException
            , EncodedDateTimeException, IOException {
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            String parameter = pathname.split("/", -1)[3];
            String unit = sqldss.getEffectiveRetrieveUnit(parameter);
            return get(pathname, unit, readEntireSet);
        }
        else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    /**
     * Retrieves data from a single record or entire data set in a specified unit
     * @param pathname The pathname to retrieve data for
     * @param unit The unit to retrieve data in
     * @param readEntireSet Whether to retrieve the entire data set
     * @return The retrieved data
     * @throws ApiException If <ul>
     *     <li>the underlying SqlDss object has been closed</li>
     *     <li>thrown by {@link ApiUtil#isTimeSeriesApiName(String)}</li>
     *     <li>thrown by {@link ApiUtil#toSqlDssName(TimeSeriesContainer)}</li>
     *     <li>thrown by {@link ApiUtil#updateTscToApi(TimeSeriesContainer)}</li>
     *     <li>thrown by {@link #get(String, String)}</li>
     *     <li><code>pathname</code> is not recognized as a valid record type</li>
     * </ul>
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link SqlDss#retrieveTimeSeries(String, String, Boolean)}</li>
     *     <li>{@link ApiUtil#updateTscToApi(TimeSeriesContainer)}</li>
     * </ul>
     * @throws SQLException On SQL error
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link SqlDss#retrieveTimeSeries(String, String, Boolean)}</li>
     *     <li>thrown by {@link #get(String, String)}</li>
     *      * </ul>
     * @throws IOException If thrown by {@link SqlDss#retrieveTimeSeries(String, Long, Long, String, Boolean)}
     */
    public DataContainer get(String pathname, String unit, boolean readEntireSet) throws ApiException, SqlDssException,
            SQLException, EncodedDateTimeException, IOException {
        if (!sqldss.isOpen()) {
            throw new ApiException("File has been closed: " + sqldss.getFileName());
        }
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            if (readEntireSet) {
                TimeSeriesContainer tsc = sqldss.retrieveTimeSeries(
                        ApiUtil.toSqlDssName(pathname),
                        unit,
                        null);
                ApiUtil.updateTscToApi(tsc);
                return tsc;
            }
           else {
                return get(pathname, unit);
            }
        }
       else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    public DataContainer get(DSSIdentifier dssId) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    protected DataContainer get(DataReference dataSet) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    /**
     * Stores a time series to the database
     * @param dataContainer The data to store
     * @throws ApiException If <ul>
     *     <li>thrown by {@link ApiUtil#updateTscToSqlDss(TimeSeriesContainer)}</li>
     *     <li><code>dataContainer</code> is not of a recognized record type</li>
     * </ul>
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link ApiUtil#updateTscToSqlDss(TimeSeriesContainer)}</li>
     *     <li>{@link SqlDss#storeTimeSeries(TimeSeriesContainer, String)}</li>
     * </ul>
     * @throws SQLException If SQL Error
     * @throws EncodedDateTimeException If thrown by {@link SqlDss#storeTimeSeries(TimeSeriesContainer, String)}
     */
    public void put(DataContainer dataContainer) throws ApiException, SqlDssException, SQLException,
            EncodedDateTimeException {
        if (dataContainer instanceof TimeSeriesContainer) {
            TimeSeriesContainer tsc2 = (TimeSeriesContainer) (((TimeSeriesContainer) dataContainer).clone());
            ApiUtil.updateTscToSqlDss(tsc2);
            String storeRule = tsc2.getTimeIntervalSeconds() == 0 ?
                               sqldss.getIrregularStoreRule().name() :
                               sqldss.getRegularStoreRule().name();

            sqldss.storeTimeSeries(tsc2, storeRule);
        }
       else {
            throw new ApiException("Cannot store " + dataContainer.getClass().getName() + " objects");
        }
    }

    public void put(DataContainerTransformer rsc) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    /**
     * Retrieves the time extents of a time series in the database
     * @param pathname The pathname of the time series (D pathname part is ignored)
     * @param start An HecTime object that is set to the first value time, if the time series exists
     * @param end An HecTime object that is set to the last value time, if the time series exists
     * @return Whether the time series exists
     * @throws ApiException If thrown by <ul>
     *     <li>{@link #recordExists(String)}</li>
     *     <li>{@link ApiUtil#toSqlDssName(TimeSeriesContainer)}</li>
     * </ul>
     * @throws EncodedDateTimeException If thrown by <ul>
     *     <li>{@link TimeSeries#getTimeSeriesExtents(String, Long[], Connection)}</li>
     *     <li>{@link EncodedDateTime#toHecTime(long)}</li>
     * </ul>
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link #recordExists(String)}</li>
     *     <li>{@link TimeSeries#getTimeSeriesExtents(String, Long[], Connection)}</li>
     * </ul>
     * @throws SQLException I SQL error
     */
    public boolean getTimeSeriesExtents(String pathname, HecTime start, HecTime end) throws ApiException,
            EncodedDateTimeException, SqlDssException, SQLException {
        if (!recordExists(pathname)) {
            return false;
        }
        Long[] extents = new Long[2];
        TimeSeries.getTimeSeriesExtents(ApiUtil.toSqlDssName(pathname), extents, sqldss.getConnection());
        start.set(EncodedDateTime.toHecTime(extents[0]));
        end.set(EncodedDateTime.toHecTime(extents[1]));
        return true;
    }

    /**
     * Retrieves whether a record exists in the database
     * @param pathname The pathname of the record
     * @return Whether the record exists
     * @throws ApiException If thrown by {@link ApiUtil#toSqlDssName(String)}
     * @throws SqlDssException If thrown by {@link TimeSeries#getTimeSeriesSpecKey(String, Connection)}
     * @throws SQLException If SQL error
     */
    public boolean recordExists(String pathname) throws ApiException, SqlDssException, SQLException {
        String sqlDssName = ApiUtil.toSqlDssName(pathname);
        long key = TimeSeries.getTimeSeriesSpecKey(sqlDssName, sqldss.getConnection());
        return key > 0;
    }

    public List<String> getPathnameList() throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<CondensedReference> getCondensedCatalog() throws ApiException {
        throw new ApiException("Not Implemented");
    }

    protected void checkForError() {
        // noop
    }

    public void forceMultiUserAccess() {
        // noop
    }

    /**
     * Closes the underlying SQLDSS file
     * @throws SqlDssException If thrown by {@link SqlDss#close()}
     * @throws SQLException If SQL error
     */
    public void done() throws SqlDssException, SQLException {
        sqldss.close();
    }

    /**
     * @return Whether the file is remote. Always <code>false</code> as SQLDSS doesn't support remote files
     */
    public boolean isRemote() {
        return false;
    }

    /**
     * @return The name of the underlying SQLDSS file
     */
    public String getFilename() {
        return sqldss.getFileName();
    }

    /**
     * @return The start of the default time window
     * @throws EncodedDateTimeException If thrown by {@link SqlDss#getStartTime()}
     */
    public String getStartTime() throws EncodedDateTimeException {
        Long startTime = sqldss.getStartTime();
        return startTime == null ? null : EncodedDateTime.toString(startTime);
    }

    /**
     * @return The end of the default time window
     * @throws EncodedDateTimeException If thrown by {@link SqlDss#getEndTime()}
     */
    public String getEndTime() throws EncodedDateTimeException {
        Long endTime = sqldss.getEndTime();
        return endTime == null ? null : EncodedDateTime.toString(endTime);
    }

    /**
     * @return The current trim-missing state
     */
    public boolean getTrimMissing() {
        return sqldss.getTrimMissing();
    }

    /**
     * @return The integer value of the default regular time series store rule
     */
    public int getRegularStoreMethod() {
        return sqldss.getRegularStoreRuleValue();
    }

    /**
     * @return The integer value of the default irregular time series store rule
     */
    public int getIrregularStoreMethod() {
        return sqldss.getIrregularStoreRuleValue();
    }

    public int recordsUpdated(long startTime, List<String> pathnames, List<Long> updateTimes,
                              List<Integer> recordTypes) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int recordsUpdated(HecTime startTime, List<String> pathnames, List<Long> updateTimes,
                              List<Integer> recordTypes) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int recordsUpdated(String startTime, List<String> pathnames, List<Long> updateTimes,
                              List<Integer> recordTypes) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> recordsUpdated(long startTime) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> recordsUpdated(HecTime startTime) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> recordsUpdated(String startTime) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    /**
     * Sets the default time window
     * @param timeWindow The time window string
     * @throws EncodedDateTimeException If thrown by {@link EncodedDateTime#encodeDateTime(String)}
     */
    public void setTimeWindow(String timeWindow) throws EncodedDateTimeException {
        HecTime startTime = new HecTime();
        HecTime endTime = new HecTime();
        HecTime.getTimeWindow(timeWindow, startTime, endTime);
        sqldss.setStartTime(EncodedDateTime.encodeDateTime(startTime));
        sqldss.setEndTime(EncodedDateTime.encodeDateTime(endTime));
    }

    /**
     * Sets the default time window
     * @param startTime The start of the default time window
     * @param endTime The end of the default time window
     * @throws EncodedDateTimeException If thrown by {@link EncodedDateTime#encodeDateTime(String)}
     */
    public void setTimeWindow(String startTime, String endTime) throws EncodedDateTimeException {
        sqldss.setStartTime(EncodedDateTime.encodeDateTime(startTime));
        sqldss.setEndTime(EncodedDateTime.encodeDateTime(endTime));
    }

    /**
     * Sets the trim-missing state
     * @param trimMissing the new trim-missing state
     */
    public void setTrimMissing(boolean trimMissing) {
        sqldss.setTrimMissing(trimMissing);
    }

    /**
     * Sets the default regular time series store rule
     * @param method The integer value of the new regular time series store rule
     */
    public void setRegularStoreMethod(int method) {
        sqldss.setRegularStoreRule(method);
    }

    /**
     * Sets the default irregular time series store rule
     * @param method The integer value of the new irregular time series store rule
     */
    public void setIrregularStoreMethod(int method) {
        sqldss.setIrregularStoreRule(method);
    }

    public int duplicateRecords(List<String> pathnameList, List<String> newPathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int duplicateRecords(String[] pathnameList, String[] newPathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int renameRecords(List<String> pathnameList, List<String> newPathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int renameRecords(String[] pathnameList, String[] newPathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int copyRecordsFrom(String toDSSFilename, List<String> pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int copyRecordsFrom(String toDSSFilename, String[] pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int copyRecordsInto(String fromDSSFilename, List<String> pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int copyRecordsInto(String fromDSSFilename, String[] pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int delete(List<String> pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int delete(String[] pathnameList) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public HecMath read(String pathname) throws HecMathException, ApiException, SqlDssException, SQLException,
            EncodedDateTimeException, IOException {
        TimeSeriesContainer tsc = (TimeSeriesContainer) get(pathname);
        return new TimeSeriesMath(tsc);
    }

    public HecMath read(String pathname, String timeWindow) throws Exception {
        HecTime startTime = new HecTime();
        HecTime endTime = new HecTime();
        HecTime.getTimeWindow(timeWindow, startTime, endTime);
        TimeSeriesContainer tsc = (TimeSeriesContainer) get(pathname, startTime.dateAndTime(-13),
                endTime.dateAndTime(-13));
        return new TimeSeriesMath(tsc);
    }

    public HecMath read(String pathname, String startTime, String endTime) throws Exception {
        TimeSeriesContainer tsc = (TimeSeriesContainer) get(pathname, startTime, endTime);
        return new TimeSeriesMath(tsc);
    }

    public HecMath read(String pathname, String startTime, String endTime, boolean trimMissing) throws Exception {
        boolean oldTrimMissing = sqldss.getTrimMissing();
        sqldss.setTrimMissing(trimMissing);
        TimeSeriesContainer tsc = (TimeSeriesContainer) get(pathname, startTime, endTime);
        sqldss.setTrimMissing(oldTrimMissing);
        return new TimeSeriesMath(tsc);
    }

    public int write(@NotNull HecMath mathGuy) {
        try {
            put(mathGuy.getData());
            return 0;
        }
        catch (ApiException | SqlDssException | SQLException | EncodedDateTimeException | HecMathException e) {
            return -1;
        }
    }

    public int write(DataContainerTransformer rating) throws HecMathException, ApiException {
        throw new ApiException("Not Implemented");
    }

    public int write(DataContainer dataContainer) throws HecMathException, ApiException {
        throw new ApiException("Not Implemented");
    }

    public int write(TimeSeriesMath mathGuy, String storeMethod) throws HecMathException, ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> getCatalogedPathnames() throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> getCatalogedPathnames(boolean forceNew) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> getCatalogedPathnames(String scanString) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> searchPathnames(String scanString) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public List<String> getCatalogedPathnames(String scanString, boolean forceNew) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public int hashCode() {
        throw new RuntimeException("Not Implemented");
    }

    public boolean equals(Object obj) {
        throw new RuntimeException("Not Implemented");
    }

    protected Object clone() throws CloneNotSupportedException {
        throw new RuntimeException("Not Implemented");
    }

    public String toString() {
        return String.format("{%s %s}", this.getClass().getName(), sqldss.getFileName());
    }

    public void commit() throws SQLException {
        sqldss.commit();
    }

    public void setAutoCommit(boolean state) throws SQLException {
        sqldss.setAutoCommit(state);
    }

    public boolean getAutoCommit() {
        return sqldss.getAutoCommit();
    }

    @Override
    public void close() throws SqlDssException, SQLException {
        try {
            sqldss.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sqldss.close();
    }
}
