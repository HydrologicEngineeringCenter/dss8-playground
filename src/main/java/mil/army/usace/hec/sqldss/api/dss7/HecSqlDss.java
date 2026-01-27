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
import mil.army.usace.hec.sqldss.core.Constants;
import mil.army.usace.hec.sqldss.core.CoreException;
import mil.army.usace.hec.sqldss.core.EncodedDateTime;
import mil.army.usace.hec.sqldss.core.EncodedDateTimeException;
import mil.army.usace.hec.sqldss.core.Interval;
import mil.army.usace.hec.sqldss.core.TimeSeries;
import mil.army.usace.hec.sqldss.core.Unit;
import mil.army.usace.hec.sqldss.core.SqlDss;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static mil.army.usace.hec.sqldss.core.TimeSeries.*;

public class HecSqlDss implements AutoCloseable {

    SqlDss sqldss = null;

    public HecSqlDss(String fileName, String startTime, String endTime, boolean mustExist) throws CoreException,
            SQLException, IOException, EncodedDateTimeException {
        sqldss = new SqlDss(fileName, startTime, endTime, mustExist);
    }

    @NotNull
    @Contract("_ -> new")
    @Scriptable
    public static HecSqlDss open(String dssFileName) throws CoreException, SQLException, IOException,
            EncodedDateTimeException {
        return new HecSqlDss(dssFileName, null, null, false);
    }

    @NotNull
    @Contract("_, _ -> new")
    @Scriptable
    public static HecSqlDss open(String dssFileName, String timeWindow) throws CoreException, SQLException, IOException,
            EncodedDateTimeException, ApiException {
        HecTime startTime = new HecTime();
        HecTime endTime = new HecTime();
        if (0 != HecTime.getTimeWindow(timeWindow, startTime, endTime)) {
            throw new ApiException("Invalid time window: " + timeWindow);
        }
        return new HecSqlDss(dssFileName, startTime.dateAndTime(-13), endTime.dateAndTime(-13), false);
    }

    @NotNull
    @Contract("_, _, _ -> new")
    @Scriptable
    public static HecSqlDss open(String dssFileName, String startTime, String endTime) throws CoreException,
            SQLException, IOException, EncodedDateTimeException {
        return new HecSqlDss(dssFileName, startTime, endTime, false);
    }

    @NotNull
    @Contract("_, _ -> new")
    @Scriptable
    public static HecSqlDss open(String dssFileName, boolean mustExist) throws CoreException, SQLException, IOException,
            EncodedDateTimeException {
        return new HecSqlDss(dssFileName, null, null, mustExist);
    }

    @Scriptable
    public static boolean getDssFilename(String inputFilename, stringContainer outputFilename,
                                         boolean alwaysGetCanonical) throws IOException {
        return hec.heclib.dss.HecDss.getDssFilename(inputFilename, outputFilename, alwaysGetCanonical);
    }

    public boolean isOpened() {
        return sqldss.isOpen();
    }

    public DataContainer get(String pathname) throws ApiException, EncodedDateTimeException, CoreException,
            SQLException, IOException {
        return getInUnit(pathname, null);
    }

    public DataContainer getInUnit(String pathname, String unit) throws ApiException, EncodedDateTimeException, CoreException,
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
            String coreName = null;
            boolean exists = false;
            for (String pt: Constants.PARAMETER_TYPES) {
                coreName = ApiUtil.toCoreName(pathname, pt);
                if (TimeSeries.getTimeSeriesSpecKey(coreName, sqldss.getConnection()) > 0) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                throw new ApiException("No such time series: " + pathname);
            }
            tsc = getTimeSeriesValues(
                    coreName,
                    EncodedDateTime.toHecTime(startTime),
                    EncodedDateTime.toHecTime(endTime),
                    sqldss.getTrimMissing(),
                    sqldss.getConnection()
            );
            if (sqldss.getTrimMissing()) {
                TimeSeries.trimTimeSeriesContainer((tsc));
            }
            ApiUtil.updateTscToApi(tsc);
            if (unit != null) {
                Unit.convertUnits(tsc, unit, sqldss.getConnection());
            }
            return tsc;
        }
       else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    public DataContainer get(String pathname, String startTime, String endTime) throws Exception {
        return getInUnit(pathname, null, startTime, endTime);
    }

    public DataContainer getInUnit(String pathname, String unit, String startTime, String endTime) throws Exception {
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
            tsc = getTimeSeriesValues(
                    ApiUtil.toApiName(pathname),
                    startHecTime,
                    endHecTime,
                    sqldss.getTrimMissing(),
                    sqldss.getConnection()
            );
            ApiUtil.updateTscToApi(tsc);
            if (unit != null) {
                Unit.convertUnits(tsc, unit, sqldss.getConnection());
            }
            return tsc;
        }
       else {
            throw new ApiException("Pathname not recognized as a valid data type");
        }
    }

    public DataContainer get(String pathname, boolean readEntireSet) throws ApiException, CoreException, SQLException
            , EncodedDateTimeException, IOException {
        return getInUnit(pathname, null, readEntireSet);
    }

    public DataContainer getInUnit(String pathname, String unit, boolean readEntireSet) throws ApiException, CoreException, SQLException
            , EncodedDateTimeException, IOException {
        if (!sqldss.isOpen()) {
            throw new ApiException("File has been closed: " + sqldss.getFileName());
        }
        if (ApiUtil.isTimeSeriesApiName(pathname)) {
            if (readEntireSet) {
                TimeSeriesContainer tsc = getAllTimeSeriesValues(
                        ApiUtil.toCoreName(pathname),
                        sqldss.getTrimMissing(),
                        sqldss.getConnection());
                ApiUtil.updateTscToApi(tsc);
                if (unit != null) {
                    Unit.convertUnits(tsc, unit, sqldss.getConnection());
                }
                return tsc;
            }
           else {
                return getInUnit(pathname, unit);
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

    public void put(DataContainer dataContainer) throws ApiException, CoreException, SQLException,
            EncodedDateTimeException, IOException {
        if (dataContainer instanceof TimeSeriesContainer) {
            TimeSeriesContainer tsc2 = (TimeSeriesContainer) (((TimeSeriesContainer) dataContainer).clone());
            ApiUtil.updateTscToCore(tsc2);
            String storeRule = tsc2.getTimeIntervalSeconds() == 0 ?
                               sqldss.getIrregularStoreRuleValue().name() :
                               sqldss.getRegularStoreRuleValue().name();

            putTimeSeriesValues(tsc2, storeRule, sqldss.getConnection());
        }
       else {
            throw new ApiException("Cannot store " + dataContainer.getClass().getName() + " objects");
        }
    }

    public void put(DataContainerTransformer rsc) throws ApiException {
        throw new ApiException("Not Implemented");
    }

    public boolean getTimeSeriesExtents(String pathname, HecTime start, HecTime end) throws ApiException,
            EncodedDateTimeException, CoreException, SQLException, IOException {
        if (!recordExists(pathname)) {
            return false;
        }
        Long[] extents = new Long[2];
        TimeSeries.getTimeSeriesExtents(ApiUtil.toCoreName(pathname), extents, sqldss.getConnection());
        start.set(EncodedDateTime.toHecTime(extents[0]));
        end.set(EncodedDateTime.toHecTime(extents[1]));
        return true;
    }

    public boolean recordExists(String pathname) throws ApiException, CoreException, SQLException {
        String coreName = ApiUtil.toCoreName(pathname);
        long key = TimeSeries.getTimeSeriesSpecKey(coreName, sqldss.getConnection());
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

    public void done() throws CoreException {
        sqldss.close();
    }

    public boolean isRemote() {
        return false;
    }

    public String getFilename() {
        return sqldss.getFileName();
    }

    public String getStartTime() throws EncodedDateTimeException {
        Long startTime = sqldss.getStartTime();
        return startTime == null ? null : EncodedDateTime.toString(startTime);
    }

    public String getEndTime() throws EncodedDateTimeException {
        Long endTime = sqldss.getEndTime();
        return endTime == null ? null : EncodedDateTime.toString(endTime);
    }

    public boolean getTrimMissing() {
        return sqldss.getTrimMissing();
    }

    public int getRegularStoreMethod() {
        return sqldss.getRegularStoreRule();
    }

    public int getIrregularStoreMethod() {
        return sqldss.getIrregularStoreRule();
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

    public void setTimeWindow(String timeWindow) throws EncodedDateTimeException {
        HecTime startTime = new HecTime();
        HecTime endTime = new HecTime();
        HecTime.getTimeWindow(timeWindow, startTime, endTime);
        sqldss.setStartTime(EncodedDateTime.encodeDateTime(startTime));
        sqldss.setEndTime(EncodedDateTime.encodeDateTime(endTime));
    }

    public void setTimeWindow(String startTime, String endTime) throws EncodedDateTimeException {
        sqldss.setStartTime(EncodedDateTime.encodeDateTime(startTime));
        sqldss.setEndTime(EncodedDateTime.encodeDateTime(endTime));
    }

    public void setTrimMissing(boolean trimMissing) {
        sqldss.setTrimMissing(trimMissing);
    }

    public void setRegularStoreMethod(int method) {
        sqldss.setRegularStoreRule(method);
    }

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

    public HecMath read(String pathname) throws HecMathException, ApiException, CoreException, SQLException,
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

    public int write(HecMath mathGuy) {
        try {
            put(mathGuy.getData());
            return 0;
        }
        catch (ApiException | CoreException | SQLException | EncodedDateTimeException | HecMathException | IOException e) {
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
    public void close() throws CoreException {
        try {
            sqldss.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sqldss.close();
    }
}
