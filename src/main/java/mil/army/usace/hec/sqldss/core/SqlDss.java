package mil.army.usace.hec.sqldss.core;

import hec.io.TimeSeriesContainer;
import mil.army.usace.hec.sqldss.core.init.Init;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static mil.army.usace.hec.sqldss.core.Constants.UNIT_SYSTEM.EN;

/**
 * The main class for SQLDSS access.
 *
 * Several default may be set for these objects that affect storing and retrieving of data:
 * <dl>
 *     <dt>Storing</dt>
 *     <dd>
 *         <dl>
 *             <dt>Auto-commit</dt>
 *             <dd>On by default. Minimizes data loss at the cost of performance. For high-volume data storing, <code>
 *                 setAutoCommit(false);</code> and call <code>commit():</code> occasionally
 *             </dd>
 *             <dt>Store Rules</dt>
 *             <dd>Separate store rules for regular and irregular time series control how data being stored is combined
 *             with data existing in the database</dd>
 *         </dl>
 *     </dd>
 *     <dt>Retrieving</dt>
 *     <dd>
 *         <dl>
 *             <dt>Trim Missing</dt>
 *             <dd>Calling <code>setTrimMissing(trim)</code> with <code>trim</code> set to <code>true</code> or
 *             <code>false</code> controls whether blocks of missing data at the beginning and end of a time window
 *             are removed or preserved when retrieving time series</dd>
 *             <dt>Default Unit System</dt>
 *             <dd>Originally set to <code>EN</code>, but settable to <code>SI</code>, the default unit system causes
 *             data to be retrieved in the default unit in that unit system for retrieved parameter</dd>
 *             <dt>Retrieve Units</dt>
 *             <dd>Calling <code>setRetrieveUnit(parameter, unit)</code> overrides the default unit system and
 *             causes data retrieved for the specified parameter to always be retrieved in the specified unit</dd>
 *         </dl>
 *     </dd>
 * </dl>
 */
public class SqlDss implements AutoCloseable {
    private Connection conn = null;
    private String fileName = null;
    private Long startTime = null;
    private Long endTime = null;
    private boolean trimMissing = false;
    private boolean autoCommit = true;
    private Constants.UNIT_SYSTEM unitSystem = EN;
    private Constants.REGULAR_STORE_RULE regularStoreRule = Constants.REGULAR_STORE_RULE.REPLACE_ALL;
    private Constants.IRREGULAR_STORE_RULE irregularStoreRule = Constants.IRREGULAR_STORE_RULE.REPLACE_ALL;
    private final Map<String, String> retrieveUnits = new HashMap<>();


    /**
     * Prevent zero-parameter construction
     */
    private SqlDss() {
        throw new AssertionError("Cannot instantiate without parameters");
    }

    /**
     * Factory method with file name only
     * @param fileName The name of the SQLDSS file
     * @return The SqlDss object
     * @throws SQLException If thrown by underlying constructor
     * @throws SqlDssException If thrown by underlying constructor
     * @throws IOException If thrown by underlying constructor
     * @throws EncodedDateTimeException If thrown by underlying constructor
     */
    @NotNull
    @Contract("_ -> new")
    public static SqlDss open(String fileName) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName);
    }

    /**
     * Factory method with file name and existence flag
     * @param fileName The name of the SQLDSS file
     * @param mustExist Whether the SQLDSS file must already exist
     * @return The SqlDss object
     * @throws SQLException If thrown by underlying constructor
     * @throws SqlDssException If thrown by underlying constructor
     * @throws IOException If thrown by underlying constructor
     * @throws EncodedDateTimeException If thrown by underlying constructor
     */
    @NotNull
    @Contract("_, _ -> new")
    public static SqlDss open(String fileName, boolean mustExist) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, mustExist);
    }

    /**
     * Factory method with file name and default time window
     * @param fileName The name of the SQLDSS file
     * @param startTime The start time of the default time window (UTC or time zone naive)
     * @param endTime The end time of the default time window (UTC or time zone naive)
     * @return The SqlDss object
     * @throws SQLException If thrown by underlying constructor
     * @throws SqlDssException If thrown by underlying constructor
     * @throws IOException If thrown by underlying constructor
     * @throws EncodedDateTimeException If thrown by underlying constructor
     */
    @NotNull
    @Contract("_, _, _ -> new")
    public static SqlDss open(String fileName, String startTime, String endTime) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, startTime, endTime);
    }

    /**
     * Factory method with file name, default time window, and existence flag
     * @param fileName The name of the SQLDSS file
     * @param startTime The start time of the default time window (UTC or time zone naive)
     * @param endTime The end time of the default time window (UTC or time zone naive)
     * @param mustExist Whether the SQLDSS file must already exist
     * @return The SqlDss object
     * @throws SQLException If thrown by underlying constructor
     * @throws SqlDssException If thrown by underlying constructor
     * @throws IOException If thrown by underlying constructor
     * @throws EncodedDateTimeException If thrown by underlying constructor
     */
    @NotNull
    @Contract("_, _, _, _ -> new")
    public static SqlDss open(String fileName, String startTime, String endTime, boolean mustExist) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, startTime, endTime, mustExist);
    }

    /**
     * Constructor with file name only
     * @param fileName The name of the SQLDSS file
     * @throws SQLException If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws SqlDssException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws IOException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     */
    public SqlDss(String fileName) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        this(fileName, null, null, false);
    }

    /**
     * Constructor with file name and existence flag
     * @param fileName The name of the SQLDSS file
     * @param mustExist Whether the SQLDSS file must already exist
     * @throws SQLException If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws SqlDssException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws IOException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     */
    public SqlDss(String fileName, boolean mustExist) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        this(fileName, null, null, mustExist);
    }

    /**
     * Constructor with file name and default time window
     * @param fileName The name of the SQLDSS file
     * @param startTime The start time of the default time window (UTC or time zone naive)
     * @param endTime The end time of the default time window (UTC or time zone naive)
     * @throws SQLException If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws SqlDssException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws IOException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     * @throws EncodedDateTimeException  If thrown by {@link #SqlDss(String, String, String, boolean)}
     */
    public SqlDss(String fileName, String startTime, String endTime) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        this(fileName, startTime, endTime, false);
    }

    /**
     * Constructor with file name, default time window, and existence flag
     * @param fileName The name of the SQLDSS file
     * @param startTime The start time of the default time window (UTC or time zone naive)
     * @param endTime The end time of the default time window (UTC or time zone naive)
     * @param mustExist Whether the SQLDSS file must already exist
     * @throws SQLException If SQL error
     * @throws SqlDssException If:
     * <ul>
     *     <li>only one of <code>startTime</code> and <code>endTime</code> is not null</li>
     *     <li>if <code>fileName</code> doesn't exist when <code>mustExist</code> is <code>true</code></li>
     * </ul>
     * @throws IOException If thrown by {@link Init#initializeDb(Connection)}
     * @throws EncodedDateTimeException If thrown by {@link EncodedDateTime#encodeDateTime(String)}
     */
    public SqlDss(String fileName, String startTime, String endTime, boolean mustExist) throws SQLException, SqlDssException, IOException, EncodedDateTimeException {
        Path filePath = Path.of(fileName);
        boolean exists = Files.exists(filePath);
        this.fileName = filePath.toString();
        if (startTime == null) {
            if (endTime != null) {
                throw new SqlDssException("End time may not be specified without start time");
            }
        }
        else {
            if (endTime == null) {
                throw new SqlDssException("Start time may not be specified without end time");
            }
        }
        this.startTime = startTime == null ? null : EncodedDateTime.encodeDateTime(startTime);
        this.endTime = endTime == null ? null : EncodedDateTime.encodeDateTime(endTime);
        if (mustExist && !exists) {
            throw new SqlDssException(String.format("File %s does not exist", fileName));
        }
        conn = DriverManager.getConnection("jdbc:sqlite:"+this.fileName);
        try (Statement st = conn.createStatement()) {
            st.execute("pragma page_size = 8192");
            st.execute("pragma foreign_keys = ON");
            if (!exists) {
                Init.initializeDb(conn);
            }
            BaseParameter.load(conn);
        }
    }

    /**
     * @return Whether the SQLDSS file is open
     */
    public boolean isOpen() {
        return conn != null;
    }

    /**
     * @return The JDBC connection
     */
    public Connection getConnection() {
        return conn;
    }

    /**
     * @return The SQLDSS file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Sets the start of the default time window
     * @param startTime The start of the default time window
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return The start of the default time window
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * Sets the end of the default time winoow
     * @param endTime The end of the default time window
     */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return The end of the default time window
     */
    public Long getEndTime() {
        return endTime;
    }

    /**
     * Set whether to trim blocks of missing values at the beginning and end of retrieved time series
     * @param trim The flag determining whether to trim
     */
    public void setTrimMissing(boolean trim) {
        trimMissing = trim;
    }

    /**
     * @return Whether SQLDSS will trim blocks of missing values at the beginning and end of retrieved time series
     */
    public boolean getTrimMissing() {
        return trimMissing;
    }

    /**
     * Sets the default store rule (merge algorithm) for regular time series
     * @param rule The default regular time series store rule
     */
    public void setRegularStoreRule(Constants.REGULAR_STORE_RULE rule) {
        regularStoreRule = rule;
    }

    /**
     * Sets the default store rule (merge algorithm) for regular time series
     * @param rule The default regular time series store rule
     */
    public void setRegularStoreRule(int rule) {
        regularStoreRule = Constants.REGULAR_STORE_RULE.fromCode(rule);
    }

    /**
     * Sets the default store rule (merge algorithm) for regular time series
     * @param rule The default regular time series store rule
     */
    public void setRegularStoreRule(@NotNull String rule) {
        regularStoreRule = Constants.REGULAR_STORE_RULE.valueOf(rule.toUpperCase());
    }

    /**
     * @return The integer value for default store rule for regular time series
     */
    public int getRegularStoreRuleValue() {
        return regularStoreRule.getCode();
    }

    /**
     * @return The default store rule for regular time series
     */
    public Constants.REGULAR_STORE_RULE getRegularStoreRule() {
        return regularStoreRule;
    }

    /**
     * Sets the default store rule (merge algorithm) for irregular time series
     * @param rule The default irregular time series store rule
     */
    public void setIrregularStoreRule(Constants.IRREGULAR_STORE_RULE rule) {
        irregularStoreRule = rule;
    }

    /**
     * Sets the default store rule (merge algorithm) for irregular time series
     * @param rule The default irregular time series store rule
     */
    public void setIrregularStoreRule(int rule) {
        irregularStoreRule = Constants.IRREGULAR_STORE_RULE.fromCode(rule);
    }

    /**
     * Sets the default store rule (merge algorithm) for irregular time series
     * @param rule The default irregular time series store rule
     */
    public void setIrregularStoreRule(@NotNull String rule) {
        irregularStoreRule = Constants.IRREGULAR_STORE_RULE.valueOf(rule.toUpperCase());
    }

    /**
     * @return The integer value for default store rule for irregular time series
     */
    public int getIrregularStoreRuleValue() {
        return irregularStoreRule.getCode();
    }

    /**
     * @return The default store rule for irregular time series
     */
    public Constants.IRREGULAR_STORE_RULE getIrregularStoreRule() {
        return irregularStoreRule;
    }

    /**
     * Performs a commit operation on the database connection
     * @throws SQLException If thrown by database connection
     */
    public void commit() throws SQLException {
        conn.commit();
    }

    /**
     * Sets the auto-commit state of the database connection
     * @param autoCommit The auto-commit state
     * @throws SQLException If thrown by database connection
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        conn.setAutoCommit(this.autoCommit);
    }

    /**
     * @return The auto-commit state of the database connection
     */
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * Sets the default unit system for data retrieval
     * @param unitSystem The default unit system
     */
    public void setUnitSystem(Constants.UNIT_SYSTEM unitSystem) {
        this.unitSystem = unitSystem;
    }

    /**
     * @return The default unit system for data retrieval
     */
    public Constants.UNIT_SYSTEM getUnitSystem() {
        return unitSystem;
    }

    /**
     * Remove all by-parameter retrieval unit specifications
     */
    public void clearRetrieveUnits() {
        retrieveUnits.clear();
    }

    /**
     * Remove the retrieval unit specification, if any, for the specified parameter
     * @param parameter The parameter to remove the retrival unit specification for
     * @throws SqlDssException If thrown by {@link Parameter#getParameter(String, Connection)}
     * @throws SQLException If thrown by {@link Parameter#getParameter(String, Connection)}
     */
    public void clearRetrieveUnits(String parameter) throws SqlDssException, SQLException {
        retrieveUnits.remove(Parameter.getParameter(parameter, getConnection()));
    }

    /**
     * Specifies the unit to use for retrieving data for the specified parameter
     * @param parameter The parameter the unit is used for
     * @param unit The unit to retrieve the parameter in
     * @throws SqlDssException If thrown by {@link Parameter#getParameter(String, Connection)}
     * @throws SQLException If thrown by {@link Parameter#getParameter(String, Connection)}
     */
    public void setRetrieveUnit(String parameter, String unit) throws SqlDssException, SQLException {
        retrieveUnits.put(
                Parameter.getParameter(parameter, getConnection()),
                Unit.getUnit(unit, getConnection())
        );
    }

    /**
     * Retrieves the unit used, if any, for retrieving data for the specified parameter.
     * @param parameter The parameter to retrieve the unit for
     * @return The retrieval unit, if any
     */
    public String getRetrieveUnit(String parameter) {
        return retrieveUnits.get(parameter);
    }

    /**
     * Retrieves the default unit for a specified parameter in the specified unit system
     * @param unitSystem The unit system to retrieve the unit for
     * @param parameter The parameter to retrieve the unit for
     * @return The unit
     * @throws SQLException If SQL error
     */
    public String getUnitSystemUnit(Constants.@NotNull UNIT_SYSTEM unitSystem, @NotNull String parameter) throws SQLException {
        String baseParameter = parameter.indexOf('-') == -1 ? parameter : parameter.split("-", 2)[0];
        String sql = String.format("select default_%s_unit from base_parameter where name = ?", unitSystem.name());
        try (PreparedStatement ps = getConnection().prepareStatement(sql)) {
            ps.setString(1, baseParameter);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    /**
     * Retrieves the effective data retrieval unit for a parameter. If a retrieval unit is specified for the parameter,
     * then it is returned. Otherwise, the default unit for the parameter in the default retrieval unit system is returned.
     * @param parameter The parameter to retrieve the effective data retrieval unit for
     * @return The effective data retrieval unit
     * @throws SQLException If thrown by {@link #getUnitSystemUnit(Constants.UNIT_SYSTEM, String)}
     */
    public String getEffectiveRetrieveUnit(String parameter) throws SQLException {
        String unit = getRetrieveUnit(parameter);
        if (unit == null) {
            unit = getUnitSystemUnit(unitSystem, parameter);
        }
        return unit;
    }

    /**
     * Closes the database connection
     * @throws SqlDssException If already closed
     * @throws SQLException If SQL error
     */
    @Override
    public void close() throws SqlDssException, SQLException {
        if (conn == null) {
            throw new SqlDssException("Already closed");
        }
        else {
            try {
                conn.close();
            }
            finally {
                conn = null;
            }
        }
    }

    /**
     * Retrieve a time series from the database
     * @param name The SQLDSS time series name
     * @param startTime The start of the time window. If null, the start of the default time window is used
     * @param endTime The end of the time window. If null, the end of the default time window is used
     * @param unit The unit to retrieve the values in. If null, the effective retrieval unit for the parameter is used
     * @param trimMissing Whether to blocks of trim consecutive missing values from the start and end of the retrieved data.
     *                    If null, the value of {@link #getTrimMissing()} is used
     * @return The time series
     * @throws SqlDssException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws SQLException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws IOException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     */
    public TimeSeriesContainer retrieveTimeSeries(
            @NotNull String name,
            Long startTime,
            Long endTime,
            String unit,
            Boolean trimMissing) throws SqlDssException, SQLException, EncodedDateTimeException, IOException {

        return TimeSeries.retrieveTimeSeriesValues(
                name,
                startTime == null ? getStartTime() : startTime,
                endTime == null ? getEndTime() : endTime,
                trimMissing == null ? getTrimMissing() : trimMissing,
                unit == null ? getEffectiveRetrieveUnit(name.split("\\|", -1)[1]) : unit,
                this);
    }

    /**
     * Retrieve all values for a time series from the database
     * @param name The SQLDSS time series name
     * @param unit The unit to retrieve the values in. If null, the effective retrieval unit for the parameter is used
     * @param trimMissing Whether to blocks of trim consecutive missing values from the start and end of the retrieved data.
     *                    If null, the value of {@link #getTrimMissing()} is used
     * @return The time series
     * @throws SqlDssException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws SQLException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     * @throws IOException If thrown by {@link TimeSeries#retrieveTimeSeriesValues(String, long, long, boolean, String, SqlDss)}
     */
    public TimeSeriesContainer retrieveTimeSeries(
            @NotNull String name,
            String unit,
            Boolean trimMissing) throws SqlDssException, SQLException, EncodedDateTimeException, IOException {

        return TimeSeries.retrieveAllTimeSeriesValues(
                name,
                trimMissing == null ? getTrimMissing() : trimMissing,
                unit == null ? getEffectiveRetrieveUnit(name.split("\\|", -1)[1]) : unit,
                this);
    }

    /**
     * Store a time series to the database
     * @param tsc The time series to store
     * @param storeRule The store rule to use
     * @throws SqlDssException If thrown by {@link TimeSeries#storeTimeSeriesValues(TimeSeriesContainer, String, SqlDss)}
     * @throws SQLException If thrown by {@link TimeSeries#storeTimeSeriesValues(TimeSeriesContainer, String, SqlDss)}
     * @throws EncodedDateTimeException If thrown by {@link TimeSeries#storeTimeSeriesValues(TimeSeriesContainer, String, SqlDss)}
     */
    public void storeTimeSeries(TimeSeriesContainer tsc, String storeRule) throws SqlDssException, SQLException,
            EncodedDateTimeException {

        TimeSeries.storeTimeSeriesValues(tsc, storeRule, this);
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
     * @return An array of time series catalog names
     * @throws SqlDssException If <code>flags</code> is invalid
     * @throws SQLException If SQL error
     */
    public String @NotNull [] catalogTimeSeries(String nameRegex, boolean condensed, String flags) throws SqlDssException,
            SQLException {

        return TimeSeries.catalogTimeSeries(nameRegex, condensed, flags, this);
    }

    /**
     * Mark an array of time series records as deleted
     * @param recordSpecs The time series records to delete (uncondensed catalog names for each time series record)
     * @throws SqlDssException If thrown by {@link TimeSeries#deleteTimeSeriesRecords(String[], SqlDss)}
     * @throws SQLException If SQL error
     */
    public void deleteTimeSeriesRecords(String[] recordSpecs)
            throws SqlDssException, SQLException {

        TimeSeries.deleteTimeSeriesRecords(recordSpecs, this);
    }

    /**
     * Mark an array of time series records as not deleted
     * @param recordSpecs The time series records to delete (uncondensed catalog names for each time series record)
     * @throws SqlDssException If thrown by {@link TimeSeries#undeleteTimeSeriesRecords(String[], SqlDss)}
     * @throws SQLException If SQL error
     */
    public void undeleteTimeSeriesRecords(String[] recordSpecs)
            throws SqlDssException, SQLException {

        TimeSeries.undeleteTimeSeriesRecords(recordSpecs, this);
    }
}
