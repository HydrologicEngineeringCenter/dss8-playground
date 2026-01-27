package mil.army.usace.hec.sqldss.core;

import mil.army.usace.hec.sqldss.core.init.Init;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlDss implements AutoCloseable {
    private Connection conn = null;
    private String fileName = null;
    private Long startTime = null;
    private Long endTime = null;
    private boolean trimMissing = false;
    private boolean autoCommit = true;
    private Constants.REGULAR_STORE_RULE regularStoreRule = Constants.REGULAR_STORE_RULE.REPLACE_ALL;
    private Constants.IRREGULAR_STORE_RULE irregularStoreRule = Constants.IRREGULAR_STORE_RULE.REPLACE_ALL;


    private SqlDss() {
        throw new AssertionError("Cannot instantiate without parameters");
    }

    @NotNull
    @Contract("_ -> new")
    public static SqlDss open(String fileName) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName);
    }

    @NotNull
    @Contract("_, _ -> new")
    public static SqlDss open(String fileName, boolean mustExist) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, mustExist);
    }

    @NotNull
    @Contract("_, _, _ -> new")
    public static SqlDss open(String fileName, String startTime, String endTime) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, startTime, endTime);
    }

    @NotNull
    @Contract("_, _, _, _ -> new")
    public static SqlDss open(String fileName, String startTime, String endTime, boolean mustExist) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        return new SqlDss(fileName, startTime, endTime, mustExist);
    }

    public SqlDss(String fileName) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        this(fileName, null, null, false);
    }

    public SqlDss(String fileName, boolean mustExist) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        this(fileName, null, null, mustExist);
    }

    public SqlDss(String fileName, String startTime, String endTime) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        this(fileName, startTime, endTime, false);
    }

    public SqlDss(String fileName, String startTime, String endTime, boolean mustExist) throws SQLException, CoreException, IOException, EncodedDateTimeException {
        Path filePath = Path.of(fileName);
        boolean exists = Files.exists(filePath);
        this.fileName = filePath.toString();
        if (startTime == null) {
            if (endTime != null) {
                throw new CoreException("End time may not be specified without start time");
            }
        }
        else {
            if (endTime == null) {
                throw new CoreException("Start time may not be specified without end time");
            }
            this.startTime = EncodedDateTime.encodeDateTime(startTime);
            this.endTime = EncodedDateTime.encodeDateTime(endTime);
        }
        this.startTime = startTime == null ? null : EncodedDateTime.encodeDateTime(startTime);
        this.endTime = endTime == null ? null : EncodedDateTime.encodeDateTime(endTime);
        if (mustExist && !exists) {
            throw new CoreException(String.format("File %s does not exist", fileName));
        }
        conn = DriverManager.getConnection("jdbc:sqlite:"+this.fileName);
        try (Statement st = conn.createStatement()) {
            st.execute("pragma page_size = 8192");
            st.execute("pragma foreign_keys = ON");
            conn.setAutoCommit(false);
            if (!exists) {
                Init.initializeDb(conn);
            }
            Interval.load(conn);
            BaseParameter.load(conn);
        }
    }

    public boolean isOpen() {
        return conn != null;
    }

    public Connection getConnection() {
        return conn;
    }

    public String getFileName() {
        return fileName;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setTrimMissing(boolean state) {
        trimMissing = state;
    }

    public boolean getTrimMissing() {
        return trimMissing;
    }

    public void setRegularStoreRule(Constants.REGULAR_STORE_RULE rule) {
        regularStoreRule = rule;
    }

    public void setRegularStoreRule(int rule) {
        regularStoreRule = Constants.REGULAR_STORE_RULE.fromCode(rule);
    }

    public void setRegularStoreRule(@NotNull String rule) {
        regularStoreRule = Constants.REGULAR_STORE_RULE.valueOf(rule.toUpperCase());
    }

    public int getRegularStoreRule() {
        return regularStoreRule.getCode();
    }

    public Constants.REGULAR_STORE_RULE getRegularStoreRuleValue() {
        return regularStoreRule;
    }

    public void setIrregularStoreRule(Constants.IRREGULAR_STORE_RULE rule) {
        irregularStoreRule = rule;
    }

    public void setIrregularStoreRule(int rule) {
        irregularStoreRule = Constants.IRREGULAR_STORE_RULE.fromCode(rule);
    }

    public void setIrregularStoreRule(@NotNull String rule) {
        irregularStoreRule = Constants.IRREGULAR_STORE_RULE.valueOf(rule.toUpperCase());
    }

    public int getIrregularStoreRule() {
        return irregularStoreRule.getCode();
    }

    public Constants.IRREGULAR_STORE_RULE getIrregularStoreRuleValue() {
        return irregularStoreRule;
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public void setAutoCommit(boolean state) throws SQLException {
        autoCommit = state;
        conn.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void close() throws CoreException {
        if (conn == null) {
            throw new CoreException("Already closed");
        }
        else {
            try {
                conn.close();
            }
            catch (SQLException e) {
                throw new CoreException(e);
            }
            conn = null;
        }
    }
}
