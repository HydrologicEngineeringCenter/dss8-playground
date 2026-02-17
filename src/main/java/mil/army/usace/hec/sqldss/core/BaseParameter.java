package mil.army.usace.hec.sqldss.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Utility class for dealing with base parameters
 */
public class BaseParameter {

    /**
     * Prevent class instantiation
     */
    private BaseParameter() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * The universe of available base parameters
     */
    static String[] baseParameters;

    /**
     * Loads base parameters from database into static array
     * @param conn The JDBC connection
     * @throws SQLException on SQL error
     */
    public static void load(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select count(*) from base_parameter")) {
            try(ResultSet rs = ps.executeQuery()) {
                int count = (int)rs.getLong("count(*)");
                baseParameters = new String[count];
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("select name from base_parameter")) {
            try (ResultSet rs = ps.executeQuery()) {
                for(int i = 0; rs.next(); ++i) {
                    baseParameters[i] = rs.getString("name");
                }
            }
        }
    }

    /**
     * Tests whether a specified string matches any base parameter
     * @param s The string to test
     * @return The test result
     */
    public static boolean isBaseParameter(String s) {
        for (String bp: baseParameters) {
            if (s.equalsIgnoreCase(bp)) {
                return true;
            }
        }
        return false;
    }
}
