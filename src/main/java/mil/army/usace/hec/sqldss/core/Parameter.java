package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Utility class for working with parameters
 */
public class Parameter {

    /**
     * Prevent class instantiation
     */
    private Parameter() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Store a parameter to the database and returns its database key. If the parameter already exists, its existing
     * database key is returned
     * @param name The name of the parameter to store
     * @param conn The JDBC connection
     * @return The database key of the parameter
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getBaseParameter(String, Connection)}
     */
    static long putParameter(String name, Connection conn) throws SQLException, SqlDssException {
        long key = getParameterKey(name, conn);
        if (key < 0) {
            String baseParameter = name;
            String subParameter = "";
            boolean nullKey;
            if (name.indexOf('-') != -1) {
                String[] parts = name.split("-", 2);
                baseParameter = parts[0];
                subParameter = parts[1];
            }
            baseParameter = getBaseParameter(baseParameter, conn);
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into parameter (base_parameter, sub_parameter) values (?, ?)"
            )) {
                ps.setString(1, baseParameter);
                ps.setString(2, subParameter);
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

    /**
     * Retrieves the case-correct base parameter name for a case-insensitive base parameter name
     * @param name The case-insensitive base parameter name
     * @param conn The JDBC connection
     * @return The case-correct base parameter name
     * @throws SQLException If SQL error
     * @throws SqlDssException If there is no match for the case-insensitive base parameter name
     */
    static @NotNull String getBaseParameter(String name, Connection conn) throws SQLException, SqlDssException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select name from base_parameter where name = ?"
        )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                String actualName = rs.getString("name");
                if (actualName == null || actualName.isEmpty()) {
                    throw new SqlDssException("No such base parameter: " + name);
                }
                return actualName;
            }
        }
    }

    /**
     * Retrieves the database key for the specified case-insensitive parameter name
     * @param name The case-insensitive parameter name
     * @param conn The JDBC connection
     * @return The database key for the parameter name
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getBaseParameter(String, Connection)}
     */
    static long getParameterKey(@NotNull String name, Connection conn) throws SQLException, SqlDssException {
        String baseParameter = name;
        String subParameter = "";
        boolean nullKey;
        long key;
        if (name.indexOf('-') != -1) {
            String[] parts = name.split("-", 2);
            baseParameter = parts[0];
            subParameter = parts[1];
        }
        baseParameter = getBaseParameter(baseParameter, conn);
        try (PreparedStatement ps = conn.prepareStatement(
                "select key from parameter where base_parameter = ? and sub_parameter = ?"
        )) {
            ps.setString(1, baseParameter);
            ps.setString(2, subParameter);
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
     * Retrieves the case-correct parameter name for a case-insensitive parameter name
     * @param name The case-insensitive parameter name
     * @param conn The JDBC connection
     * @return The case-correct parameter name
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getParameterKey(String, Connection)}
     */
    static String getParameter(@NotNull String name, Connection conn) throws SQLException, SqlDssException {
        String sql = """
                select base_parameter,
                       sub_parameter
                  from parameter
                 where p.key = ?""";
        long key = getParameterKey(name, conn);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery(sql)) {
                rs.next();
                String baseParameter = rs.getString("base_parameter");
                String subParameter = rs.getString("sub_parameter");
                return subParameter == null || subParameter.isEmpty()
                        ? baseParameter
                        : String.format("%s-%s", baseParameter, subParameter);
            }
        }
    }
}
