package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Utility class for working with locations
 */
public class Location {

    /**
     * Prevent class instantiation
     */
    private Location() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Retrieve the database location key for a specified location name
     * @param locationName The location name
     * @param conn The JDBC connection
     * @return The location name
     * @throws SQLException If SQL error
     */
    public static long getLocationKey(@NotNull String locationName, Connection conn) throws SQLException {
        return getLocationKey(locationName, new String[1], conn);
    }

    /**
     * Retrieve JSON information for a specified location
     * @param locationName The location to retrieve the information for
     * @param conn The JDBC connection
     * @return The (possibly empty) information in JSON format
     * @throws SQLException If SQL error
     */
    public static String getLocationInfo(@NotNull String locationName, Connection conn) throws SQLException {
        String[] info = new String[1];
        getLocationKey(locationName, info, conn);
        return info[0];
    }


    /**
     * Retrieve JSON information for a specified location
     * @param key The database key for the specified location
     * @param conn The JDBC connection
     * @return The (possibly empty) information in JSON format
     * @throws SQLException If SQL error
     */
    public static String getLocationInfo(long key, Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select info from location where key = ?"
        )) {
            ps.setLong(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString("info");
            }
        }
    }

    /**
     * Retrieve the database location key and information for a specified location name
     * @param locationName The location name
     * @param info A String array whose first element will receive the (possibly empty) location information in JSON format
     * @param conn The JDBC connection
     * @return The location name
     * @throws SQLException If SQL error
     */
    public static long getLocationKey(@NotNull String locationName, String @NotNull [] info, Connection conn) throws SQLException {
        long key;
        boolean nullKey;
        info[0] = "";
        //--------------------//
        // query for location //
        //--------------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "select key, info from location where name=?"
        )) {
            ps.setString(1, locationName);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                key = rs.getLong("key");
                nullKey = rs.wasNull();
                info[0] = rs.getString("info");
            }
        }
        if (nullKey) {
            return -1;
        }
        return key;
    }

    /**
     * Stores a location to the database and returns its database key. If the location already exists, the existing key is returned.
     * @param locationName The name of the location to store
     * @param conn The JDBC connection
     * @return The database key of the location
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown in {@link #putLocation(String, String, boolean, Connection)}
     */
    public static long putLocation(String locationName, Connection conn) throws SQLException, SqlDssException {
        return putLocation(locationName, null, true, conn);
    }

    /**
     * Stores a location and its information to the database and returns the database key. If the location already exists,
     * the existing key is returned.
     * @param locationName The name of the location to store
     * @param info The location information to store, if any, in JSON format
     * @param mergeInfo Whether to merge the specified location information with any existing information in the dataabse.
     *                  If <code>false</code>, the specified location information will overwrite any existing information.
     * @param conn The JDBC connection
     * @return The database key of the location
     * @throws SQLException If SQL error
     * @throws SqlDssException If other errors storing location
     */
    public static long putLocation(String locationName, String info, boolean mergeInfo, Connection conn) throws SQLException, SqlDssException {
        validateLocationName(locationName);
        boolean nullKey;
        String[] existingInfo = new String[1];
        long key = getLocationKey(locationName, existingInfo, conn);
        if (key > 0) {
            //-------------------------------------------------------------------//
            // location already exists: compare info and see if we need to merge //
            //-------------------------------------------------------------------//
            info = info == null ? "" : info;
            if (!info.isEmpty()) {
                Util.validateJsonString(info);
            }
            existingInfo[0] = existingInfo[0] == null ? "" : existingInfo[0];
            if (!info.equals(existingInfo[0])) {
                if (mergeInfo) {
                    if (info.isEmpty()) {
                        info = existingInfo[0];
                    }
                    else if (!existingInfo[0].isEmpty()) {
                        info = Util.mergeJsonStrings(info, existingInfo[0]);
                    }
                }
                if (!info.equals(existingInfo[0])) {
                    //------------------------------//
                    // write the merged info string //
                    //------------------------------//
                    String sql = "update location set info = ? where key = ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, info);
                        ps.setLong(2, key);
                        ps.executeUpdate();
                    }
                }
            }
            return key;
        }
        //--------------------------------------//
        // location doesn't exist, so create it //
        //--------------------------------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into location (name, info) values (?, ?)"
        )) {
            ps.setString(1, locationName);
            ps.setString(2, info);
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
            throw new SqlDssException("Error storing location " + locationName);
        }
        return key;
    }

    /**
     * Validates a location name: must not be null or empty.
     * @param locationName The location name to validate
     * @throws SqlDssException If the location name is null or empty
     */
    static void validateLocationName(String locationName) throws SqlDssException {
        if (locationName == null || locationName.isEmpty()) {
            throw new SqlDssException("Location name may not be null or empty");
        }
    }
}
