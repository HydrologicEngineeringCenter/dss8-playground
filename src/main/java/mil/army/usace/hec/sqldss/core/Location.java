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
        String context = "";
        String location;
        String baseLocation;
        String subLocation = "";
        info[0] = "";
        long baseKey;
        long key;
        boolean nullKey;
        if (locationName.indexOf(':') == -1) {
            location = locationName;
        }
        else {
            String[] parts = locationName.split(":", 2);
            context = parts[0].strip();
            location = parts[1].strip();
        }
        if (location.indexOf('-') == -1) {
            baseLocation = location;
        }
        else {
            String[] parts = location.split("-", 2);
            baseLocation = parts[0].strip();
            subLocation = parts[1].strip();
        }
        //-------------------------//
        // query for base location //
        //-------------------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "select key from base_location where context=? and name=?"
        )) {
            ps.setString(1, context);
            ps.setString(2, baseLocation);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                baseKey = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            return -1;
        }
        //--------------------//
        // query for location //
        //--------------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "select key, info from location where base_location=? and sub_location=?"
        )) {
            ps.setLong(1, baseKey);
            ps.setString(2, subLocation);
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
        String context = "";
        String location;
        String baseLocation;
        String subLocation = "";
        long baseKey;
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
        if (locationName.indexOf(':') == -1) {
            location = locationName;
        }
        else {
            String[] parts = locationName.split(":", 2);
            context = parts[0].strip();
            location = parts[1].strip();
        }
        if (location.indexOf('-') == -1) {
            baseLocation = location;
        }
        else {
            String[] parts = location.split("-", 2);
            baseLocation = parts[0].strip();
            subLocation = parts[1].strip();
        }
        //-------------------------//
        // query for base location //
        //-------------------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "select key from base_location where context=? and name=?"
        )) {
            ps.setString(1, context);
            ps.setString(2, baseLocation);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                baseKey = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            //----------------------//
            // insert base location //
            //----------------------//
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into base_location (context, name) values (?, ?)"
            )) {
                ps.setString(1, context);
                ps.setString(2, baseLocation);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    Constants.SQL_SELECT_LAST_INSERT_ROWID
            )) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    baseKey = rs.getLong(Constants.LAST_INSERT_ROWID);
                    nullKey = rs.wasNull();
                }
            }
        }
        if (nullKey) {
            throw new SqlDssException("Error storing base location " + locationName);
        }
        //-----------------//
        // insert location //
        //-----------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into location (base_location, sub_location, info) values (?, ?, ?)"
        )) {
            ps.setLong(1, baseKey);
            ps.setString(2, subLocation);
            ps.setString(3, info);
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
            if (context.isEmpty()) {
                throw new SqlDssException("Error retrieving location " + location);
            }
            else {
                throw new SqlDssException("Error retrieving location " + context + ":" + baseLocation);

            }
        }
        return key;
    }
}
