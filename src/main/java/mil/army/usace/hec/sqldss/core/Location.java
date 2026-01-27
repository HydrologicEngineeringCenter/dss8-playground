package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Location {

    private Location() {
        throw new AssertionError("Cannot instantiate");
    }

    public static long getLocationKey(@NotNull String locationName, Connection conn) throws SQLException {
        String context = "";
        String location;
        String baseLocation;
        String subLocation = "";
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
                "select key from location where base_location=? and sub_location=?"
        )) {
            ps.setLong(1, baseKey);
            ps.setString(2, subLocation);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                key = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            return -1;
        }
        return key;
    }

    public static long putLocation(String locationName, Connection conn) throws SQLException, CoreException {
        String context = "";
        String location;
        String baseLocation;
        String subLocation = "";
        long baseKey;
        boolean nullKey;
        long key = getLocationKey(locationName, conn);
        if (key > 0) {
            return key;
        }
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
            throw new CoreException("Error storing base location " + locationName);
        }
        //-----------------//
        // insert location //
        //-----------------//
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into location (base_location, sub_location) values (?, ?)"
        )) {
            ps.setLong(1, baseKey);
            ps.setString(2, subLocation);
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
                throw new CoreException("Error retrieving location " + location);
            }
            else {
                throw new CoreException("Error retrieving location " + context + ":" + baseLocation);

            }
        }
        return key;
    }
}
