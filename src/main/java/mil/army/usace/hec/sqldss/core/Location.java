package mil.army.usace.hec.sqldss.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Location {

    private Location() {
        throw new AssertionError("Cannot instantiate");
    }

    public static long getLocationKey(@NotNull String locationName,Connection conn) throws SQLException {
        return getLocationKey(locationName, new String[1], conn);
    }

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

    public static long putLocation(String locationName, Connection conn) throws SQLException, CoreException {
        return putLocation(locationName, null, true, conn);
    }

    public static long putLocation(String locationName, String info, boolean mergeInfo, Connection conn) throws SQLException, CoreException {
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
            existingInfo[0] = existingInfo[0] == null ? "" : existingInfo[0];
            if (!info.equals(existingInfo[0])) {
                if (mergeInfo) {
                    if (info.isEmpty()) {
                        info = existingInfo[0];
                    }
                    else if (!existingInfo[0].isEmpty()) {
                        //-------------------------------------//
                        // actually merge the two info strings //
                        //-------------------------------------//
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode existingRoot = mapper.readTree(existingInfo[0]);
                            JsonNode incomingRoot = mapper.readTree(info);
                            HashMap<String, JsonNode> mergedItems = new HashMap<>();
                            for (Iterator<Map.Entry<String, JsonNode>> it = existingRoot.fields(); it.hasNext(); ) {
                                Map.Entry<String, JsonNode> item = it.next();
                                mergedItems.put((String) item.getKey(), (JsonNode) item.getValue());
                            }
                            for (Iterator<Map.Entry<String, JsonNode>> it = incomingRoot.fields(); it.hasNext(); ) {
                                Map.Entry<String, JsonNode> item = it.next();
                                mergedItems.put((String) item.getKey(), (JsonNode) item.getValue());
                            }
                            info = mapper.writeValueAsString(mergedItems);
                        }
                        catch (JsonProcessingException e) {
                            throw new CoreException(e);
                        }
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
            throw new CoreException("Error storing base location " + locationName);
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
                throw new CoreException("Error retrieving location " + location);
            }
            else {
                throw new CoreException("Error retrieving location " + context + ":" + baseLocation);

            }
        }
        return key;
    }
}
