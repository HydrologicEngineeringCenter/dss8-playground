package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Interval {

    static Map<String, String> intervalNames = new HashMap<>();
    static Map<String, Integer> intervalMinutes = new HashMap<>();
    static Map<String, String> intervalBlockSizes = new HashMap<>();

    private Interval() {
        throw new AssertionError("Cannot instantiate");
    }

    public static void load(Connection conn) throws SQLException {
        String sql = "select * from interval";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    int minutes = (int)rs.getLong("minutes");
                    String blockSize = rs.getString("block_size");
                    intervalNames.put(name.toLowerCase(), name);
                    intervalMinutes.put(name, minutes);
                    intervalBlockSizes.put(name, blockSize);
                }
            }
        }
    }

    static @NotNull String getInterval(String name) throws CoreException {
        String actualName;
        actualName = intervalNames.get(name.toLowerCase());
        if (actualName == null) {
            throw new CoreException("No such interval: "+name);
        }
        return actualName;
    }

    public static int getIntervalMinutes(String interval) throws CoreException {
        String intervalName = getInterval(interval);
        Integer minutes = intervalMinutes.get(intervalName);
        if (minutes == null) {
            throw new CoreException("No such interval: "+interval);
        }
        return minutes;
    }

    public static @NotNull String getBlockSize(String interval) throws CoreException {
        String intervalName = getInterval(interval);
        String blockSize = intervalBlockSizes.get(intervalName);
        if (blockSize == null) {
            throw new CoreException("No such interval: "+interval);
        }
        return blockSize;
    }

    public static int getBlockSizeMinutes(String interval) throws CoreException {
        String intervalName = getInterval(interval);
        return intervalMinutes.get(intervalBlockSizes.get(intervalName));
    }
}
