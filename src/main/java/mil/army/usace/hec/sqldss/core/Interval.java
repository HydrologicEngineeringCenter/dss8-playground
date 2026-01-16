package mil.army.usace.hec.sqldss.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Interval {

    static Map<String, Integer> intervalMinutes = new HashMap<>();
    static Map<String, String> intervalBlockSizes = new HashMap<>();

    private Interval() {
        throw new AssertionError("Cannot instantiate");
    }

    static {
        try (InputStream in = Interval.class.getResourceAsStream("init/interval.tsv")) {
            if (in == null) {
                throw new CoreException("Could not open interval resource");
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("*")) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    intervalMinutes.put(parts[0], Integer.parseInt(parts[1]));
                    intervalBlockSizes.put(parts[0], parts[2]);
                }
            }
        }
        catch (IOException | CoreException e) {
            throw new RuntimeException(e);
        }
    }

    static String getInterval(String name, Connection conn) throws CoreException, SQLException {
        String actualName;
        try (PreparedStatement ps = conn.prepareStatement(
                "select name from interval where name = ?"
        )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                actualName = rs.getString("name");
            }
        }
        return actualName;
    }

    public static int getIntervalMinutes(String interval) throws CoreException {
        Integer minutes = intervalMinutes.get(interval);
        if (minutes == null) {
            throw new CoreException("No such interval: "+interval);
        }
        return minutes;
    }

    public static String getBlockSize(String interval) throws CoreException {
        String blockSize = intervalBlockSizes.get(interval);
        if (blockSize == null) {
            throw new CoreException("No such interval: "+interval);
        }
        return blockSize;
    }

    public static int getBlockSizeMinutes(String interval) {
        return intervalMinutes.get(intervalBlockSizes.get(interval));
    }
}
