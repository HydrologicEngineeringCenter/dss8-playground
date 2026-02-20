package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for working with intervals
 */
public class Interval {

    /**
     * Map of case-insensitive interval names to case-correct interval names
     */
    static Map<String, String> intervalNames = new HashMap<>();
    /**
     * Map of interval names to interval minutes
     */
    static Map<String, Integer> intervalMinutes = new HashMap<>();
    /**
     * Map of interval names to SQLDSS block size interval names
     */
    static Map<String, String> intervalBlockSizes = new HashMap<>();

    /**
     * Prevent class instantiation
     */
    private Interval() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Populate interval maps from same resource used to populate interval table
     */
    static {
        try (InputStream in = Interval.class.getResourceAsStream("init/interval.tsv")) {
            if (in == null) {
                throw new RuntimeException("Could not open interval resource");
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("*")) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    intervalNames.put(parts[0].toLowerCase(), parts[0]);
                    intervalMinutes.put(parts[0], Integer.parseInt(parts[1]));
                    intervalBlockSizes.put(parts[0], parts[2]);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve a case-correct interval name from a case-insensitive interval name
     * @param name The case-insensitive interval name
     * @return The case-correct interval name
     * @throws SqlDssException If no match is found for <code>name</code>
     */
    public static @NotNull String getInterval(@NotNull String name) throws SqlDssException {
        String actualName;
        actualName = intervalNames.get(name.toLowerCase());
        if (actualName == null) {
            throw new SqlDssException("No such interval: "+name);
        }
        return actualName;
    }

    /**
     * Retrieve the interval minutes for a case-insensitive interval name
     * @param interval The case-insensitive interval name
     * @return The interval minutes
     * @throws SqlDssException If no match is found for <code>interval</code>
     */
    public static int getIntervalMinutes(String interval) throws SqlDssException {
        String intervalName = getInterval(interval);
        Integer minutes = intervalMinutes.get(intervalName);
        if (minutes == null) {
            throw new SqlDssException("No such interval: "+interval);
        }
        return minutes;
    }

    /**
     * Retrieve the SQLDSS block size interval name for a case-insensitive interval name
     * @param interval The case-insensitive interval name
     * @return The interval name of the block size for <code>interval</code>
     * @throws SqlDssException If no match is found for <code>interval</code>
     */
    public static @NotNull String getBlockSize(String interval) throws SqlDssException {
        String intervalName = getInterval(interval);
        String blockSize = intervalBlockSizes.get(intervalName);
        if (blockSize == null) {
            throw new SqlDssException("No such interval: "+interval);
        }
        return blockSize;
    }

    /**
     * Retrieve the SQLDSS block size interval minutes for a case-insensitive interval name
     * @param interval The case-insensitive interval name
     * @return The interval minutes of the block size for <code>interval</code>
     * @throws SqlDssException If no match is found for <code>interval</code>
     */
    public static int getBlockSizeMinutes(String interval) throws SqlDssException {
        String intervalName = getInterval(interval);
        return intervalMinutes.get(intervalBlockSizes.get(intervalName));
    }
}
