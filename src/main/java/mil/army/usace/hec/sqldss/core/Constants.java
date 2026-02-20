package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for useful pre-defined constant values
 */
public class Constants {
    /**
     * Prevent class instantiation
     */
    private Constants() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * The number of minutes that represents a <code>1Century</code> interval
     */
    public static final int CENTURY_MINUTES = 52560000;
    /**
     * The number of minutes that represents a <code>1Decade</code> interval
     */
    public static final int DECADE_MINUTES = 5256000;
    /**
     * The number of minutes that represents a <code>1Year</code> interval
     */
    public static final int YEAR_MINUTES = 525600;
    /**
     * The number of minutes that represents a <code>1Month</code> interval
     */
    public static final int MONTH_MINUTES = 43200;
    /**
     * The number of minutes in a day
     */
    public static final int DAY_MINUTES = 1440;
    /**
     * The number of minutes in an hour
     */
    public static final int HOUR_MINUTES = 60;
    /**
     * Value of 5 lowest order bits for a MISSING quality code
     */
    public static final int QUALITY_MISSING_VALUE = 5;
    /**
     * Value of 5 lowest order bits for a REJECTED quality code
     */
    public static final int QUALITY_REJECTED_VALUE = 17;
    /**
     * Mask for the SCREENED (lowest order bit) and VALIDITY (4 next lowest order bite) portions of a quality code
     */
    public static final int QUALITY_SCREENED_VALIDITY_MASK = 31;
    /**
     * The SQL to retrieve the retrieve the auto-generated key for the most recent insert operation
     */
    public static final String SQL_SELECT_LAST_INSERT_ROWID = "select last_insert_rowid()";
    /**
     * The column name of the value retrieved by SQL_SELECT_LAST_INSERT_ROWID
     */
    public static final String LAST_INSERT_ROWID = "last_insert_rowid()";
    /**
     * The SQL template for retrieving time series blocks (rows from TSV table)
     */
    public static final String SQL_SELECT_TS_BLOCK = """
        select deleted,
               data
          from tsv
         where time_series = %d
           and block_start_date = ?""";
    /**
     * The universe of available parameter types
     */
    public static final String[] PARAMETER_TYPES;

    /**
     * Load the parameter types from the same resource used to populate the PARAMETER_TYPE table
     */
    static {
        //--------------------------------------------------------------------//
        // populate PARAMETER_TYPES from same resource used to populate table //
        //--------------------------------------------------------------------//
        List<String> parameter_types = new ArrayList<>();
        try (InputStream in = Constants.class.getResourceAsStream("init/parameter_type.tsv")) {
            if (in == null) {
                throw new SqlDssException("Could not open parameter_type resourde");
            }
            try (BufferedReader br = new BufferedReader((new InputStreamReader(in, StandardCharsets.UTF_8)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("*")) {
                        continue;
                    }
                    parameter_types.add(line);
                }
            }
        } catch (IOException | SqlDssException e) {
            throw new RuntimeException(e);
        }
        PARAMETER_TYPES = new String[parameter_types.size()];
        for (int i = 0; i < parameter_types.size(); ++i) {
            PARAMETER_TYPES[i] = parameter_types.get(i);
        }
    }
    /**
     * The universe of unit systems
     */
    public enum UNIT_SYSTEM {
        /**
         * <code><b>0:</b></code> Syst&egrave;me International
         */
        SI(0, "Système International"),
        /**
         * <code><b>1:</b></code> English (Imperial)
         */
        EN(1, "English (Imperial)");

        /**
         * The numeric code
         */
        private final int code;
        /**
         * The description
         */
        private final String description;
        /**
         * Constructor
         * @param code The numeric code
         * @param description The text description
         */
        UNIT_SYSTEM(int code, String description) {
            this.code = code;
            this.description = description;
        }
        /**
         * @return The numeric code
         */
        public int getCode() {
            return code;
        }
        /**
         * @return The text description
         */
        public String getDescription() { return description; }
        /**
         * Get a UNIT_SYSTEM from its numeric code
         * @param code The numeric code
         * @return the UNIT_SYSTEM
         */
        public static @NotNull UNIT_SYSTEM fromCode(int code) {
            for (UNIT_SYSTEM t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown UNIT_SYSTEM code: " + code);
        }
    }
    /**
     * The universe of record types
     */
    public enum RECORD_TYPE {
        /**
         * <code><b>90:</b></code> Array
         */
        ARR(90, "Array"),
        /**
         * <code><b>105:</b></code> Regular-interval time series doubles
         */
        RTD(105, "Regular-interval time series doubles"),
        /**
         * <code><b>106:</b></code> Regular-interval time series double pattern
         */
        RTTD( 106, "Regular-interval time series double pattern"),
        /**
         * <code><b>107:</b></code> Regular-interval time series double profile
         */
        RTPD(107, "Regular-interval time series double profile"),
        /**
         * <code><b>115:</b></code> Irregular-interval time series doubles
         */
        ITD(115, "Irregular-interval time series doubles"),
        /**
         * <code><b>116:</b></code> Irregular-interval time series double pattern
         */
        ITTD( 116, "Irregular-interval time series double pattern"),
        /**
         * <code><b>117:</b></code> Irregular-interval time series double profile
         */
        ITPD(117, "Irregular-interval time series double profile"),
        /**
         * <code><b>205:</b></code> Paired Data doubles
         */
        PDD(205, "Paired Data doubles"),
        /**
         * <code><b>300:</b></code> Text Data
         */
        TXT(300, "Text Data"),
        /**
         * <code><b>310:</b></code> Text Table
         */
        TT(310, "Text Table"),
        /**
         * <code><b>400:</b></code> Gridded - Undefined grib with time
         */
        GUT(400, "Gridded - Undefined grib with time"),
        /**
         * <code><b>401:</b></code> Gridded - Undefined grid
         */
        GU(401, "Gridded - Undefined grid"),
        /**
         * <code><b>410:</b></code> Gridded - HRAP grid with time reference
         */
        GHT(410, "Gridded - HRAP grid with time reference"),
        /**
         * <code><b>411:</b></code> Gridded - HRAP grid
         */
        GH(411, "Gridded - HRAP grid"),
        /**
         * <code><b>420:</b></code> Gridded - Albers with time reference
         */
        GAT(420, "Gridded - Albers with time reference"),
        /**
         * <code><b>421:</b></code> Gridded - Albers
         */
        GA(421, "Gridded - Albers"),
        /**
         * <code><b>430:</b></code> Gridded - Specified Grid with time reference
         */
        GST(430, "Gridded - Specified Grid with time reference"),
        /**
         * <code><b>431:</b></code> Gridded - Specified Grid
         */
        GS(431, "Gridded - Specified Grid"),
        /**
         * <code><b>450:</b></code> Spatial - TIN
         */
        TIN(450, "Spatial - TIN"),
        /**
         * <code><b>600:</b></code> Generic File
         */
        FILE(600, "Generic File"),
        /**
         * <code><b>610:</b></code> Image
         */
        IMAGE(610, "Image");

        /**
         * The numeric code
         */
        private final int code;
        /**
         * The description
         */
        private final String description;
        /**
         * Constructor
         * @param code The numeric code
         * @param description The text description
         */
        RECORD_TYPE(int code, String description) {
            this.code = code;
            this.description = description;
        }
        /**
         * @return The numeric code
         */
        public int getCode() {
            return code;
        }
        /**
         * @return The text description
         */
        public String getDescription() { return description; }
        /**
         * Get a RECORD_TYPE from its numeric code
         * @param code The numeric code
         * @return the RECORD_TYPE
         */
        public static @NotNull RECORD_TYPE fromCode(int code) {
            for (RECORD_TYPE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown RECORD_TYPE code: " + code);
        }
    }
    /**
     * The universe of regular time series store rules
     */
    public enum REGULAR_STORE_RULE {
        /**
         * <code><b>0:</b> Replace all existing values</code>
         */
        REPLACE_ALL(0),
        /**
         * <code><b>1:</b></code> Only replace existing values that are missing or rejected
         */
        REPLACE_MISSING_VALUES_ONLY(1),
        /**
         * <code><b>2:</b></code> alias for REPLACE_ALL for backward compatibility
         */
        REPLACE_ALL_CREATE(2),
        /**
         * <code><b>3:</b></code> alias for REPLACE_ALL for backward compatibility
         */
        REPLACE_ALL_DELETE(3),
        /**
         * <code><b>4:</b></code> Only replace existing values with non-missing replacements
         */
        REPLACE_WITH_NON_MISSING(4),
        /**
         * <code><b>5:</b></code> Do not replace any existing values
         */
        DO_NOT_REPLACE(5);

        /**
         * The numeric code
         */
        private final int code;
        /**
         * Constructor
         * @param code The numeric code
         */
        REGULAR_STORE_RULE(int code) {
            this.code = code;
        }
        /**
         * @return The numeric code
         */
        public int getCode() {
            return code;
        }
        /**
         * Get a REGULAR_STORE_RULE from its numeric code
         * @param code The numeric code
         * @return the REGULAR_STORE_RULE
         */
        public static @NotNull REGULAR_STORE_RULE fromCode(int code) {
            for (REGULAR_STORE_RULE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown REGULAR_STORE_RULE code: " + code);
        }
    }
    /**
     * The universe of irregular time series store rules
     */
    public enum IRREGULAR_STORE_RULE {
        /**
         * <code><b>0:</b></code> Replace all existing values
         */
        REPLACE_ALL(0),
        /**
         * <code><b>0:</b></code> Alias for REPLACE_ALL for backward compatibility
         */
        MERGE(0),
        /**
         * <code><b>1:</b></code> Delete all existing values in time window of replacement values and store replacements
         */
        DELETE_INSERT(1),
        /**
         * <code><b>2:</b></code> Only replace existing values that are missing or rejected
         */
        REPLACE_MISSING_VALUES_ONLY(2),
        /**
         * <code><b>3:</b></code> Only replace existing values with non-missing replacements
         */
        REPLACE_WITH_NON_MISSING(3),
        /**
         * <code><b>4:</b></code> Do not replace any existing values
         */
        DO_NOT_REPLACE(4);

        /**
         * The numeric code
         */
        private final int code;
        /**
         * Constructor
         * @param code The numeric code
         */
        IRREGULAR_STORE_RULE(int code) {
            this.code = code;
        }
        /**
         * @return The numeric code
         */
        public int getCode() {
            return code;
        }
        /**
         * Get an IRREGULAR_STORE_RULE from its numeric code
         * @param code The numeric code
         * @return the IRREGULAR_STORE_RULE
         */
        public static @NotNull IRREGULAR_STORE_RULE fromCode(int code) {
            for (IRREGULAR_STORE_RULE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown IRREGULAR_STORE_RULE code: " + code);
        }
    }
}
