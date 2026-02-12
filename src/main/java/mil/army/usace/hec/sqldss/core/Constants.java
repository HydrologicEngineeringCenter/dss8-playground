package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Constants {
    private Constants() {
        throw new AssertionError("Cannot instantiate");
    }
    public static final int YEAR_MINUTES = 525600;
    public static final int MONTH_MINUTES = 43200;
    public static final int DAY_MINUTES = 1440;
    public static final int HOUR_MINUTES = 60;
    public static final String SQL_SELECT_LAST_INSERT_ROWID = "select last_insert_rowid()";
    public static final String LAST_INSERT_ROWID = "last_insert_rowid()";
    public static final String[] PARAMETER_TYPES;

    static {
        //--------------------------------------------------------------------//
        // populate PARAMETER_TYPES from same resource used to populate table //
        //--------------------------------------------------------------------//
        List<String> parameter_types = new ArrayList<>();
        try (InputStream in = Constants.class.getResourceAsStream("init/parameter_type.tsv")) {
            if (in == null) {
                throw new CoreException("Could not open parameter_type resourde");
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
        } catch (IOException | CoreException e) {
            throw new RuntimeException(e);
        }
        PARAMETER_TYPES = new String[parameter_types.size()];
        for (int i = 0; i < parameter_types.size(); ++i) {
            PARAMETER_TYPES[i] = parameter_types.get(i);
        }
    }

    public enum UNIT_SYSTEM {
        SI(0, "Système International"),
        EN(1, "English (Imperial)");
        private final int code;
        private final String description;

        UNIT_SYSTEM(int code, String description) {
            this.code = code;
            this.description = description;
        }

        public int getCode() {
            return code;
        }

        public String getDescription() { return description; }

        public static @NotNull UNIT_SYSTEM fromCode(int code) {
            for (UNIT_SYSTEM t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown UNIT_SYSTEM code: " + code);
        }
    }

    public enum RECORD_TYPE {
        ARR(90, "Array"),
        RTD(105, "Regular-interval time series doubles"),
        RTTD( 106, "Regular-interval time series double pattern"),
        RTPD(107, "Regular-interval time series double profile"),
        ITD(115, "Irregular-interval time series doubles"),
        ITTD( 116, "Irregular-interval time series double pattern"),
        ITPD(117, "Irregular-interval time series double profile"),
        PDD(205, "Paired Data doubles"),
        TXT(300, "Text Data"),
        TT(310, "Text Table"),
        GUT(400, "Gridded - Undefined grib with time"),
        GU(401, "Gridded - Undefined grid"),
        GHT(410, "Gridded - HRAP grid with time reference"),
        GH(411, "Gridded - HRAP grid"),
        GAT(420, "Gridded - Albers with time reference"),
        GA(421, "Gridded - Albers"),
        GST(430, "Gridded - Specified Grid with time reference"),
        GS(431, "Gridded - Specified Grid"),
        TIN(450, "Spatial - TIN"),
        FILE(600, "Generic File"),
        IMAGE(610, "Image");

        private final int code;
        private final String description;

        RECORD_TYPE(int code, String description) {
            this.code = code;
            this.description = description;
        }

        public int getCode() {
            return code;
        }

        public String getDescription() { return description; }

        public static @NotNull RECORD_TYPE fromCode(int code) {
            for (RECORD_TYPE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown RECORD_TYPE code: " + code);
        }
    }

    public enum REGULAR_STORE_RULE {
        REPLACE_ALL(0),
        REPLACE_MISSING_VALUES_ONLY(1),
        REPLACE_ALL_CREATE(2), // alias for REPLACE_ALL for backward compatibility
        REPLACE_ALL_DELETE(3), // alias for REPLACE_ALL for backward compatibility
        REPLACE_WITH_NON_MISSING(4),
        DO_NOT_REPLACE(5);

        private final int code;

        REGULAR_STORE_RULE(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static @NotNull REGULAR_STORE_RULE fromCode(int code) {
            for (REGULAR_STORE_RULE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown REGULAR_STORE_RULE code: " + code);
        }
    }

    public enum IRREGULAR_STORE_RULE {
        REPLACE_ALL(0),
        MERGE(0), // alias for REPLACE_ALL for backward compatibility
        DELETE_INSERT(1),
        REPLACE_MISSING_VALUES_ONLY(2),
        REPLACE_WITH_NON_MISSING(3),
        DO_NOT_REPLACE(4);

        private final int code;

        IRREGULAR_STORE_RULE(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static @NotNull IRREGULAR_STORE_RULE fromCode(int code) {
            for (IRREGULAR_STORE_RULE t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Unknown IRREGULAR_STORE_RULE code: " + code);
        }
    }
}
