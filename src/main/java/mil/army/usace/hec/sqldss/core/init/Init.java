package mil.army.usace.hec.sqldss.core.init;

import hec.heclib.util.HecTime;
import mil.army.usace.hec.sqldss.core.Constants;
import mil.army.usace.hec.sqldss.core.CoreException;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class Init {

    private Init() {
        throw new AssertionError("Cannot instantiate");
    }

    public static void initializeDb(Connection conn) throws SQLException, CoreException, IOException {
        createDssInfoTable(conn);
        createAbstractParamTable(conn);
        createUnitTable(conn);
        createUnitAliasTable(conn);
        createUnitConversionTable(conn);
        createIntervalTable(conn);
        createDurationTable(conn);
        createBaseParameterTable(conn);
        createParameterTable(conn);
        createBaseLocationTable(conn);
        createLocationTable(conn);
        createLocationInfoTable(conn);
        createTimeSeriesTable(conn);
        createTsvTable(conn);
        createTsvInfoTable(conn);
        conn.commit();
    }
    
    public static void createDssInfoTable(@NotNull Connection conn) throws SQLException {
        String sql =
                """
                        create table dss_info(
                          key text collate nocase primary key,
                          value text)""";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        }
        sql = "insert into dss_info (key, value) values (?, ?)";
        HecTime t = new HecTime();
        t.setCurrent();
        String[][] info = {
                {"version", "8.0.0"},
                {"created", t.dateAndTime(-13)}
        };
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String[] infoRec : info) {
                ps.setString(1, infoRec[0]);
                ps.setString(2, infoRec[1]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static void createAbstractParamTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sql =
                """
                        create table abstract_parameter(
                          key integer primary key,
                          name text unique collate nocase)""";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        }
        sql = "insert into abstract_parameter (name) values (?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (InputStream in = Init.class.getResourceAsStream("abstract_parameter.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open abstract_parameter resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        ps.setString(1, line);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createUnitTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sql =
                """
                        create table unit(
                            name text collate nocase primary key,
                            abstract_parameter_key integer not null,
                            unit_system text,
                            long_name text not null,
                            foreign key (abstract_parameter_key) references abstract_parameter (key))""";
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
        sql = "insert into unit (name, abstract_parameter_key, unit_system, long_name) values (?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (InputStream in = Init.class.getResourceAsStream("unit.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open unit resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        String[] parts = line.split("\t", -1);
                        ps.setString(1, parts[0]);
                        ps.setString(2, parts[1]);
                        ps.setString(3, parts[2]);
                        ps.setString(4, parts[3]);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createUnitAliasTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sql =
                """
                        create table unit_alias(
                          alias text collate nocase primary key,
                          unit text not null,
                          foreign key (unit) references unit (name))""";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        }
        sql = "insert into unit_alias values(?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (InputStream in = Init.class.getResourceAsStream("unit_alias.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open unit_alias resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        String[] parts = line.split("\t", -1);
                        ps.setString(1, parts[0]);
                        ps.setString(2, parts[1]);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createUnitConversionTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sql =
                """
                        create table unit_conversion(
                          from_unit text collate nocase not null,
                          to_unit text collate nocase not null,
                          factor real,
                          offset real,
                          function text,
                          primary key (from_unit, to_unit),
                          foreign key (from_unit) references unit (name),
                          foreign key (to_unit) references unit (name))""";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        }
        sql =
                """
                        insert
                          into unit_conversion
                          (from_unit, to_unit, factor, offset, function)
                          values (?, ?, ?, ?, ?)""";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (InputStream in = Init.class.getResourceAsStream("unit_conversion.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open unit_conversion resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        String[] parts = line.split("\t", -1);
                        ps.setString(1, parts[0]);
                        ps.setString(2, parts[1]);
                        if (parts[2].isEmpty()) {
                            ps.setNull(3, Types.NUMERIC);
                        }
                        else {
                            ps.setDouble(3, Double.parseDouble(parts[2]));
                        }
                        if (parts[3].isEmpty()) {
                            ps.setNull(4, Types.NUMERIC);
                        }
                        else {
                            ps.setDouble(4, Double.parseDouble(parts[3]));
                        }
                        ps.setString(5, parts[4]);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createBaseParameterTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sqlTable =
                """
                        create table base_parameter(
                          name text collate nocase primary key,
                          abstract_parameter_key integer not null,
                          default_si_unit text not null,
                          default_en_unit text not null,
                          long_name text,
                          foreign key (abstract_parameter_key) references abstract_parameter (key),
                          foreign key (default_si_unit) references unit (name),
                          foreign key (default_en_unit) references unit (name))""";
        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
        String sqlInsert =
                "insert" +
                        "  into base_parameter (name, abstract_parameter_key, default_si_unit, default_en_unit, long_name)\n" +
                        "  values (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
            try (InputStream in = Init.class.getResourceAsStream("base_parameter.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open base_parameter resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        String[] parts = line.split("\t", -1);
                        ps.setString(1, parts[0]);
                        ps.setLong(2, Long.parseLong(parts[1]));
                        ps.setString(3, parts[2]);
                        ps.setString(4, parts[3]);
                        ps.setString(5, parts[4]);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createIntervalTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sqlTable =
                """
                        create table interval(
                          name collate nocase primary key,
                          minutes integer not null,
                          block_size text not null)""";

        String sqlIndex = "create index idx_interval_minutes on interval (minutes)";

        String sqlInsert = "insert into interval (name, minutes, block_size) values(?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(sqlIndex)) {
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
            try (InputStream in = Init.class.getResourceAsStream("interval.tsv")) {
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
                        ps.setString(1, parts[0]);
                        ps.setLong(2, Long.parseLong(parts[1]));
                        ps.setString(3, parts[2]);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createDurationTable(@NotNull Connection conn) throws SQLException, IOException, CoreException {
        String sqlTable =
                """
                        create table duration(
                          name text collate nocase primary key,
                          minutes integer not null)""";

        String sqlInsert = "insert into duration (name, minutes) values(?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
            try (InputStream in = Init.class.getResourceAsStream("duration.tsv")) {
                if (in == null) {
                    throw new CoreException("Could not open duration resource");
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("*")) {
                            continue;
                        }
                        String[] parts = line.split("\t", -1);
                        ps.setString(1, parts[0]);
                        ps.setLong(2, Long.parseLong(parts[1]));
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    public static void createParameterTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table parameter(
                          key integer primary key,
                          base_parameter text not null collate nocase,
                          sub_parameter text default ('') collate nocase,
                          foreign key (base_parameter) references base_parameter (name))""";

        String sqlIndex = "create unique index idx_parameter on parameter (base_parameter, sub_parameter)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(sqlIndex)) {
            ps.executeUpdate();
        }
    }

    public static void createBaseLocationTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table base_location(
                          key integer primary key,
                          context text not null default ('') collate nocase, -- office, A pathname part, ...
                          name text not null collate nocase)""";

        String sqlIndex = "create unique index idx_base_location on base_location (context, name)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(sqlIndex)) {
            ps.executeUpdate();
        }
    }

    public static void createLocationTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table location(
                          key integer primary key,
                          base_location integer,
                          sub_location text default ('') collate nocase,
                          info text default (''), -- JSON object
                          foreign key (base_location) references base_location (key))""";

        String sqlIndex = "create unique index idx_location on location (base_location, sub_location)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(sqlIndex)) {
            ps.executeUpdate();
        }
    }

    public static void createLocationInfoTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table location_info(
                          key integer primary key,
                          info text not null, -- JSON object
                          foreign key (key) references location (key))""";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
    }

    public static void createTimeSeriesTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table time_series(
                          key integer primary key,
                          location integer not null,
                          parameter integer not null,
                          parameter_type text not null check (parameter_type in (:types:)),
                          interval text not null,
                          duration text not null,
                          version text default ('') collate nocase,
                          interval_offset text default (''), -- ISO 8601 (e.g., PT15M)
                          foreign key (location) references location (key),
                          foreign key (parameter) references parameter (key),
                          foreign key (interval) references interval (name),
                          foreign key (duration) references duration (name))""";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Constants.PARAMETER_TYPES.length; ++i) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("'").append(Constants.PARAMETER_TYPES[i]).append("'");
        }
        String sqlIndex = "create unique index idx_time_series on time_series (location, parameter, parameter_type, interval, duration, version)";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable.replace(":types:", sb.toString()))) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(sqlIndex)) {
            ps.executeUpdate();
        }
    }

    public static void createTsvTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table tsv(
                          time_series integer,
                          block_start_date integer, -- encoded -?\\d+\\d{2}\\d{2} for extended dates
                          data blob,
                          primary key (time_series, block_start_date),
                          foreign key (time_series) references time_series (key))""";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
    }

    public static void createTsvInfoTable(@NotNull Connection conn) throws SQLException {
        String sqlTable =
                """
                        create table tsv_info(
                          time_series integer,
                          block_start_date integer,       -- encoded -?\\d+\\d{2}\\d{2} for extended dates
                          value_count integer not null,
                          first_time integer not null,    -- encoded -?\\d+\\d{2}\\d{2} d{2}:d{2}:d{2} for extended dates
                          last_time integer not null,     -- encoded -?\\d+\\d{2}\\d{2} d{2}:d{2}:d{2} for extended dates
                          min_value real,
                          max_value real,
                          last_update integer not null,   -- Unix epoch millisecionds
                          primary key (time_series, block_start_date),
                          foreign key (time_series) references time_series (key))""";

        try (PreparedStatement ps = conn.prepareStatement(sqlTable)) {
            ps.executeUpdate();
        }
    }
}
