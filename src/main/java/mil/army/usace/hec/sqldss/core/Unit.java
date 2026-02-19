package mil.army.usace.hec.sqldss.core;

import hec.io.TimeSeriesContainer;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static hec.lang.Const.UNDEFINED_DOUBLE;
import static mil.army.usace.hec.sqldss.core.Constants.*;

/**
 * Utility class for working with units
 */
public class Unit {

    /**
     * Prevent class instantiation
     */
    private Unit() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Recognized RPN mathematical operators
     */
    static final HashSet<String> operators = new HashSet<>(Arrays.asList("+", "-", "*", "/", "//", "%", "^", "**"));

    /**
     * Retrieve a case-correct unit name from a case-insensitive unit name or unit alias
     * @param name The case-insensitive name or alias to retrieve the unit name for
     * @param conn The JDBC connection
     * @return The case-correct unit name
     * @throws SQLException If SQL error
     * @throws SqlDssException If no unit matches <code>name</code>
     */
    @NotNull
    public static String getUnit(String name, Connection conn) throws SQLException, SqlDssException {
        String unit = null;
        try (PreparedStatement ps = conn.prepareStatement("select name from unit where name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                unit = rs.getString("name");
            }
        }
        if (unit == null || unit.isEmpty()) {
            String sql = "select unit from unit_alias where alias = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    unit = rs.getString("name");
                }
            }
        }
        if (unit == null || unit.isEmpty()) {
            throw new SqlDssException("No such unit or unit alias: " + name);
        }
        return unit;
    }

    /**
     * Returns an array of units that are compatible with the specified unit (i.e., are attached to the same abstract
     * parameter)
     * @param name The unit to retrieve compatible units for
     * @param conn The JDBC connection
     * @return The array of compatible unit names
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getUnit(String, Connection)}
     */
    @NotNull
    public static String @NotNull [] getCompatibleUnits(String name, Connection conn) throws SQLException, SqlDssException {
        List<String> list = new ArrayList<>();
        String sql = "select name from unit where abstract_parameter_key = (select abstract_parameter_key from unit " +
                "where name = ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, getUnit(name, conn));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(rs.getString("name"));
                }
            }
        }
        return list.toArray(new String[0]);
    }

    /**
     * Retrieves the unit conversion necessary to convert values for a specified parameter in a specified unit to the
     * database storage unit for the parameter.
     * <dl>
     *     <dt>><code>function[0]</code> is null or return</dt>
     *     <dd>The conversion to the database storage unit is <code>dbVal = val * factor[0] + offset[0]</code></dd>
     *     <dt>><code>function[0]</code> is non-null or return</dt>
     *     <dd>The conversion is the application of the <code>function[0]</code> with the current value represented by
     *     the token <code>ARG1</code></dd>
     * </dl>
     * @param fromUnit The current unit of values
     * @param parameter The parameter of the vaules
     * @param factor An array of length at least one whose first element receives the unit conversion factor
     * @param offset An array of length at least one whose first element receives the unit conversion offset
     * @param function An array of length at least one whose first element receives the unit conversion RPN function
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getUnitConversion(String, String, double[], double[], String[], Connection)}
     */
    public static void getUnitConverisonForStoring(
            String fromUnit,
            @NotNull String parameter,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, SqlDssException {
        String baseParameter = parameter.split("-", 2)[0];
        String sql = "select default_si_unit from base_parameter where name = ?";
        String dbUnit;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, baseParameter);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                dbUnit = rs.getString("default_si_unit");
            }
        }
        getUnitConversion(fromUnit, dbUnit, factor, offset, function, conn);
    }

    /**
     * Retrieves the unit conversion necessary to convert database values for a specified parameter to a specified retrieval
     * unit.
     * <dl>
     *     <dt>><code>function[0]</code> is null or return</dt>
     *     <dd>The conversion to the database storage unit is <code>val = dbVal * factor[0] + offset[0]</code></dd>
     *     <dt>><code>function[0]</code> is non-null or return</dt>
     *     <dd>The conversion is the application of the <code>function[0]</code> with the database value represented by
     *     the token <code>ARG1</code></dd>
     * </dl>
     * @param toUnit The retrieval unit for the values
     * @param parameter The parameter of the vaules
     * @param factor An array of length at least one whose first element receives the unit conversion factor
     * @param offset An array of length at least one whose first element receives the unit conversion offset
     * @param function An array of length at least one whose first element receives the unit conversion RPN function
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by {@link #getUnitConversion(String, String, double[], double[], String[], Connection)}
     */
    public static void getUnitConversionForRetrieving(
            String toUnit,
            @NotNull String parameter,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, SqlDssException {
        String baseParameter = parameter.split("-", 2)[0];
        String sql = "select default_si_unit from base_parameter where name ? ";
        String dbUnit;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, baseParameter);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                dbUnit = rs.getString("default_si_unit");
            }
        }
        getUnitConversion(dbUnit, toUnit, factor, offset, function, conn);
    }

    /**
     * Retrieves a generic unit conversion from the database
     * <dl>
     *     <dt>><code>function[0]</code> is null or return</dt>
     *     <dd>The conversion to the database storage unit is <code>toUnitVal = fromUnitVal * factor[0] + offset[0]</code></dd>
     *     <dt>><code>function[0]</code> is non-null or return</dt>
     *     <dd>The conversion is the application of the <code>function[0]</code> with the <code>fromUnit</code> value represented by
     *     the token <code>ARG1</code></dd>
     * </dl>
     * @param fromUnit The unit to convert from
     * @param toUnit The unit to convert to
     * @param factor An array of length at least one whose first element receives the unit conversion factor
     * @param offset An array of length at least one whose first element receives the unit conversion offset
     * @param function An array of length at least one whose first element receives the unit conversion RPN function
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If <code>fromUnit</code> and <code>toUnit</code> are not compatible units
     */
    public static void getUnitConversion(
            String fromUnit,
            String toUnit,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, SqlDssException {
        String sql = "select factor, offset, function from unit_conversion where from_unit=? and to_unit=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fromUnit);
            ps.setString(2, toUnit);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    factor[0] = rs.getDouble("factor");
                    offset[0] = rs.getDouble("offset");
                    function[0] = rs.getString("function");
                }
                else {
                    throw new SqlDssException("Cannot convert from unit " + fromUnit + " to unit " + toUnit);
                }
            }
        }
    }

    /**
     * Converts a single value from one unit to another
     * @param value The value to convert
     * @param fromUnit The unit to convert from
     * @param toUnit The unit to convert to
     * @param conn The JDBC connection
     * @return The converted value
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link #getUnitConversion(String, String, double[], double[], String[], Connection)}</li>
     *     <li>{@link #performConversion(double, double, double, String)}</li>
     * </ul>
     */
    public static double convertUnits(
            double value,
            String fromUnit,
            String toUnit,
            Connection conn) throws SQLException, SqlDssException {
        double[] factor = new double[1];
        double[] offset = new double[1];
        String[] function = new String[1];
        getUnitConversion(fromUnit, toUnit, factor, offset, function, conn);
        return performConversion(value, factor[0], offset[0], function[0]);
    }

    /**
     * Converts a TimeSeriesContainer in-place to a specified unit
     * @param tsc The TimeSeriesContainer to convert in-place
     * @param toUnit The unit to convert to
     * @param conn The JDBC connection
     * @throws SQLException If SQL error
     * @throws SqlDssException If thrown by <ul>
     *     <li>{@link #getUnitConversion(String, String, double[], double[], String[], Connection)}</li>
     *     <li>{@link #performConversion(double, double, double, String)}</li>
     * </ul>
     */
    public static void convertUnits(
            @NotNull TimeSeriesContainer tsc,
            String toUnit,
            Connection conn) throws SqlDssException, SQLException {
        double[] factor = new double[1];
        double[] offset = new double[1];
        String[] function = new String[1];
        getUnitConversion(tsc.units, toUnit, factor, offset, function, conn);
        if (tsc.quality == null) {
            for (int i = 0; i < tsc.numberValues; ++i) {
                if (tsc.values[i] != UNDEFINED_DOUBLE) {
                    tsc.values[i]  = performConversion(tsc.values[i], factor[0], offset[0], function[0]);
                }
            }
        }
        else {
            for (int i = 0; i < tsc.numberValues; ++i) {
                if (tsc.values[i] != UNDEFINED_DOUBLE && (tsc.quality[i] & QUALITY_SCREENED_VALIDITY_MASK) != QUALITY_MISSING_VALUE) {
                    tsc.values[i]  = performConversion(tsc.values[i], factor[0], offset[0], function[0]);
                }
            }
        }
        tsc.units = toUnit;
    }

    /**
     * Performs a unit conversion on a single value
     * <dl>
     *     <dt>><code>function</code> is null</dt>
     *     <dd>The conversion to the database storage unit is <code>toUnitVal = value * factor + offset</code></dd>
     *     <dt>><code>function</code> is non-null or return</dt>
     *     <dd>The conversion is the application of the <code>function</code> with the <code>value</code> represented by
     *     the token <code>ARG1</code></dd>
     * </dl>
     * @param value The value to convert
     * @param factor The unit conversion factor
     * @param offset The unit conversion offset
     * @param function The unit conversion function
     * @return The converted value
     * @throws SqlDssException If thrown by {@link #executeFunction(double, String)}
     */
    public static double performConversion(
            double value,
            double factor,
            double offset,
            String function) throws SqlDssException {
        if (function != null && !function.isEmpty()) {
            return executeFunction(value, function);
        }
        else {
            return value * factor + offset;
        }
    }

    /**
     * Performs an RPN function on a single
     * @param value The value to have the function performed on
     * @param function The RPN function, with <code>value</code> represented by the token <code>ARG1</code>
     * @return The resulting value
     * @throws SqlDssException If <code>function</code> is not a valid RPN function of one variable (<code>ARG1</code>)
     */
    static double executeFunction(double value, @NotNull String function) throws SqlDssException {
        String[] parts = function.split("\\s+", -1);
        ArrayDeque<String> stack = new ArrayDeque<>();
        for (String part : parts) {
            try {
                if (part.equalsIgnoreCase("ARG1")) {
                    stack.push(String.valueOf(value));
                }
                else if (part.equalsIgnoreCase("-ARG1")) {
                    stack.push(String.valueOf(-value));
                }
                else if (operators.contains(part)) {
                    double arg2 = Double.parseDouble(stack.pop());
                    double arg1 = Double.parseDouble(stack.pop());
                    stack.push(String.valueOf(performOperation(arg1, arg2, part)));
                }
                else {
                    stack.push(part);
                }
            }
            catch (Exception e) {
                throw new SqlDssException("Invalid conversion function: " + function);
            }
        }
        if (stack.size() != 1) {
            throw new SqlDssException("Invalid conversion function: " + function);
        }
        try {
            return Double.parseDouble(stack.pop());
        }
        catch (Exception e) {
            throw new SqlDssException("Invalid conversion function: " + function);
        }
    }

    /**
     * Performs a mathematical operation on two arguments
     * @param arg1 The first argument
     * @param arg2 The second argument
     * @param operator The mathematical operator
     * @return The result of the operation
     * @throws SqlDssException If <code>operator</code> is unexpected
     */
    static double performOperation(double arg1, double arg2, @NotNull String operator) throws SqlDssException {
        return switch (operator) {
            case "+" -> arg1 + arg2;
            case "-" -> arg1 - arg2;
            case "*" -> arg1 * arg2;
            case "/" -> arg1 / arg2;
            case "//" -> Math.floor(arg1 / arg2);
            case "^", "**" -> Math.pow(arg1, arg2);
            default -> throw new SqlDssException("Unexpected operator: " + operator);
        };
    }
}
