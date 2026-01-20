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

public class Unit {

    private Unit() {
        throw new AssertionError("Cannot instantiate");
    }

    static final HashSet<String> operators = new HashSet<>(Arrays.asList("+", "-", "*", "/", "//", "%", "^", "**"));

    public static long getUnitKey(String name, Connection conn) throws SQLException, CoreException {
        long key;
        boolean nullKey;
        try (PreparedStatement ps = conn.prepareStatement("select key from unit where name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                key = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            try (PreparedStatement ps = conn.prepareStatement("select key from unit_alias where alias = ?")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    key = rs.getLong("key");
                    nullKey = rs.wasNull();
                }
            }
        }
        if (nullKey) {
            throw new CoreException("No such unit or unit alias: " + name);
        }
        return key;
    }

    @NotNull
    public static String getUnit(String name, Connection conn) throws SQLException, CoreException {
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
            throw new CoreException("No such unit or unit alias: " + name);
        }
        return unit;
    }

    @NotNull
    public static String[] getCompatibleUnits(String name, Connection conn) throws SQLException, CoreException {
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

    public static void getUnitConverisonForStoring(
            String fromUnit,
            @NotNull String parameter,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, CoreException {
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

    public static void getUnitConversionForRetrieving(
            String toUnit,
            @NotNull String parameter,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, CoreException {
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

    public static void getUnitConversion(
            String fromUnit,
            String toUnit,
            double[] factor,
            double[] offset,
            String[] function,
            Connection conn) throws SQLException, CoreException {
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
                    throw new CoreException("Cannot convert from unit " + fromUnit + " to unit " + toUnit);
                }
            }
        }
    }

    public static double convertUnits(
            double value,
            String fromUnit,
            String toUnit,
            Connection conn) throws SQLException, CoreException {
        double[] factor = new double[1];
        double[] offset = new double[1];
        String[] function = new String[1];
        getUnitConversion(fromUnit, toUnit, factor, offset, function, conn);
        return performConversion(value, factor[0], offset[0], function[0]);
    }

    public static void convertUnits(
            @NotNull TimeSeriesContainer tsc,
            String toUnit,
            Connection conn) throws CoreException, SQLException {
        double[] factor = new double[1];
        double[] offset = new double[1];
        String[] function = new String[1];
        getUnitConversion(tsc.units, toUnit, factor, offset, function, conn);
        for (int i = 0; i < tsc.numberValues; ++i) {
            if (tsc.values[i] != UNDEFINED_DOUBLE) {
               tsc.values[i]  = performConversion(tsc.values[i], factor[0], offset[0], function[0]);
            }
        }
    }

    public static double performConversion(
            double value,
            double factor,
            double offset,
            String function) throws CoreException {
        if (function != null && !function.isEmpty()) {
            return executeFunction(value, function);
        }
        else {
            return value * factor + offset;
        }

    }
    static double executeFunction(double value, @NotNull String function) throws CoreException {
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
                throw new CoreException("Invalid conversion function: " + function);
            }
        }
        if (stack.size() != 1) {
            throw new CoreException("Invalid conversion function: " + function);
        }
        try {
            return Double.parseDouble(stack.pop());
        }
        catch (Exception e) {
            throw new CoreException("Invalid conversion function: " + function);
        }
    }

    static double performOperation(double arg1, double arg2, String operator) throws CoreException {
        switch (operator) {
            case "+" : return arg1 + arg2;
            case "-" : return arg1 - arg2;
            case "*" : return arg1 * arg2;
            case "/" : return arg1 / arg2;
            case "//": return Math.floor(arg1 / arg2);
            case "^" :
            case "**": return Math.pow(arg1, arg2);
            default  : throw new CoreException("Unexpected operator: " + operator);
        }
    }
}
