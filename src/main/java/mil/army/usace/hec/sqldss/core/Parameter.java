package mil.army.usace.hec.sqldss.core;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Parameter {

    private Parameter() {
        throw new AssertionError("Cannot instantiate");
    }

    static long putParameter(String name, Connection conn) throws CoreException, SQLException {
        long key = getParameterKey(name, conn);
        if (key < 0) {
            String baseParameter = name;
            String subParameter = "";
            boolean nullKey;
            if (name.indexOf('-') != -1) {
                String[] parts = name.split("-", 2);
                baseParameter = parts[0];
                subParameter = parts[1];
            }
            baseParameter = getBaseParameter(baseParameter, conn);
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into parameter (base_parameter, sub_parameter) values (?, ?)"
            )) {
                ps.setString(1, baseParameter);
                ps.setString(2, subParameter);
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
                key = -1;
            }
        }
        return key;
    }

    static @NotNull String getBaseParameter(String name, Connection conn) throws CoreException, SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select name from base_parameter where name = ?"
        )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                String actualName = rs.getString("name");
                if (actualName == null || actualName.isEmpty()) {
                    throw new CoreException("No such base parameter: " + name);
                }
                return actualName;
            }
        }
    }

    static long getParameterKey(@NotNull String name, Connection conn) throws CoreException, SQLException {
        String baseParameter = name;
        String subParameter = "";
        boolean nullKey;
        long key;
        if (name.indexOf('-') != -1) {
            String[] parts = name.split("-", 2);
            baseParameter = parts[0];
            subParameter = parts[1];
        }
        baseParameter = getBaseParameter(baseParameter, conn);
        try (PreparedStatement ps = conn.prepareStatement(
                "select key from parameter where base_parameter = ? and sub_parameter = ?"
        )) {
            ps.setString(1, baseParameter);
            ps.setString(2, subParameter);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                key = rs.getLong("key");
                nullKey = rs.wasNull();
            }
        }
        if (nullKey) {
            key = -1;
        }
        return key;
    }

    static String getParameter(@NotNull String name, Connection conn) throws CoreException, SQLException {
        String sql = """
                select base_parameter,
                       sub_parameter
                  from parameter
                 where p.key = ?""";
        long key = getParameterKey(name, conn);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery(sql)) {
                rs.next();
                String baseParameter = rs.getString("base_parameter");
                String subParameter = rs.getString("sub_parameter");
                return subParameter == null || subParameter.isEmpty()
                        ? baseParameter
                        : String.format("%s-%s", baseParameter, subParameter);
            }
        }
    }
}
