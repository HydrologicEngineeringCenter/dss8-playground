package mil.army.usace.hec.sqldss.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BaseParameter {

    private BaseParameter() {
        throw new AssertionError("Cannot instantiate");
    }

    static String[] baseParameters;

    public static void load(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select count(*) from base_parameter")) {
            try(ResultSet rs = ps.executeQuery()) {
                int count = (int)rs.getLong("count(*)");
                baseParameters = new String[count];
            }
        }
        try (PreparedStatement ps = conn.prepareStatement("select name from base_parameter")) {
            try (ResultSet rs = ps.executeQuery()) {
                for(int i = 0; rs.next(); ++i) {
                    baseParameters[i] = rs.getString("name");
                }
            }
        }
    }

    public static boolean isBaseParameter(String s) {
        for (String bp: baseParameters) {
            if (s.equalsIgnoreCase(bp)) {
                return true;
            }
        }
        return false;
    }
}
