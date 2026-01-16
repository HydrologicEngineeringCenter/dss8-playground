package mil.army.usace.hec.sqldss.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Duration {

    public static final String PATTERN_ISO_8601_DURATION = "P(?:(?:(\\d+)Y)?(?:(\\d+)M)?(?:(\\d+)D)?)?(?:T(?:(\\d+)H)?(?:(\\d+)M)?(?:(\\d+)S)?)?";

    private Duration() {
        throw new AssertionError("Cannot instantiate");
    }

    static String getDuration(String name, Connection conn) throws CoreException, SQLException {
        String actualName;
        try (PreparedStatement ps = conn.prepareStatement(
                "select name from duration where name = ?"
        )) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                actualName = rs.getString("name");
            }
        }
        return actualName;
    }

    static int iso8601ToMinutes(String s) throws CoreException {
        int minutes = 0;
        Pattern pat = Pattern.compile(PATTERN_ISO_8601_DURATION);
        Matcher m = pat.matcher(s);
        if (!m.matches()) {
            throw new CoreException("String is not an ISO-8601 duration string: " + s);
        }
        int[] minutesByGroup = new int[]{0, 525600, 43200, 1440, 60, 1, 0};
        for (int group = 1; group < m.groupCount(); ++group) {
            String groupText = m.group(group);
            if (groupText != null && !groupText.isEmpty()) {
                minutes += minutesByGroup[group];
            }
        }
        return minutes;
    }

    static String minutesToIso8601(int minutes) {
        if (minutes == 0) {
            return "PT0S";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("P");
        if (minutes >= Constants.YEAR_MINUTES) {
            int y = minutes / Constants.YEAR_MINUTES;
            sb.append(y).append("Y");
            minutes -= y * Constants.YEAR_MINUTES;
        }
        if (minutes >= Constants.MONTH_MINUTES) {
            int m = minutes / Constants.MONTH_MINUTES;
            sb.append(m).append("M");
            minutes -= m * Constants.MONTH_MINUTES;
        }
        if (minutes >= Constants.DAY_MINUTES) {
            int d = minutes / Constants.DAY_MINUTES;
            sb.append(d).append("D");
            minutes -= d * Constants.DAY_MINUTES;
        }
        if (minutes > 0) {
            sb.append("T");
            if (minutes > Constants.HOUR_MINUTES) {
                int h = minutes / Constants.HOUR_MINUTES;
                sb.append(h).append("H");
                minutes -= h * Constants.HOUR_MINUTES;
            }
            if (minutes > 0) {
                sb.append(minutes).append("M");
            }
        }
        return sb.toString();
    }
}
