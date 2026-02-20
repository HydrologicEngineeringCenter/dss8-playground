package mil.army.usace.hec.sqldss.api.dss7;

import hec.io.TimeSeriesContainer;
import mil.army.usace.hec.sqldss.api.ApiException;
import mil.army.usace.hec.sqldss.core.BaseParameter;
import mil.army.usace.hec.sqldss.core.Interval;
import mil.army.usace.hec.sqldss.core.SqlDssException;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

/**
 * Utility class for HEC-DSS v7 API for SQLDSS
 */
public final class ApiUtil {

    /**
     * Regular expression for interval names
     */
    static Pattern intervalPattern = Pattern.compile("^(\\d)+(Minute|Hour|Day|Week|Month|Year|Decade)s?(Local)?$",
            Pattern.CASE_INSENSITIVE);

    /**
     * Prevent class instantiation
     */
    private ApiUtil() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Determine whether  a string is a valid HEC-DSS v7 time series pathname
     * @param apiName The string to test
     * @return Whether the string is a valid HEC-DSS v7 time series pathname
     * @throws ApiException If <code>apiName</code> is not a valid HEC-DSS v7 pathname
     */
    public static boolean isTimeSeriesApiName(@NotNull String apiName) throws ApiException {
        String[] parts = apiName.split("/", -1);
        if (parts.length != 8) {
            throw new ApiException("Invalid pathname: " + apiName);
        }
        String location = parts[2];
        String baseParameter = parts[3].split("-", -1)[0];
        String interval = parts[5];
        String version = parts[6];
        return (!location.isEmpty()
                && !version.isEmpty()
                && BaseParameter.isBaseParameter(baseParameter)
                && intervalPattern.matcher(interval).matches());
    }

    /**
     * Generate an SQLDSS name from a TimeSeriesContainer that has an HEC-DSS v7 pathname
     * @param tsc The TimeSeriesContainer
     * @return The SQLDSS name
     * @throws ApiException If <code>tsc.fullName</code> is invalid or not recognized as a specific HEC-DSS v7 record type
     */
    @NotNull
    public static String toSqlDssName(@NotNull TimeSeriesContainer tsc) throws ApiException {
        return toSqlDssName(tsc.fullName, tsc.type);
    }

    /**
     * Generate an SQLDSS name from an HEC-DSS v7 pathname and a parameter type
     * @param apiName The HEC-DSS v7 pathname
     * @param parameterType The parameter type
     * @return The SQLDSS name
     * @throws ApiException If <code>apiName</code> is invalid or not recognized as a specific HEC-DSS v7 record type
     */
    @NotNull
    public static String toSqlDssName(@NotNull String apiName, String parameterType) throws ApiException {
        String[] parts = apiName.split("/", -1);
        if (parts.length != 8) {
            throw new ApiException("Invalid pathname: " + apiName);
        }
        if (isTimeSeriesApiName(apiName)) {
            String context = parts[1];
            String location = parts[2];
            String parameter = parts[3];
            String interval = parts[5];
            String duration = parameterType.startsWith("INST-") ? "0" : interval;
            String version = parts[6];
            return context.replace('|', '/') + (context.isEmpty() ? "" : ":") +
                    location.replace('|', '/') + "|" +
                    parameter + "|" +
                    parameterType + "|" +
                    interval + "|" +
                    duration + "|" +
                    version.replace('|', '/');
        }
        else {
            throw new ApiException("Can currently only change time series pathnames");
        }
    }

    /**
     * Generate an SQLDSS name from an HEC-DSS v7 pathname, assuming INST-VAL for the parameter type
     * @param apiName The HEC-DSS v7 pathname
     * @return The SQLDSS name
     * @throws ApiException If <code>apiName</code> is invalid or not recognized as a specific HEC-DSS v7 record type
     */
    @NotNull
    public static String toSqlDssName(@NotNull String apiName) throws ApiException {
        return toSqlDssName(apiName, "INST-VAL");
    }

    /**
     * Generate an HEC-DSS v7 pathname from an SQLDSS name
     * @param sqlDssName The SQLDSS name
     * @return The HEC-DSS v7 pathname
     * @throws ApiException If <code>sqlDssName</code> is not of a recognized record type
     */
    @NotNull
    static String toApiName(@NotNull String sqlDssName) throws ApiException {
        String[] parts = sqlDssName.split("\\|", -1);
        if (parts.length != 6) {
            throw new ApiException("Invalid core name: " + sqlDssName);
        }
        String apiName;
        String context = "";
        String location = parts[0];
        if (location.indexOf(':') != -1) {
            String[] parts2 = location.split(":", 2);
            context = parts2[0];
            location = parts2[1];
        }
        apiName = "/" + context.replace('/', '|')
                + "/" + location.replace('/', '|')
                + "/" + parts[1]
                + "//" + parts[3]
                + "/" + parts[5].replace('/', '|')
                + "/";
        if (!isTimeSeriesApiName(apiName)) {
            throw new ApiException("Can currently only change time series pathnames");
        }
        return apiName;
    }

    /**
     * Converts a TimeSeriesContainer in-place from SQLDSS to HEC-DSS v7
     * @param tsc The TimeSeriesContainer
     * @throws ApiException If <code>tsc.fullName</code> is not a of a valid SQLDSS record type
     * @throws SqlDssException If thrown by {@link Interval#getIntervalMinutes(String)}
     */
    static void updateTscToApi(@NotNull TimeSeriesContainer tsc) throws ApiException, SqlDssException {
        String[] parts = tsc.fullName.split("\\|", -1);
        if (parts.length != 6) {
            throw new ApiException("Invalid core name: " + tsc.fullName);
        }
        String context = "";
        String location = parts[0];
        if (location.indexOf(':') != -1) {
            String[] parts2 = location.split(":", 2);
            context = parts2[0];
            location = parts2[1];
        }
        String baseLocation = location;
        String subLocation = "";
        if (location.indexOf('-') != -1) {
            String[] parts2 = location.split("-", 2);
            baseLocation = parts2[0];
            subLocation = parts2[1];
        }
        String parameter = parts[1];
        String baseParameter = parameter;
        String subParameter = "";
        if (parameter.indexOf('-') != -1) {
            String[] parts2 = parameter.split("-", 2);
            baseParameter = parts2[0];
            subParameter = parts2[1];
        }
        String interval = parts[3];
        String version = parts[5];
        tsc.fullName = "/" + context.replace('/', '|')
                + "/" + location.replace('/', '|')
                + "/" + parameter
                + "//" + interval
                + "/" + version.replace('/', '|')
                + "/";
        tsc.type = parts[2];
        tsc.location = baseLocation;
        tsc.subLocation = subLocation;
        tsc.parameter = baseParameter;
        tsc.subParameter = subParameter;
        tsc.version = version;
        tsc.interval = Interval.getIntervalMinutes(interval);
    }

    /**
     * Converts a TimeSeriesContainer in-place from HEC-DSS v7 to SQLDSS
     * @param tsc The TimeSeriesContainer
     * @throws ApiException If <code>tsc.fullName</code> is not a of a valid HEC-DSS v7 record type
     * @throws SqlDssException If thrown by {@link Interval#getIntervalMinutes(String)}
     */
    static void updateTscToSqlDss(@NotNull TimeSeriesContainer tsc) throws ApiException, SqlDssException {
        String[] parts = tsc.fullName.split("/", -1);
        if (parts.length != 8) {
            throw new ApiException("Invalid pathname: " + tsc.fullName);
        }
        String context = parts[1];
        String location = parts[2];
        if (location.indexOf(':') != -1) {
            String[] parts2 = location.split(":", 2);
            context = parts2[0];
            location = parts2[1];
        }
        String baseLocation = location;
        String subLocation = "";
        if (location.indexOf('-') != -1) {
            String[] parts2 = location.split("-", 2);
            baseLocation = parts2[0];
            subLocation = parts2[1];
        }
        String parameter = parts[3];
        String baseParameter = parameter;
        String subParameter = "";
        if (parameter.indexOf('-') != -1) {
            String[] parts2 = parameter.split("-", 2);
            baseParameter = parts2[0];
            subParameter = parts2[1];
        }
        String interval = parts[5];
        String version = parts[6];
        tsc.fullName = context.replace('|', '/') + (context.isEmpty() ? "" : ":") +
                location.replace('|', '/') + "|" +
                parameter + "|" +
                tsc.type + "|" +
                interval + "|" +
                (tsc.type.startsWith("INST-") ? "0" : interval) + "|" +
                version.replace('|', '/');
        tsc.location = baseLocation;
        tsc.subLocation = subLocation;
        tsc.parameter = baseParameter;
        tsc.subParameter = subParameter;
        tsc.version = version;
        tsc.interval = Interval.getIntervalMinutes(interval);
    }
}
