package mil.army.usace.hec.sqldss.api.dss7;

import hec.io.TimeSeriesContainer;
import mil.army.usace.hec.sqldss.api.ApiException;
import mil.army.usace.hec.sqldss.core.BaseParameter;
import mil.army.usace.hec.sqldss.core.SqlDssException;
import mil.army.usace.hec.sqldss.core.Interval;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

public final class ApiUtil {

    static Pattern intervalPattern = Pattern.compile("^(\\d)+(Minute|Hour|Day|Week|Month|Year|Decade)s?(Local)?$",
            Pattern.CASE_INSENSITIVE);

    private ApiUtil() {
        throw new AssertionError("Cannot instantiate");
    }

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

    @NotNull
    public static String toCoreName(@NotNull TimeSeriesContainer tsc) throws ApiException {
        return toCoreName(tsc.fullName, tsc.type);
    }

    @NotNull
    public static String toCoreName(@NotNull String apiName, String dataType) throws ApiException {
        String[] parts = apiName.split("/", -1);
        if (parts.length != 8) {
            throw new ApiException("Invalid pathname: " + apiName);
        }
        if (isTimeSeriesApiName(apiName)) {
            String context = parts[1];
            String location = parts[2];
            String parameter = parts[3];
            String interval = parts[5];
            String duration = dataType.startsWith("INST-") ? "0" : interval;
            String version = parts[6];
            return context.replace('|', '/') + (context.isEmpty() ? "" : ":") +
                    location.replace('|', '/') + "|" +
                    parameter + "|" +
                    dataType + "|" +
                    interval + "|" +
                    duration + "|" +
                    version.replace('|', '/');
        }
        else {
            throw new ApiException("Can currently only change time series pathnames");
        }
    }

    @NotNull
    public static String toCoreName(@NotNull String apiName) throws ApiException {
        return toCoreName(apiName, "INST-VAL");
    }

    @NotNull
    static String toApiName(@NotNull String coreName) throws ApiException {
        String[] parts = coreName.split("\\|", -1);
        if (parts.length != 6) {
            throw new ApiException("Invalid core name: " + coreName);
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

    static void updateTscToCore(@NotNull TimeSeriesContainer tsc) throws ApiException, SqlDssException {
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
