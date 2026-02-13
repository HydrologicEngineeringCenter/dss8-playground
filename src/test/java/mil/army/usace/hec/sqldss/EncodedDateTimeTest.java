package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.time.ZoneId;
import java.util.Arrays;

import static mil.army.usace.hec.sqldss.core.EncodedDateTime.changeTimeZone;
import static mil.army.usace.hec.sqldss.core.EncodedDateTime.encodeDateTime;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EncodedDateTimeTest {

    void copyArrays(int @NotNull [] values, String @NotNull [] dateParts, String @NotNull [] timeParts) {
        values[0] = Integer.parseInt(dateParts[0]);
        values[1] = Integer.parseInt(dateParts[1]);
        values[2] = Integer.parseInt(dateParts[2]);
        values[3] = Integer.parseInt(timeParts[0]);
        values[4] = Integer.parseInt(timeParts[1]);
        values[5] = Integer.parseInt(timeParts[2]);
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/time_zone_conversions.tsv", delimiter = '\t')
    public void testConvertTimeZone(@NotNull String dt1, @NotNull String dt2, String tz1, String tz2) throws Exception {
        String[] parts;
        String[] dateParts;
        String[] timeParts;
        int[] values = new int[6];
        parts = dt1.split("\\s+", -1);
        dateParts = parts[0].split("-", -1);
        timeParts = parts[1].split(":", -1);
        copyArrays(values, dateParts, timeParts);
        long encoded = encodeDateTime(values);
        parts = dt2.split("\\s+", -1);
        dateParts = parts[0].split("-", -1);
        timeParts = parts[1].split(":", -1);
        copyArrays(values, dateParts, timeParts);
        long expected = encodeDateTime(values);
        assertEquals(expected, changeTimeZone(encoded, ZoneId.of(tz1), ZoneId.of(tz2)));
    }
}
