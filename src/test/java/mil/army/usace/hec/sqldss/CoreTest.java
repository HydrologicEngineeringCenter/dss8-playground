package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import mil.army.usace.hec.sqldss.core.Location;
import mil.army.usace.hec.sqldss.core.SqlDss;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CoreTest {

    @Test
    public static void testLocation() throws Exception {
        String dbFileName = "test.sqldss";
        String locationName = "ctx:BaseLoc-SubLoc";
        Files.deleteIfExists(Path.of(dbFileName));
        try (SqlDss db = SqlDss.open(dbFileName)) {
            Connection conn = db.getConnection();
            Location.putLocation(locationName, conn);
            assertEquals(1, Location.getLocationKey(locationName, conn));
        }
    }
}
