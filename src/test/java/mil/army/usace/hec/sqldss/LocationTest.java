package mil.army.usace.hec.sqldss.mil.army.usace.hec.sqldss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.flogger.FluentLogger;
import mil.army.usace.hec.sqldss.core.CoreException;
import mil.army.usace.hec.sqldss.core.EncodedDateTimeException;
import mil.army.usace.hec.sqldss.core.Location;
import mil.army.usace.hec.sqldss.core.SqlDss;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class LocationTest {

    static FluentLogger logger = FluentLogger.forEnclosingClass();
    static SqlDss _db = null;

    static boolean equalJsonValues(JsonNode value1, JsonNode value2) throws CoreException {
        JsonNodeType valType = value1.getNodeType();
        if (valType != value2.getNodeType()) {
            return false;
        }
        switch(valType) {
            case STRING:
                if (!value1.asText("").equals(value2.asText(""))) {
                    return false;
                }
                break;
            case NUMBER:
                if (value1.isInt() || value1.isLong()) {
                    if (value1.asLong() != value2.asLong()) {
                        return false;
                    }
                    break;
                }
                else if (value1.isFloatingPointNumber()) {
                    if (value1.asDouble() != value2.asDouble()) {
                        return false;
                    }
                    break;
                }
                else {
                    throw new CoreException(String.format("Unexpected numeric type: %s", valType.name()));
                }
            case BOOLEAN:
                if (value1.asBoolean() != value2.asBoolean()) {
                    return false;
                }
                break;
            default:
                throw new CoreException(String.format("Don't know how to compare objects of type %s", valType.name()));
        }
        return true;
    }

    static boolean equalJsonStrings(String info1, String info2) throws JsonProcessingException, CoreException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root1 = mapper.readTree(info1);
        JsonNode root2 = mapper.readTree(info2);
        Map<String, JsonNode> items1 = new HashMap<>();
        Map<String, JsonNode> items2 = new HashMap<>();
        //--------------------------//
        // collect items from info1 //
        //--------------------------//
        for (Iterator<Map.Entry<String, JsonNode>> it = root1.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> item = it.next();
            items1.put(item.getKey(), item.getValue());
        }
        //------------------------------------------------------//
        // collect items from info2 and compare each with info1 //
        //------------------------------------------------------//
        for (Iterator<Map.Entry<String, JsonNode>> it = root2.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> item = it.next();
            String key = item.getKey();
            JsonNode value = item.getValue();
            if (!items1.containsKey(key)) {
                return false;
            }
            JsonNode otherVal = items1.get(key);
            if (!equalJsonValues(value, otherVal)) {
                return false;
            }
            items2.put(key, value);
        }
        //-----------------------------------------//
        // see if info1 has any items not in info2 //
        //-----------------------------------------//
        for (Iterator<Map.Entry<String, JsonNode>> it = root1.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> item = it.next();
            String key = item.getKey();
            if (!items2.containsKey(key)) {
                return false;
            }
        }
        return true;
    }

    SqlDss getDb() throws IOException, CoreException, SQLException, EncodedDateTimeException {
        if (_db == null) {
            Path dir = Paths.get("build/test-artifacts", "CoreTest");
            Files.createDirectories(dir);
            String dbFileName = dir.resolve("tester.sqldss").toString();
            Files.deleteIfExists(Path.of(dbFileName));
            logger.atInfo().log("Opening new SqlDss: %s", dbFileName);
            _db = SqlDss.open(dbFileName);
        }
        return _db;
    }

    @Test
    public void testLocation() throws Exception {
        String locationName = "ctx:BaseLoc-SubLoc";
        try (SqlDss db = getDb()) {
            Connection conn = db.getConnection();
            //-------------------//
            // test merging info //
            //-------------------//
            Location.putLocation(locationName, conn);
            String[] existingInfo = new String[1];
            assertEquals(1, Location.getLocationKey(locationName, existingInfo, conn));
            assertEquals(null, existingInfo[0]);
            String info1 = """
                    {"elevation": 400, "vertical-datum": "NGVD29", "active": false}""";
            String info2 = """
                    {"elevation": 500, "active": true}""";
            String info3 = """
                    {"elevation": 500, "vertical-datum": "NGVD29", "active": true}""";
            String[] info = new String[1];
            Location.putLocation(locationName, info1, false, conn);
            Location.getLocationKey(locationName, info, conn);
            assertTrue(equalJsonStrings(info1, info[0]));
            Location.putLocation(locationName, info2, true, conn);
            Location.getLocationKey(locationName, info, conn);
            assertTrue(equalJsonStrings(info3, info[0]));
            Location.putLocation(locationName, info2, false, conn);
            Location.getLocationKey(locationName, info, conn);
            assertTrue(equalJsonStrings(info2, info[0]));
            assertThrows(
                    CoreException.class,
                    () -> Location.putLocation(
                            locationName,
                            "{\"elevation\": 500, \"active: true}",
                            false,
                            conn),
                    "Invalid JSON info:");
            //---------------------------------//
            // test get location keys and info //
            //---------------------------------//
            long key = Location.getLocationKey(locationName, conn);
            assertEquals(1, key);;
            info[0] = Location.getLocationInfo(key, conn);
            assertTrue(equalJsonStrings(info2, info[0]));
            info[0] = Location.getLocationInfo(locationName, conn);
            assertTrue(equalJsonStrings(info2, info[0]));
        }
    }
}
