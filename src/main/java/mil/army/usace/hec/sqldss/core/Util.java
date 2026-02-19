package mil.army.usace.hec.sqldss.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Generic utility class for SQLDSS
 */
public class Util {

    /**
     * Prevent class instantiation
     */
    private Util() {
        throw new AssertionError("Cannot instantiate");
    }

    /**
     * Gzip a byte array
     * @param data The data to gzip
     * @return The gzipped data
     * @throws IOException If thrown by Gzip code
     */
    public static byte @NotNull [] gzipBytes(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
            gzip.finish();
        }
        return bos.toByteArray();
    }

    /**
     * Gunzip a byte array
     * @param gzData The data to gunzip
     * @return The gzipped data
     * @throws IOException If thrown by Gzip code
     */
    public static byte @NotNull [] gunzipBytes(byte[] gzData) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(gzData));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buf = new byte[8192];
            int n;
            while ((n = gzip.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        }
    }

    /**
     * Verifies that a string is valid JSON
     * @param jsonStr The string to validate
     * @throws SqlDssException If <code>jsonStr</code> is not valid JSON
     */
    public static void validateJsonString(String jsonStr) throws SqlDssException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode existingRoot = mapper.readTree(jsonStr);
        } catch (JsonProcessingException e) {
            throw new SqlDssException(String.format("Invalid JSON info: %s", jsonStr));
        }
    }

    /**
     * Merges two JSON strings, replacing items in the existing string if they exist in the incoming string
     * @param incoming The "new" or "incoming" JSON string
     * @param existing The "old" or "existing" JSON string
     * @return The merged JSON string
     * @throws SqlDssException If one of the strings is not valid JSON
     */
    public static String mergeJsonStrings(String incoming, String existing) throws SqlDssException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode existingRoot = mapper.readTree(existing);
            JsonNode incomingRoot = mapper.readTree(incoming);
            HashMap<String, JsonNode> mergedItems = new HashMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> it = existingRoot.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> item = it.next();
                mergedItems.put((String) item.getKey(), (JsonNode) item.getValue());
            }
            // incoming will overwrite existing for same key
            for (Iterator<Map.Entry<String, JsonNode>> it = incomingRoot.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> item = it.next();
                mergedItems.put((String) item.getKey(), (JsonNode) item.getValue());
            }
            return mapper.writeValueAsString(mergedItems);
        }
        catch (JsonProcessingException e) {
            throw new SqlDssException(e);
        }
    }
}
