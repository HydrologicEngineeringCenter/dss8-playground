package mil.army.usace.hec.sqldss.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Util {

    private Util() {
        throw new AssertionError("Cannot instantiate");
    }

    public static byte[] gzipBytes(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
            gzip.finish();
        }
        return bos.toByteArray();
    }

    public static byte[] gunzipBytes(byte[] gzData) throws IOException {
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

    public static String mergeJsonStrings(String incoming, String existing) throws CoreException {
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
            throw new CoreException(e);
        }
    }
}
