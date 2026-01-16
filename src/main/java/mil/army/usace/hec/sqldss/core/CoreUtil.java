package mil.army.usace.hec.sqldss.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CoreUtil {

    private CoreUtil() {
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
}
