package mil.army.usace.hec.sqldss.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BaseParameter {

    private BaseParameter() {
        throw new AssertionError("Cannot instantiate");
    }

    static String[] baseParameters;
    static {
        try (InputStream in = BaseParameter.class.getResourceAsStream("init/base_parameter.tsv")) {
            if (in == null) {
                throw new CoreException("Could not open base_parameter resource");
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                List<String> baseParamList = new ArrayList<>();
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isEmpty() || line.startsWith("*")) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    baseParamList.add(parts[0]);
                }
                baseParameters = baseParamList.toArray(new String[0]);
            }
        }
        catch (IOException | CoreException e) {
            throw new RuntimeException(e);
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
