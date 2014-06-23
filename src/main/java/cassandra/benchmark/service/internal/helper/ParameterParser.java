package cassandra.benchmark.service.internal.helper;

import cassandra.benchmark.service.internal.Constants;

import java.util.Map;

/**
 * Created by cosh on 02.06.14.
 */
public class ParameterParser {
    public static Integer extractBatchSize(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.batchSizeString);
        if (extractedParameterString != null) {
            return Integer.parseInt((extractedParameterString));
        }

        return -1;
    }

    private static String extractParameterString(Map<String, String> parameters, String interestingElement) {
        if (interestingElement != null) {
            if (parameters.containsKey(interestingElement)) {
                return parameters.get(interestingElement);
            }
        }

        return null;
    }

    public static Long extractnumberOfRowsCount(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.rowCountString);
        if (extractedParameterString != null) {
            return Long.parseLong((extractedParameterString));
        }

        return -1L;
    }

    public static Integer extractWideRowCount(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.wideRowCountString);
        if (extractedParameterString != null) {
            return Integer.parseInt((extractedParameterString));
        }

        return -1;
    }
}
