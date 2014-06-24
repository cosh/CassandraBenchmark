package cassandra.benchmark.service.internal.helper;

import cassandra.benchmark.service.internal.Constants;

import java.awt.*;
import java.util.Map;

/**
 * Helper class for parsing the parameter from the execution context
 *
 * Created by cosh on 02.06.14.
 */
public class ParameterParser {

    /**
     * Extracts the batch size
     * @param parameter The parameter map
     * @return The correct batch size or -1
     */
    public static Integer extractBatchSize(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.batchSizeString);
        if (extractedParameterString != null) {
            return Integer.parseInt((extractedParameterString));
        }

        return null;
    }

    /**
     * Extracts the interesting element from the parameter map
     * @param parameters The parameter map
     * @param interestingElement The interesting element/string
     * @return The interesting value of the string or null
     */
    private static String extractParameterString(Map<String, String> parameters, String interestingElement) {
        if (interestingElement != null) {
            if (parameters.containsKey(interestingElement)) {
                return parameters.get(interestingElement);
            }
        }

        return null;
    }

    /**
     * Extracts the number of rows from the parameter map
     * @param parameter The parameter map
     * @return The number of rows or null
     */
    public static Long extractnumberOfRowsCount(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.rowCountString);
        if (extractedParameterString != null) {
            return Long.parseLong((extractedParameterString));
        }

        return null;
    }

    /**
     * Extracts the number of columns per row
     * @param parameter The parameter map
     * @return The number of columns per row or null
     */
    public static Integer extractColumnCountPerRow(final Map<String, String> parameter) {
        String extractedParameterString = extractParameterString(parameter, Constants.columnsPerRowCount);
        if (extractedParameterString != null) {
            return Integer.parseInt((extractedParameterString));
        }

        return null;
    }
}
