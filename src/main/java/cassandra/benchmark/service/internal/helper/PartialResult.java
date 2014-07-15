package cassandra.benchmark.service.internal.helper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cosh on 15.07.14.
 */
public class PartialResult {
    private final List<Long> measures;
    private final List<String> errors;

    public PartialResult() {
        errors = new ArrayList<String>();
        measures = new ArrayList<Long>();
    }

    public void addMeasurement(Long measurement) {
        measures.add(measurement);
    }

    public void addErrors(String errorMessage) {
        errors.add(errorMessage);
    }

    public List<Long> getMeasures() {
        return measures;
    }

    public List<String> getErrors() {
        return errors;
    }
}
