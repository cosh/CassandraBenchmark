package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 20.06.14.
 */
public interface ScenarioPluginManager {
    /**
     * Returns the available scenarios
     * @return The available scenarios
     */
    Map<String, Scenario> getAllAvailableScenarios();

    /**
     * Tries to get a scenario
     * @param scenario The scenario if it's available, otherwise null
     * @param name The name of the scenario
     * @return True for success otherwise false
     */
    boolean tryGetScenario(Scenario scenario, final String name);

}
