package cassandra.benchmark.service.internal.scenario;

import java.util.Map;

/**
 * Created by cosh on 20.06.14.
 */
public interface ScenarioPluginManager {
    /**
     * Returns the available scenarios
     *
     * @return The available scenarios
     */
    Map<String, Scenario> getAllAvailableScenarios();

    /**
     * Gets a scenario
     *
     * @param name The Name of the scenario
     * @return The scenario if it's available, otherwise null
     */
    Scenario tryGetScenario(final String name);

}
