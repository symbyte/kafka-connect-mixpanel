package org.apache.kafka.connect.mixpanel;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.*;

/**
 * Implementation of the Connector to pull event data from Mixpanel and store it into a Kafka topic.
 * Created by Kostas.
 */
public class MixPanelConnector extends SourceConnector {

    private static final String TOPIC_NAME = "topic";
    private static final String API_KEY = "api_key";
    private static final String API_SECRET = "api_secret";
    private static final String POLL_FREQUENCY = "poll_frequency";
    private static final String UPDATE_WINDOW = "update_window";

    private String topic;
    private String api_key;
    private String api_secret;
    private String poll_frequency;
    private String update_window;

    private static final ConnectException invalidPollFrequencyException =
      new ConnectException("poll_frequency must be a valid integer greater than 0");

    private static final ConnectException invalidUpdateWindowException =
      new ConnectException("update_window must be a valid integer greater than 0");

    private static final ConfigDef CONFIG_DEF = new ConfigDef();

    @Override
    public ConfigDef config() {
      return CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        ConfigDef configDef = config();
        List<ConfigValue> configValues = configDef.validate(connectorConfigs);
        return new Config(configValues);
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Starts the connector and check that the provider configuration is complete.
     */
    @Override
    public void start(Map<String, String> map) {

        topic = map.get(TOPIC_NAME);
        api_key = map.get(API_KEY);
        api_secret = map.get(API_SECRET);

        if (topic == null || topic.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException("MixPanelConnector should only have a single topic when used as a source.");
        if(api_key == null || api_key.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'api key' setting");
        if(api_secret == null || api_secret.isEmpty())
            throw new ConnectException("MixPanelConnector configuration must include 'api secret' setting");
        try {
          poll_frequency = map.get(POLL_FREQUENCY);
          int poll_frequency_int = Integer.parseInt(poll_frequency);

          if (poll_frequency_int <= 0) {
            throw MixPanelConnector.invalidPollFrequencyException;
          }
        } catch (NumberFormatException e) {
          throw MixPanelConnector.invalidPollFrequencyException;
        }

        try {
          update_window = map.get(UPDATE_WINDOW);
          int update_window_int = Integer.parseInt(update_window);

          if(update_window_int <= 0) {
            throw MixPanelConnector.invalidUpdateWindowException;
          }
        } catch (NumberFormatException e) {
          throw MixPanelConnector.invalidUpdateWindowException;
        }
    }

    /**
     * Create configurations for the tasks based on the current configuration of the Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MixPanelTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

            Map<String, String> config = new HashMap<>();
            config.put(TOPIC_NAME, topic);
            config.put(API_KEY, api_key);
            config.put(API_SECRET, api_secret);
            config.put(POLL_FREQUENCY, poll_frequency);
            config.put(UPDATE_WINDOW, update_window);
            configs.add(config);

        return configs;
    }

    @Override
    public void stop() {

    }
}
