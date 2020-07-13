package org.graylog2.plugins.kafka;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;


public class KafkaInputPluginMetaData implements PluginMetaData {

    private static final String PLUGIN_PROPERTIES = "org.graylog2.plugins.graylog2-plugin-input-kafka/graylog-plugin.properties";

    @Override
    public final String getUniqueId() {
        return KafkaInputPlugin.class.getCanonicalName();
    }

    @Override
    public final String getName() {
        return "Kafka (New Consumer) Input";
    }

    @Override
    public final String getAuthor() {
        return "Joaquin Sargiotto";
    }

    @Override
    public final URI getURL() {
        return URI.create("https://www.graylog.org/");
    }

    @Override
    public final Version getVersion() {
        return Version.fromPluginProperties(this.getClass(),
                                            PLUGIN_PROPERTIES,
                                            "version",
                                            Version.from(0, 0, 0, "unknown"));
    }

    @Override
    public final String getDescription() {
        return "Kafka Input Plugin that uses the new Consumer API.";
    }

    @Override
    public final Version getRequiredVersion() {
        return Version.fromPluginProperties(this.getClass(),
                                            PLUGIN_PROPERTIES,
                                            "graylog.version",
                                            Version.CURRENT_CLASSPATH);
    }

    @Override
    public final Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.emptySet();
    }
}
