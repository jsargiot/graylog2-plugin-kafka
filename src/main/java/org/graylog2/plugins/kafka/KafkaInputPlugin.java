package org.graylog2.plugins.kafka;

import java.util.Arrays;
import java.util.Collection;

import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.PluginModule;


public class KafkaInputPlugin implements Plugin {

    @Override
    public final PluginMetaData metadata() {
        return new KafkaInputPluginMetaData();
    }

    @Override
    public final Collection<PluginModule> modules() {
        return Arrays.<PluginModule>asList(new KafkaInputPluginModule());
    }
}
