package org.graylog2.plugins.kafka;

import org.graylog2.plugin.PluginModule;


public class KafkaInputPluginModule extends PluginModule {

    @Override
    protected final void configure() {
        addTransport("kafka-new-consumer-transport", KafkaTransport.class);
        addMessageInput(RawKafkaInput.class);
    }
}
