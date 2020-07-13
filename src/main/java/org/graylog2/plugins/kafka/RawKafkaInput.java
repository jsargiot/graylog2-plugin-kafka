package org.graylog2.plugins.kafka;

import javax.inject.Inject;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

import org.graylog2.inputs.codecs.RawCodec;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;


public class RawKafkaInput extends MessageInput {

    @AssistedInject
    public RawKafkaInput(final MetricRegistry metricRegistry,
                         final @Assisted Configuration configuration,
                         final KafkaTransport.Factory factory,
                         final LocalMetricRegistry localRegistry,
                         final RawCodec.Factory codecFactory,
                         final Config config,
                         final Descriptor descriptor,
                         final ServerStatus serverStatus) {
        super(metricRegistry,
              configuration,
              factory.create(configuration),
              localRegistry,
              codecFactory.create(configuration),
              config,
              descriptor,
              serverStatus);
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<RawKafkaInput> {
        @Override
        RawKafkaInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super("Raw/Plaintext Kafka (New Consumer)", false, "");
        }
    }

    public static class Config extends MessageInput.Config {
        @Inject
        public Config(final KafkaTransport.Factory transport,
                      final RawCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
