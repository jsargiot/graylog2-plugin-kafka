package org.graylog2.plugins.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.system.NodeId;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import static com.codahale.metrics.MetricRegistry.name;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;


public class KafkaTransport extends ThrottleableTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransport.class);

    public static final String CK_GROUP_ID = "graylog2-kafka-input";
    public static final String CK_BOOTSTRAP_SERVERS = "bootstrap_servers";
    public static final String CK_TOPIC_FILTER = "topic_filter";
    public static final String CK_THREADS = "threads";
    public static final String CK_CONSUMER_PROPERTIES = "consumer_properties";

    private final Configuration configuration;
    private final MetricRegistry localRegistry;
    private final NodeId nodeId;
    private final EventBus serverEventBus;
    private final ServerStatus serverStatus;
    private final ScheduledExecutorService scheduler;
    private final MetricRegistry metricRegistry;
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesRead = new AtomicLong(0);
    private final AtomicLong lastSecBytesReadTmp = new AtomicLong(0);

    private volatile boolean stopped = false;
    private volatile boolean paused = true;
    private volatile CountDownLatch pausedLatch = new CountDownLatch(1);

    private CountDownLatch stopLatch;
    private ArrayList<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();

    @AssistedInject
    public KafkaTransport(final @Assisted Configuration conf,
                          final LocalMetricRegistry registry,
                          final NodeId nId,
                          final EventBus eventBus,
                          final ServerStatus status,
                          final @Named("daemonScheduler")
                                ScheduledExecutorService nscheduler) {
        super(eventBus, conf);
        this.configuration = conf;
        this.localRegistry = registry;
        this.nodeId = nId;
        this.serverEventBus = eventBus;
        this.serverStatus = status;
        this.scheduler = nscheduler;
        this.metricRegistry = localRegistry;

        localRegistry.register("read_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastSecBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_1sec", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
        localRegistry.register("read_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return totalBytesRead.get();
            }
        });
        localRegistry.register("written_bytes_total", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return 0L;
            }
        });
    }

    @Subscribe
    public final void lifecycleStateChange(final Lifecycle lifecycle) {
        LOGGER.debug("Lifecycle changed to {}", lifecycle);
        switch (lifecycle) {
            case PAUSED:
            case FAILED:
            case HALTING:
                pausedLatch = new CountDownLatch(1);
                paused = true;
                break;
            default:
                paused = false;
                pausedLatch.countDown();
                break;
        }
    }

    @Override
    public void setMessageAggregator(final CodecAggregator ignored) {
    }

    @Override
    public final void doLaunch(final MessageInput input) throws MisfireException {
        serverStatus.awaitRunning(new Runnable() {
            @Override
            public void run() {
                lifecycleStateChange(Lifecycle.RUNNING);
            }
        });

        // listen for lifecycle changes
        serverEventBus.register(this);

        // Add default values to properties so they can be overrided later
        Properties props = new Properties();
        props.put("group.id", CK_GROUP_ID);
        props.put("auto.commit.interval.ms", "1000");
        props.put("bootstrap.servers", configuration.getString(CK_BOOTSTRAP_SERVERS));

        try {
            // Overwrite defaults with consumer properties config
            // file (if exists)
            props.putAll(getConsumerPropertiesFromFile(CK_CONSUMER_PROPERTIES));
        } catch (IOException e) {
            doStop();
            throw new MisfireException("Unable to read consumer properties file.", e);
        }

        // Set default commit handling
        props.put("enable.auto.commit", "true");

        final String clientId = input.getId() + "-" + nodeId + "-%d";
        final int numThreads = configuration.getInt(CK_THREADS);
        final ExecutorService executor = executorService(numThreads, clientId);
        stopLatch = new CountDownLatch(numThreads);

        // Launch every consumer thread
        for (int i = 0; i < numThreads; i++) {
            // Set id for this Thread
            props.put("client.id", String.format(clientId, i));
            // Create consumer
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
            // Subscribe it to topics that matches pattern
            consumer.subscribe(Pattern.compile(configuration.getString(CK_TOPIC_FILTER)), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) { }

                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) { }
            });
            // Add succesfully created KafkaConsumer to the group of consumers
            consumers.add(consumer);

            // Launch consumer thread
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    do {
                        try {
                            // Get available messages
                            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(1000);
                            final Iterator<ConsumerRecord<byte[], byte[]>> consumerIterator = consumerRecords.iterator();

                            // we have to use hasNext() here instead foreach,
                            // because next() marks the message as processed
                            // immediately
                            while (consumerIterator.hasNext()) {
                                if (paused) {
                                    // we try not to spin here, so we wait until
                                    // the lifecycle goes back to running.
                                    LOGGER.debug("Message processing is paused, blocking until message processing is turned back on.");
                                    Uninterruptibles.awaitUninterruptibly(pausedLatch);
                                }
                                // check for being stopped before actually
                                // getting the message, otherwise we could end
                                // up losing that message
                                if (stopped) {
                                    break;
                                }

                                if (isThrottled()) {
                                    blockUntilUnthrottled();
                                }

                                // process the message, this will immediately
                                // mark the message as having been processed.
                                // This gets tricky if we get an exception
                                // about processing it down below.
                                final byte[] bytes = consumerIterator.next().value();

                                // it is possible that the message is null
                                if (bytes == null) {
                                    continue;
                                }
                                // Update stats
                                totalBytesRead.addAndGet(bytes.length);
                                lastSecBytesReadTmp.addAndGet(bytes.length);

                                // Process message
                                final RawMessage rawMessage = new RawMessage(bytes);
                                input.processRawMessage(rawMessage);
                            }
                        } catch (KafkaException ke) {
                            LOGGER.error("Kafka consumer error, stopping consumer.", ke);
                            doStop();
                        }
                    } while (!stopped);
                    // explicitly commit our offsets when stopping.
                    // this might trigger a couple of times, but it won't hurt
                    consumer.commitAsync();
                    stopLatch.countDown();
                }
            });
        }

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                lastSecBytesRead.set(lastSecBytesReadTmp.getAndSet(0));
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private Properties getConsumerPropertiesFromFile(final String configKey) throws IOException {
        FileInputStream stream = null;
        Properties props = new Properties();
        String consumerPropertiesPath = configuration.getString(configKey);

        if (consumerPropertiesPath != null && !"".equals(consumerPropertiesPath)) {
            try {
                stream = new FileInputStream(new File(consumerPropertiesPath));
                props.load(stream);
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }
        }
        return props;
    }

    private ExecutorService executorService(final int numThreads, final String nameFormat) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
        return new InstrumentedExecutorService(
                Executors.newFixedThreadPool(numThreads, threadFactory),
                metricRegistry,
                name(this.getClass(), "executor-service"));
    }

    @Override
    public final void doStop() {
        stopped = true;

        serverEventBus.unregister(this);

        if (stopLatch != null) {
            try {
                // unpause the processors if they are blocked. this will cause
                // them to see that we are stopping, even if they were paused.
                if (pausedLatch != null && pausedLatch.getCount() > 0) {
                    pausedLatch.countDown();
                }
                final boolean allStoppedOrderly = stopLatch.await(5, TimeUnit.SECONDS);
                stopLatch = null;
                if (!allStoppedOrderly) {
                    // timed out
                    LOGGER.info("Stopping Kafka input timed out "
                            + "(waited 5 seconds for consumer threads to stop)."
                            + " Forcefully closing connection now. "
                            + "This is usually harmless when stopping the input.");
                }
            } catch (InterruptedException e) {
                LOGGER.debug("Interrupted while waiting to stop input.");
            }
        }

        for (KafkaConsumer consumer : consumers) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOGGER.error("Error while stopping consumer", e);
            }
        }
        consumers.clear();
        LOGGER.debug("Kafka (New Consumer) Input stopped succesfully");
    }

    @Override
    public final MetricSet getMetricSet() {
        return localRegistry;
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<KafkaTransport> {
        @Override
        KafkaTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends ThrottleableTransport.Config {
        @Override
        public final ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = super.getRequestedConfiguration();

            cr.addField(new TextField(
                    CK_BOOTSTRAP_SERVERS,
                    "Bootstrap Servers",
                    "127.0.0.1:9092",
                    "Comma-separated list of bootstrapping brokers addresses in the form 'name:port' or 'ip:port'",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new TextField(
                    CK_TOPIC_FILTER,
                    "Topic filter regex",
                    "^.*$",
                    "Subscribe to all topics matching specified pattern.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new NumberField(
                    CK_THREADS,
                    "Processor threads",
                    1,
                    "Number of processor threads to spawn. Use one thread per Kafka topic partition.",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new TextField(
                    CK_CONSUMER_PROPERTIES,
                    "Consumer properties file",
                    "",
                    "Optional path to a consumer properties file.",
                    ConfigurationField.Optional.OPTIONAL));

            return cr;
        }
    }
}
