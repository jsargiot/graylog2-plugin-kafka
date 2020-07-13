# Graylog Kafka (New Consumer) Input Plugin

Kafka Input Plugin for (Graylog2)[https://www.graylog.com] that uses the (New Consumer API)[https://kafka.apache.org/090/documentation.html#newconsumerapi].

## Warning!

This input plugin is not compatible with the old (and default) Kafka input plugin in Graylog2. This new consumer stores the offsets in Kafka itself while the default Kafka Input stores them in ZooKeeper. Although there are some procedures to migrate ZooKeeper offsets to Kafka this isn't always recomended for production use. The best approach is to transition to this plugin gradually.

## Getting Started

* Download the latest jar from the (releases page)[FIXME] and place it into the `plugin_dir` directory specified in you `graylog.conf` file. For more information refer to Graylog doc about (installing and loading plugins)[http://docs.graylog.org/en/2.4/pages/plugins.html#installing-and-loading-plugins].

* Restart Graylog server.

* Create a new `Input` from the web UI of type `Raw/Plaintext Kafka (New Consumer)`
