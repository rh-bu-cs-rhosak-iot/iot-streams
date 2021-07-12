package iot.meters.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TopologyProducer {

    @ConfigProperty(name = "iot-meters-topic", defaultValue = "iot-meters")
    String iotMetersTopic;

    @ConfigProperty(name = "iot-meters-update-store")
    String iotMetersStore;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(iotMetersTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .map((k, v) -> {
                if (k.startsWith("\"")) {
                    return KeyValue.pair(StringUtils.strip(k, "\""), v);
                } else {
                    return KeyValue.pair(k, v);
                } 
            })
            .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(iotMetersStore).withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

        return builder.build();
    } 
}
