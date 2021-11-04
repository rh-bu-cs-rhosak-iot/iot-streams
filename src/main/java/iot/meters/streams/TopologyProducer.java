package iot.meters.streams;

import java.util.Collections;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.debezium.serde.DebeziumSerdes;
import io.quarkus.kafka.client.serialization.JsonbSerde;
import io.vertx.core.json.JsonObject;
import iot.meters.model.Aggregation;
import iot.meters.model.MeterInfo;
import iot.meters.model.MeterUpdate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TopologyProducer {

    private static final Logger log = LoggerFactory.getLogger(TopologyProducer.class);

    @ConfigProperty(name = "iot-meters-topic", defaultValue = "iot-meters")
    String iotMetersTopic;

    @ConfigProperty(name = "iot-meters-enriched-topic", defaultValue = "iot-meters-enriched")
    String iotMetersEnrichedTopic;

    @ConfigProperty(name = "iot-change-events-topic")
    String iotChangeEventsTopic;

    @ConfigProperty(name = "iot-meters-update-store")
    String iotMetersUpdateStore;

    @ConfigProperty(name = "iot-meters-aggregated-store")
    String iotMetersAggregatedStore;

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        final Serde<MeterInfo> meterInfoSerde = DebeziumSerdes.payloadJson(MeterInfo.class);
        meterInfoSerde.configure(Collections.singletonMap("from.field", "after"), false);

        final JsonbSerde<Aggregation> aggregationSerde = new JsonbSerde<>(Aggregation.class);
        final JsonbSerde<MeterUpdate> meterUpdateSerde = new JsonbSerde<>(MeterUpdate.class);

        KTable<String, MeterInfo> meters = builder.stream(iotChangeEventsTopic, Consumed.with(Serdes.String(), meterInfoSerde))
                .map((KeyValueMapper<String, MeterInfo, KeyValue<String, MeterInfo>>) (s, meterInfo) -> {
                    log.debug("Mapping meter info for key " + meterInfo.id);
                    return KeyValue.pair(meterInfo.id, meterInfo);
                }).repartition(Repartitioned.with(Serdes.String(), meterInfoSerde).withName("meter-info-ktable"))
                .toTable();

        builder.stream(iotMetersTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .join(meters, (meterStatus, meterInfo) -> {
                    log.debug("Joining meter update to meter info with ID " + meterInfo.id);
                    MeterUpdate meterUpdate = new MeterUpdate();
                    meterUpdate.id = meterInfo.id;
                    meterUpdate.address = meterInfo.address;
                    meterUpdate.latitude = meterInfo.latitude;
                    meterUpdate.longitude = meterInfo.longitude;
                    JsonObject json1 = new JsonObject(meterStatus);
                    meterUpdate.previousStatus = json1.getString("prev");
                    meterUpdate.status = json1.getString("status");
                    meterUpdate.timestamp = json1.getLong("timestamp");
                    return meterUpdate;
                }, Joined.with(Serdes.String(), Serdes.String(), meterInfoSerde))
                .to(iotMetersEnrichedTopic, Produced.with(Serdes.String(), meterUpdateSerde));

        builder.stream(iotMetersEnrichedTopic, Consumed.with(Serdes.String(), meterUpdateSerde))
                .groupBy((key, value) -> {
                    log.debug("Grouping meter records by street " + value.street());
                    return value.street();
                }, Grouped.with("iot-meters-grouped", Serdes.String(), meterUpdateSerde))
                .aggregate(() -> new Aggregation("",0,0,0,0L), (key, update, aggregate) -> {
                    log.debug("Aggregating meter records by street " + update.street());
                    aggregate.street = key;
                    return aggregate.aggregate(update.status, update.previousStatus, update.timestamp);
                }, Named.as("iot-meters-aggregated"), Materialized.<String, Aggregation,
                        KeyValueStore<Bytes, byte[]>>as(iotMetersAggregatedStore).withKeySerde(Serdes.String()).withValueSerde(aggregationSerde));

        builder.stream(iotMetersTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(iotMetersUpdateStore).withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));

        Topology topology = builder.build();
        log.info(topology.describe().toString());
        return topology;
    } 
}
