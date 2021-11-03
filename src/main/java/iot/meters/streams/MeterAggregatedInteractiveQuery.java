package iot.meters.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import iot.meters.model.Aggregation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MeterAggregatedInteractiveQuery {

    private static Logger log = LoggerFactory.getLogger(MeterAggregatedInteractiveQuery.class);

    @ConfigProperty(name = "pod.ip")
    String podIP;

    String iotMetersAggregatedStore = "iot-meters-aggregated-store";

    @ConfigProperty(name = "quarkus.http.port")
    int httpPort;

    @Inject
    KafkaStreams streams;

    @Inject
    Vertx vertx;

    public Uni<String> getMeterStatus(String street) {
        return Uni.createFrom().item(() -> streams.queryMetadataForKey(iotMetersAggregatedStore, street, Serdes.String().serializer()))
            .onItem().transform(metadata -> {
                if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
                    log.warn("No metadata found for key: " + street);
                    return new ImmutablePair<Aggregation, String>(null, null);
                } else if (podIP.equals(metadata.activeHost().host()) && httpPort == metadata.activeHost().port()) {
                    log.debug("Key available on local host: " + street);
                    return new ImmutablePair<Aggregation, String>(store().get(street), null);
                } else {
                    log.debug("Key available on remote host " + metadata.activeHost().host() + ":" + metadata.activeHost().port() + ": " + street);
                    return new ImmutablePair<Aggregation, String>(null, metadata.activeHost().host() + ":" + metadata.activeHost().port());
                }
            }).onItem().transformToUni(p -> {
                if (p.getLeft() == null && p.getRight() == null) {
                    return Uni.createFrom().item("");
                } else if (p.getRight() == null) {
                    return Uni.createFrom().item(p.getLeft().toJson());
                } else {
                    WebClient client = WebClient.create(vertx);
                    String[] hostPort = p.getRight().split(":");
                    return client.get(Integer.parseInt(hostPort[1]), hostPort[0], "/street/" + street).send().onItem().transform(response -> {
                        if (response.statusCode() == Response.Status.OK.getStatusCode()) {
                            return response.bodyAsString();
                        } else if (response.statusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                            log.warn("Meter update with key " + street + " not found on remote host " + hostPort[0]);
                            return null;
                        } else {
                            log.warn("Unexpected response code " + response.statusCode() + " when retrieving meter status with key " + street + " on remote host " + hostPort[0]);
                            return null;
                        }
                    });
                }
            }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private ReadOnlyKeyValueStore<String, Aggregation> store() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(iotMetersAggregatedStore, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
