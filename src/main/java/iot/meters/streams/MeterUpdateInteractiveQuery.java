package iot.meters.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

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

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@ApplicationScoped
public class MeterUpdateInteractiveQuery {

    private static Logger log = LoggerFactory.getLogger(MeterUpdateInteractiveQuery.class);

    @ConfigProperty(name = "pod.ip")
    String podIP;

    @ConfigProperty(name = "iot-meters-update-store")
    String iotMetersStore;

    @ConfigProperty(name = "quarkus.http.port")
    int httpPort;

    @Inject
    KafkaStreams streams;

    @Inject
    Vertx vertx;

    public Uni<String> getMeterUpdate(String id) {
        return Uni.createFrom().item(() -> streams.queryMetadataForKey(iotMetersStore, id, Serdes.String().serializer()))        
            .onItem().transform(metadata -> {
                if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
                    log.warn("No metadata found for key: " + id);
                    return new ImmutablePair<String, String>(null, null);
                } else if (podIP.equals(metadata.activeHost().host()) && httpPort == metadata.activeHost().port()) {
                    log.debug("Key available on local host: " + id);
                    return new ImmutablePair<String, String>(store().get(id), null);
                } else {
                    log.debug("Key available on remote host " + metadata.activeHost().host() + ":" + metadata.activeHost().port() + ": " + id);
                    return new ImmutablePair<String, String>(null, metadata.activeHost().host() + ":" + metadata.activeHost().port());
                }
            }).onItem().transformToUni(p -> {
                if (p.getLeft() == null && p.getRight() == null) {
                    return Uni.createFrom().item("");
                } else if (p.getRight() == null) {
                    return Uni.createFrom().item(p.getLeft());
                } else {
                    WebClient client = WebClient.create(vertx);
                    String[] hostPort = p.getRight().split(":");
                    return client.get(Integer.parseInt(hostPort[1]), hostPort[0], "/meter/" + id).send().onItem().transform(response -> {
                        if (response.statusCode() == Response.Status.OK.getStatusCode()) {
                            return response.bodyAsString();
                        } else if (response.statusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                            log.warn("Meter update with key " + id + " not found on remote host " + hostPort[0]);
                            return null;
                        } else {
                            log.warn("Unexpected response code " + response.statusCode() + " when retrieving meter update with key " + id + " on remote host " + hostPort[0]);
                            return null;
                        }
                    });
                }
            }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private ReadOnlyKeyValueStore<String, String> store() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(iotMetersStore, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
