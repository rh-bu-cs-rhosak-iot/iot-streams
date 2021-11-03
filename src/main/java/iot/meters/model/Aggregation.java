package iot.meters.model;

import java.util.Objects;

import io.vertx.core.json.JsonObject;

public class Aggregation {

    public String street;
    public Integer available;
    public Integer occupied;
    public Integer outoforder;
    public Long timestamp;

    public Aggregation() {}

    public Aggregation(String street, Integer available, Integer occupied, Integer outoforder, Long timestamp) {
        this.street = street;
        this.available = available;
        this.occupied = occupied;
        this.outoforder = outoforder;
        this.timestamp = timestamp;
    }

    public Aggregation aggregate(String status, String previousStatus, Long timestamp) {
        this.timestamp = timestamp;
        if (Objects.equals(status, "available")) {
            this.available++;
        } else if (Objects.equals(status, "occupied")) {
            this.occupied++;
        } else if (Objects.equals(status, "out-of-order")) {
            this.outoforder++;
        }
        if (Objects.equals(previousStatus, "available") && available > 0) {
            this.available--;
        } else if (Objects.equals(previousStatus, "occupied") && occupied > 0) {
            this.occupied--;
        } else if (Objects.equals(previousStatus, "out-of-order") && outoforder > 0) {
            this.outoforder--;
        }
        return this;
    }

    public String toJson() {
        return new JsonObject().put("street", street).put("available", available).put("occupied", occupied)
                .put("outoforder", outoforder).put("updated", timestamp).encode();
    }
}
