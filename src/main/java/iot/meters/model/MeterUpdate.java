package iot.meters.model;

public class MeterUpdate {

    public String id;
    public String address;
    public Double latitude;
    public Double longitude;
    public String previousStatus;
    public String status;
    public Long timestamp;

    public MeterUpdate(){
    }

    public MeterUpdate(String id, String address, Double latitude, Double longitude, String previousStatus, String status, Long timestamp) {
        this.id = id;
        this.address = address;
        this.latitude = latitude;
        this.longitude = longitude;
        this.previousStatus = previousStatus;
        this.status = status;
        this.timestamp = timestamp;
    }

    public String street() {
        if (address == null) {
            return null;
        }
        String[] splitted = address.split(" ",2);
        if (splitted.length < 2) {
            return splitted[0];
        } else {
            return splitted[1];
        }
    }

}
