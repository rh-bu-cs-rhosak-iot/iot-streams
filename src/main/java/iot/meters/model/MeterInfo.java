package iot.meters.model;

public class MeterInfo {

    public String id;
    public String address;
    public Double latitude;
    public Double longitude;

    public MeterInfo(){
    }

    public MeterInfo(String id, String address, Double latitude, Double longitude){
        this.id = id;
        this.address = address;
        this.latitude = latitude;
        this.longitude = longitude;
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
