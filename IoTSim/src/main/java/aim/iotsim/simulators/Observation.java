package aim.iotsim.simulators;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 15.10.2013
 * Time: 12:06
 * To change this template use File | Settings | File Templates.
 */
public class Observation {

    private int ID;
    private int sender;
    private String UUID;
    private Date timeStamp;
    private int area;
    private double lat;
    private double lon;
    private double velocity;
    private int direction;

    public String getRdfType() {
        return rdfType;
    }

    public void setRdfType(String rdfType) {
        this.rdfType = rdfType;
    }

    private String rdfType;


    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public int getSender() {
        return sender;
    }

    public void setSender(int sender) {
        this.sender = sender;
    }

    public java.util.UUID getUUID() {
        //return java.util.UUID.fromString(this.getTimeStamp().toString()+String.valueOf(this.getID())+String.valueOf(this.getSender()));
        return java.util.UUID.randomUUID();
    }

    public void setUUID(String UUID) {
        this.UUID = UUID;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getArea() {
        return area;
    }

    public void setArea(int area) {
        this.area = area;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getVelocity() {
        return velocity;
    }

    public void setVelocity(double velocity) {
        this.velocity = velocity;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }


}
