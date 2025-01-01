package com.kafka.entity;

public class CarLocation {

	private String idCar;
	
	private long timestamp;
	
	private int distance;

	public CarLocation() {}

	public CarLocation(String idCar, long timestamp, int distance) {
		super();
		this.idCar = idCar;
		this.timestamp = timestamp;
		this.distance = distance;
	}

	public String getIdCar() {
		return idCar;
	}

	public void setIdCar(String idCar) {
		this.idCar = idCar;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	@Override
	public String toString() {
		return "CarLocation [idCar=" + idCar + ", timestamp=" + timestamp + ", distance=" + distance + "]";
	}
	
}
