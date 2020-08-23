package com.cleanair.airquality.dao;

import java.util.Map;

public class Measurement {
    private String location;
    private String parameter;
    private Map<String, String> date;
    private double value;
    private String unit;
    private Map<String, Double> coordinates;
    private String country;
    private String city;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public Map<String, String> getDate() {
        return date;
    }

    public void setDate(Map<String, String> date) {
        this.date = date;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Map<String, Double> getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(Map<String, Double> coordinates) {
        this.coordinates = coordinates;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return String.format("Measurement: Param=%s, Value=%f %s at %s, %s, %s.", parameter, value, unit, location, city, country);
    }
}
