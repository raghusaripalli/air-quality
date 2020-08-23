package com.cleanair.airquality.dao;

import java.util.List;
import java.util.Map;

public class Response {
    private Map<String, String> meta;
    private List<Measurement> measurements;

    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    public List<Measurement> getResults() {
        return measurements;
    }

    public void setResults(List<Measurement> measurements) {
        this.measurements = measurements;
    }

}
