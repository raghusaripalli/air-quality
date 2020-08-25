package com.cleanair.airquality.service;

import com.cleanair.airquality.dao.Measurement;
import com.cleanair.airquality.dao.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

@Service
public class ProducerService {
    private Logger log = LoggerFactory.getLogger(ProducerService.class);

    public void sendKafkaMessage(KafkaProducer<String, Measurement> producer, String topic) {
        RestTemplate restTemplate = new RestTemplate();
        long total = 50;
        int page = 1;
        int limit = 10000;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String past_hour = dateFormat.format(new Date(System.currentTimeMillis() - 3600 * 1000));
        do {
            UriComponents components = UriComponentsBuilder.newInstance()
                    .scheme("https")
                    .host("api.openaq.org")
                    .path("v1/measurements")
                    .queryParam("limit", limit)
                    .queryParam("page", page)
                    .queryParam("has_geo", true)
                    .queryParam("date_from", past_hour)
                    .build();
            String url = components.toUriString();
            log.info(url);
            ResponseEntity<Response> responseEntity = restTemplate.getForEntity(components.toUri(), Response.class);
            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                Response response = responseEntity.getBody();
                String found = Objects.requireNonNull(response).getMeta().get("found");
                total = (long) Math.ceil(Long.parseLong(found) / (limit * 1.0));
                response.getResults().forEach(measurement -> producer.send(new ProducerRecord<>(topic, measurement)));
            }
            ++page;
        } while (page < total);
    }
}
