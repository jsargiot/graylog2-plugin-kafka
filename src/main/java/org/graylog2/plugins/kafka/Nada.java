package org.graylog2.plugins.kafka;

import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Nada {

    public static void main(String [] args)
    {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode jsonNode = mapper.readTree("{\"args\": {\"use_proxy\": \"true\"}, \"qsize\": 0, \"client_ip\": \"172.19.0.1\"}");
            Map result = mapper.convertValue(jsonNode, Map.class);
            System.out.println(result.getClass().getSimpleName());
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
