package javaprof.customdeserializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javaprof.data.TruckCoordinates;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class TruckCoordinatesSerializer implements Serializer<TruckCoordinates> {

    @Override
    public byte[] serialize(String s, TruckCoordinates data) {
        try {
            ObjectMapper mapper = new ObjectMapper();

            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
