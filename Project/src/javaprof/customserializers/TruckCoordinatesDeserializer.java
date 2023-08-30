package javaprof.customserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import javaprof.data.TruckCoordinates;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckCoordinatesDeserializer implements Deserializer<TruckCoordinates> {

    @Override
    public TruckCoordinates deserialize(String s, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.readValue(data, TruckCoordinates.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
