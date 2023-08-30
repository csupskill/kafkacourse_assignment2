package javaprof;

import javaprof.customdeserializers.TruckCoordinatesSerializer;
import javaprof.data.TruckCoordinates;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.setProperty("value.serializer", TruckCoordinatesSerializer.class.getName());

        try (KafkaProducer<Integer, TruckCoordinates> producer = new KafkaProducer<>(properties)) {
            TruckCoordinates truckCoordinates = new TruckCoordinates();
            truckCoordinates.setId(10);
            truckCoordinates.setLatitude("1234.5 N");
            truckCoordinates.setLongitude("6789.0 E");
            ProducerRecord<Integer, TruckCoordinates> record = new ProducerRecord<>("TruckInfo", truckCoordinates.getId(), truckCoordinates);
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
