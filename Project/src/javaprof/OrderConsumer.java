package javaprof;

import javaprof.customserializers.TruckCoordinatesDeserializer;
import javaprof.data.TruckCoordinates;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty("value.deserializer", TruckCoordinatesDeserializer.class.getName());
        properties.setProperty("group.id", "orderGroup");

        KafkaConsumer<Integer, TruckCoordinates> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("TruckInfo"));

        ConsumerRecords<Integer, TruckCoordinates> truckInfoRecords = consumer.poll(Duration.ofMillis(750));

        for (ConsumerRecord<Integer, TruckCoordinates> record : truckInfoRecords) {
            System.out.println("The truck with id " + record.key() + " is at Long: " + record.value().getLongitude() + ", Lat: " + record.value().getLatitude());
        }

        consumer.close();
    }
}
