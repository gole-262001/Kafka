package org.example;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        String topic = "mytopic";
        String brokeurl = "localhost:9092";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokeurl);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>(topic, "mykey" + i, "myvalue" + i)
                    , new Callback() {
                        @Override
                        public void onCompletion (RecordMetadata metadata, Exception exception){
                            if (exception != null) {
                                System.out.println("Exception");
                                exception.printStackTrace();

                            } else {
                                System.out.println("Sent successfully" + metadata.offset() + "==>" + metadata.partition());
                            }
                        }
                    });
        }
    }
}