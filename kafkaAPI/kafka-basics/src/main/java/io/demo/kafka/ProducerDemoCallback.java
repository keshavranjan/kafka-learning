package io.demo.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallback {
  public static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class.getSimpleName());

  public static void main(String[] args) throws InterruptedException {
    log.info("Hello World");

    //producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    //    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world1");

    //send data- async
    int i = 0;
    while (i < 10) {
      i++;
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello_world" + i);

      producer.send(producerRecord, new Callback() {
        @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (null == exception) {
            System.out.println("received metadata partition: " + metadata.partition());
          } else {
            log.error("error", exception);
          }
        }
      });
      Thread.sleep(1000);
    }
    //flush and close the producer
    producer.flush();


  }
}
