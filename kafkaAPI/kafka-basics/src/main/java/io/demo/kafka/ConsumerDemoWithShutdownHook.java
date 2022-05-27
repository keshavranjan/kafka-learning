package io.demo.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdownHook {
  public static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdownHook.class.getSimpleName());
  public static final String BootStrapServer = "http://127.0.0.1:9092";
  public static final String GROUP_ID = "myapp-group";

  public static void main(String[] args) {
    log.info("Hello World");

    //consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootStrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    //create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
    final Thread mainThread= Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread(){

      @Override public void run() {
        System.out.println("detected a shutdown, let's exit by calling consumer.wakeup()...");
        consumer.wakeup();

        // join the main thread
        try {
          mainThread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    consumer.subscribe(Collections.singletonList("demo_java"));
    try{
    while(1==1){
      System.out.println("Polling... ");
      ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String,String> r:records) {

        System.out.println("Key: "+ r.key() + " value: " + r.value());
        System.out.println("Partition: "+ r.partition() + " Offset: " + r.offset());

      }

      }}catch (WakeupException exception){
      System.out.println("wake up exception");
      //ignore
    }catch (Exception e){
      System.out.println("unexpected exception" + e);

    }finally {
      consumer.close();
    }

    }
  }

