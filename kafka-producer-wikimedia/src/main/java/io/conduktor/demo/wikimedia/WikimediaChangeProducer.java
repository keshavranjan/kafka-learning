package io.conduktor.demo.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangeProducer {

  public static final String TOPIC_NAME = "wikimedia.recentchange";

  public static void main(String[] args) throws InterruptedException {


    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
    properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
    properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

    EventHandler handler = new WikimediaChangeHandler(kafkaProducer, TOPIC_NAME);

    String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url));

    EventSource source = builder.build();
    source.start();

    Thread.sleep(TimeUnit.MINUTES.toMillis(10));


  }

}
