package io.conduktor.demo.wikimedia;



import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

  private final KafkaProducer<String, String> kafkaProducer;
  private final String topic;

  private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;

  }

  @Override public void onOpen() throws Exception {
    // intentionally blank
  }

  @Override public void onClosed() throws Exception {
    kafkaProducer.close();
  }

  @Override public void onMessage(String event, MessageEvent messageEvent) throws Exception {
    log.info("message received");
    //async
    kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

  }

  @Override public void onComment(String comment) throws Exception {
    //nothing here, intentionally blank
  }

  @Override public void onError(Throwable t) {
    log.error("Error while reading the stream", t);
  }
}
