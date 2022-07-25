import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class OpenSearchConsumerMain {
    public static final Logger log = LoggerFactory.getLogger(OpenSearchConsumerMain.class);

    public static void main(String[] args) {
        String indexName = "wikimedia";

//        String connectionStr = "http://localhost:9200";
        String bonsaiConString = "https://n2x9yfro44:hppadb5lp4@learning-cluster-6713357189.us-west-2.bonsaisearch.net:443";
        RestHighLevelClient openSearchClient = OpenSearchHttpClient.createOpenSearchClient(bonsaiConString);
        KafkaConsumer<String, String> myKafkaConsumer = KafkaConsumerClient.getKafkaConsumerConsumer();
        /*create index if not exists*/
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        try (openSearchClient; myKafkaConsumer) {
            boolean exists = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (!exists) {
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("wikimedia index created");
            } else {
                log.info("already exists");
            }
            myKafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = myKafkaConsumer.poll(Duration.ofMillis(10000));
                int recorded = records.count();
                log.info("received {} records", recorded);
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    try {

                        //extract the id for idempotence
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest(indexName).source(record.value(), XContentType.JSON).id(id);
//                        IndexResponse indexResponse = openSearchClient.index(request, RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);
//                        log.info("inserted doc into open search with id: {}", indexResponse.getId());
                    } catch (Exception e) {
                        // ignore
                    }

                    //Sending the bulk request instead of individual request

                }
                if(bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse= openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("sent bulk requests for records {}", bulkResponse.getItems().length);
                }
                Thread.sleep(1000);
                //committing offset manually since auto commit is disabled
                myKafkaConsumer.commitAsync();
                log.info("offset committed records {}", recorded);

            }

        } catch (OpenSearchStatusException e) {//This block should never get executed
            log.info("Wikimedia index already exists....", e);
        } catch (Exception e) {
            log.error("got an exception ", e);
        }


        //main logic code
        // Close things


        //System.out.println("Main Class");
    }


    /*returns the id from json object
     */
    private static String extractId(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();

    }
}
