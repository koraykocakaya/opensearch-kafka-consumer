package com.kk.opensearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author korayk
 *
 */
public class OpenSearchConsumer {

	private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
	
	private static final String bootstrapServerAddress = "10.10.10.122:9092";
	private static final String topic = "wikimedia.change";
	private static final int timeout_poll = 3000;

	
	public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//      String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

      // we build a URI from the connection string
      RestHighLevelClient restHighLevelClient;
      URI connUri = URI.create(connString);
      // extract login information if it exists
      String userInfo = connUri.getUserInfo();

      if (userInfo == null) {
          // REST client without security
          restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

      } else {
          // REST client with security
          String[] auth = userInfo.split(":");

          CredentialsProvider cp = new BasicCredentialsProvider();
          cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

          restHighLevelClient = new RestHighLevelClient(
                  RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                          .setHttpClientConfigCallback(
                                  httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                          .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


      }

      return restHighLevelClient;
    }
	
	public static KafkaConsumer<String, String> createKafkaConsumer(){
		
		String groupId = "consumer-opensearch";
		
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerAddress);
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
		
		return consumer;
	}
	
	public static void main(String[] args) throws IOException {
		
		String indexName = "wikimedia";
		RestHighLevelClient openSearchClient = createOpenSearchClient();
		log.info("Opensarch client connected");
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		
		try(openSearchClient;consumer){
			
			CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
			if(!openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
				openSearchClient.indices().create(indexRequest, RequestOptions.DEFAULT);
				log.info("Index created");
			} else {
				log.info("Index already created");
			}

			consumer.subscribe(Collections.singleton(topic));
			
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeout_poll));
				log.info(records.count() + " record(s) received");
				for(ConsumerRecord<String, String> record: records) {
					try {
						IndexRequest requestForInsert = new IndexRequest(indexName)
								.source(record.value(), XContentType.JSON);
						IndexResponse resp = openSearchClient.index(requestForInsert, RequestOptions.DEFAULT);
						log.info(resp.getId());
					} catch (Exception e) {
						log.error("Exception thrown", e);
					}
					

				}
			}
			

		}
		

		
		
	}
}
