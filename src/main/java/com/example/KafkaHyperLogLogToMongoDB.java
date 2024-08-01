package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import com.mongodb.client.model.UpdateOptions;

public class KafkaHyperLogLogToMongoDB {
    private static final Logger logger = LoggerFactory.getLogger(KafkaHyperLogLogToMongoDB.class);

    public static void main(String[] args) {
        // Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "your_consumer_group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the earliest offset if no previous offsets are found

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile("data-topic"));

        // MongoDB configuration
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("viewer_stats");
        MongoCollection<Document> collection = database.getCollection("unique_data_subjects");

        logger.info("Connected to MongoDB");

        // Map to store HyperLogLog for each ViewerID
        Map<String, HyperLogLog> viewerIdHLLMap = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    logger.info("No new records found.");
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumed record: {}", record.value());
                    String[] parts = record.value().split(":");
                    if (parts.length == 3) {
                        String timestamp = parts[0];
                        String dataSubjectId = parts[1];
                        String viewerId = parts[2];

                        viewerIdHLLMap.computeIfAbsent(viewerId, k -> new HyperLogLog(0.01)).offer(dataSubjectId);
                    }
                }

                for (Map.Entry<String, HyperLogLog> entry : viewerIdHLLMap.entrySet()) {
                    String viewerId = entry.getKey();
                    HyperLogLog hll = entry.getValue();

                    // Fetch existing unique count from MongoDB
                    Document existingDocument = collection.find(eq("viewer_id", viewerId)).first();
                    if (existingDocument != null) {
                        long existingCount = existingDocument.getLong("unique_data_subjects");
                        HyperLogLog existingHLL = new HyperLogLog(0.01);
                        for (int i = 0; i < existingCount; i++) {
                            existingHLL.offer(i);
                        }
                        hll.addAll(existingHLL);
                    }

                    long uniqueCount = hll.cardinality();

                    // Upsert operation to update existing document or insert a new one
                    Bson filter = eq("viewer_id", viewerId);
                    Bson update = set("unique_data_subjects", uniqueCount);
                    logger.info("Upserting document for viewer_id {}: unique_data_subjects={}", viewerId, uniqueCount);
                    collection.updateOne(filter, update, new UpdateOptions().upsert(true));
                    logger.info("Upserted document for viewer_id {}: unique_data_subjects={}", viewerId, uniqueCount);
                }

                viewerIdHLLMap.clear(); // Clear the map after upserting results to MongoDB
            }
        } catch (Exception e) {
            logger.error("Error occurred while processing records", e);
        } finally {
            consumer.close();
            mongoClient.close();
        }
    }
}

