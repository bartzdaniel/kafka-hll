# Data Processing Pipeline
================================================================================


## Setup
-----------

1. Clone the repository and navigate to the project directory:

```bash
   git clone git@github.com:bartzdaniel/kafka-hll.git
   cd kafka-hll
   ```

2. Run Docker Compose to set up the environment:
```bash
docker-compose up
```
3. Generate 10,000,000 random entries and feed kafka topic:

```bash
python generate_and_feed_kafka.py
```
4. Run the HLL .jar:

```bash
java -cp target/kafka-hyperloglog-mongodb-1.0-SNAPSHOT-jar-with-dependencies.jar com.example.KafkaHyperLogLogToMongoDB
```
#### The Java file is located in the kafka-hll dir, you can recompile it using:
```bash
mvn clean package
```

6. Use the webserver to view the aggregated results at http://localhost:3000/



## Description
-----------
This project processes a stream of data, estimating the number of unique DataSubjectIds seen by each viewer using the HyperLogLog algorithm. Results are stored in MongoDB and can be viewed via a UI.

## Components
-----------
Kafka: Message broker for streaming data.
MongoDB: Database for storing results.
Zookeeper: Kafka clusters management
Backend+Frontend: UI framework for displaying results.
Docker: Containerization platform.
