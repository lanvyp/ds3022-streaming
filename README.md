# Learn Kafka

Learn how to work with Apache Kafka.

## Docker Setup

After cloning this repository, change into this directory using the command line and run the following Docker command:

```
docker compose up -d
```
This will start a 3-node Kafka cluster with a web interface. Open a browser to http://127.0.0.1:8080/ to see the RedPanda Kafka interface. From here you can browse

- Cluster state
- Topics
- Consumer Groups
- Storage usage
- etc.

## Reaching the Broker

While three instances of Kafka are running, one is elected as the cluster controller. You can see this from the home page of the web UI above. 

To communicate with the cluster from scripts or the CLI in your local environment use this address:

```
localhost:19092
```

Internally the three brokers are seen within Docker as:
```
redpanda-0:9092
redpanda-1:9092
redpanda-2:9092
```

## Stopping the Cluster

From within the same directory run this command:

```
docker compose down
```

