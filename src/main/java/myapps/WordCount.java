/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myapps;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurence histogram, write the continuous updated histogram
 * into a topic "streams-wordcount-output" where each record is an updated count of a single word.
 */
public class WordCount {

  public static void resetOffsetsForTopics(List<TopicPartition> topicPartitions) {
    Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "streams-wordcount");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Consumer<Long, String> consumer = new KafkaConsumer<>(props);

    topicPartitions = topicPartitions.stream()
        .filter(topicPartition -> topicPartition.topic().contains("streams"))
        .collect(Collectors.toList());

    consumer.assign(topicPartitions);

    // It was throwing an exception without this, but now it isn't. I don't understand
    // Guessing consumer.position does the same thing
    // https://stackoverflow.com/questions/41008610/kafkaconsumer-0-10-java-api-error-message-no-current-assignment-for-partition/41010594
    // while (consumer.assignment().size() == 0) {
    //   consumer.poll(Duration.ofMillis(10000));
    // }

    TopicPartition example = new TopicPartition("streams-plaintext-input", 0);
    System.out.println("existing offset: " + consumer.position(example));
    consumer.seekToBeginning(topicPartitions);

    // consumer.seekToBeginning is a lazy operation, calling consumer.position or consumer.poll will start it
    System.out.println("curr offset: " + consumer.position(example));
    consumer.commitSync();
    consumer.close();
  }

  public static void createTopic(AdminClient adminClient) {
    short replicationFactor = 1;
    Map<String, String> configs = new HashMap<>();
    configs.put("cleanup.policy", "delete");
    NewTopic newTopic = new NewTopic("adminclient-topic", 1, replicationFactor).configs(configs);
    adminClient.createTopics(Arrays.asList(newTopic));
  }

  public static List<String> listTopics(AdminClient adminClient) {
    try {
      List<String> topicNames = adminClient.listTopics().listings()
          .thenApply(topicListings -> {
            List<String> mappedList = new ArrayList<>();

            for (TopicListing topic : topicListings) {
              System.out.println(topic);
              mappedList.add(topic.name());
            }

            return mappedList;
          }).get();

      return topicNames;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    return Collections.emptyList();
  }

  public static List<TopicPartition> listTopicPartition(AdminClient adminClient, List<String> topicNames) {
    try {
      List<TopicPartition> topicPartitionList = adminClient.describeTopics(topicNames).all()
          .thenApply(stringTopicDescriptionMap -> {
            System.out.println(stringTopicDescriptionMap);
            List<TopicPartition> mappedList = new ArrayList<>();

            for (String topicName : stringTopicDescriptionMap.keySet()) {
              List<TopicPartitionInfo> partitionInfo = stringTopicDescriptionMap.get(topicName).partitions();
              partitionInfo.forEach(topicPartitionInfo -> mappedList.add(new TopicPartition(topicName, topicPartitionInfo.partition())));
            }

            return mappedList;
          }).get();

      return topicPartitionList;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    return Collections.emptyList();
  }

  // If you restart immediately after stopping, the existing member of the consumer group will not have enough time to be removed
  // That will cause an exception when calling commitSync. This stupid code will block until we see there are no more members
  public static void blockUntilNoMembers(AdminClient adminClient) {
    try {
      boolean hasMembers = true;
      while (hasMembers) {
        ConsumerGroupDescription description = adminClient.describeConsumerGroups(Arrays.asList("streams-wordcount"))
            .describedGroups()
            .get("streams-wordcount")
            .get();

        hasMembers = description.members().size() != 0;
        Thread.sleep(1000);
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  public static void listConsumerGroups(AdminClient adminClient) {
    adminClient.listConsumerGroups().all().whenComplete((consumerGroupListings, throwable) -> {
      for (ConsumerGroupListing consumer : consumerGroupListings) {
        System.out.println(consumer);
      }
    });
  }

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    AdminClient adminClient = KafkaAdminClient.create(props);

    createTopic(adminClient);
    List<String> topicNames = listTopics(adminClient);
    List<TopicPartition> topicPartitions = listTopicPartition(adminClient, topicNames);
    blockUntilNoMembers(adminClient);
    resetOffsetsForTopics(topicPartitions);
    listConsumerGroups(adminClient);

    final StreamsBuilder builder = new StreamsBuilder();

    builder.<String, String>stream("streams-plaintext-input")
        .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .peek((key, value) -> System.out.println(key + " " + value))
        .groupBy((key, value) -> value)
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
        .toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }
}
