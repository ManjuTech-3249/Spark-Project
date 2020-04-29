package com.npntraining

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Kafkaproducer {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);

    val input: Scanner = new Scanner(System.in);
    val topicName: String = "simple-producer-consumer";
    val key: String = "Key1";

    val props: Properties = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer");

    val producer: Producer[String, String] = new KafkaProducer[String, String](props);

    var message: String = null;
    do {
      message = input.nextLine();
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, message);
      producer.send(record);
    } while (!message.equalsIgnoreCase("quit"));

    println("Simple Producer Completed.");
  }
}