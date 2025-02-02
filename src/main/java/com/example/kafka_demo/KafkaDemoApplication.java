package com.example.kafka_demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaDemoApplication {



	public static void main(String[] args) throws InterruptedException {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaDemoApplication.class, args);
	//	SpringApplication.run(KafkaDemoApplication.class, args);


		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);
        for (int i = 0; i < 10; i++) {
			producer.sendMessage("Hello, Spring Kafka! " + i);
		}
	}


	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}

	@Bean
	public MessageListener messageListener() {
		return new MessageListener();
	}

	public static class MessageProducer {

		@Value(value = "${spring.kafka.topic-name}")
		private String topicName;

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		public void sendMessage(String msg) {
			System.out.println("Producing message -> " +  msg);
			kafkaTemplate.send(topicName, msg);
		}
	}

	public static class MessageListener {


		// public final CountDownLatch latch = new CountDownLatch(3);

		@KafkaListener(topics = "${spring.kafka.topic-name}", groupId = "${spring.kafka.consumer.group-id}")
		public void listen(String message) {
			System.out.println("Received Messasge in group : " + message);

			//latch.countDown();
		}
	}

}
