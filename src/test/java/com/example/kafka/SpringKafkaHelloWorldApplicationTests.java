package com.example.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.kafka.consumer.Receiver;
import com.example.kafka.producer.Sender;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { SpringKafkaHelloWorldApplicationTests.HELLOWORLD_TOPIC },brokerProperties = {"listeners=PLAINTEXT://localhost:9092"," port=9092"})
public class SpringKafkaHelloWorldApplicationTests {
	
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;
	
	@Autowired //spring-kafka-test includes an EmbeddedKafkaBroker -this starts zookeeper and kafka server instance on a random port before all the test cases are run and stops the instances once the test cases are finished
    private EmbeddedKafkaBroker embeddedKafka;

	static final String HELLOWORLD_TOPIC = "helloworld.t";

	@Autowired
	private Receiver receiver;

	@Autowired
	private Sender sender;
	

	@Test
	public void testReceive() throws Exception {
		sender.send("Hello Spring Kafka!");
		
		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
		
	}
}


