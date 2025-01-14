package com.example.kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext //To have the correct broker address set on the Sender and Receiver beans during each test case, we need to use the @DirtiesContext on all test classes. The reason for this is that each test case contains its own embedded Kafka broker that will each be created on a new random port. 
//By rebuilding the application context, the beans will always be set with the current broker address.
public class SpringKafkaSenderTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaSenderTest.class);

	private static String SENDER_TOPIC = "Sender.t";

	@Autowired
	private Sender sender;
	
	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	private KafkaMessageListenerContainer<String, String> container;

	private BlockingQueue<ConsumerRecord<String, String>> records;

	@ClassRule // spring-kafka-test includes an EmbeddedKafkaBroker -this starts zookeeper and
				// kafka server instance on a random port before all the test cases are run and
				// stops the instances once the test cases are finished
	public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, SENDER_TOPIC); // Create embedded
																									// Kafka brokers.
	// Parameters:count the number of brokers.controlledShutdown passed into
	// TestUtils.createBrokerConfig.topics the topics to create (2 partitions per).

	@Before
	public void setUp() throws Exception {

		  // set up the Kafka consumer properties
		Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps("sender", "false",
				embeddedKafka.getEmbeddedKafka());
		// Parameters:group the group id.autoCommit the auto commit.embeddedKafka a
		// EmbeddedKafkaBroker instance.

		// create a Kafka consumer factory
		DefaultKafkaConsumerFactory<String, String> defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<String, String>(
				consumerProperties);
		
		// set the topic that needs to be consumed,which contains runtime properties
    	ContainerProperties containerProperties = new ContainerProperties(SENDER_TOPIC);
    	
    	// create a Kafka MessageListenerContainer 
       container = new KafkaMessageListenerContainer<>(defaultKafkaConsumerFactory, containerProperties);

    // create a thread safe queue to store the received message
		records = new LinkedBlockingQueue<>();

		 // setup a Kafka message listener
		container.setupMessageListener(new MessageListener<String, String>() {

			@Override
			public void onMessage(ConsumerRecord<String, String> record) {
				LOGGER.debug("Test listener recived message = '{}'", record.toString());
				records.add(record);

			}

		});

		 // start the container and underlying message listener,The listener is started by starting the container.
		container.start();

		// wait until the container has the required number of assigned partitions,to avoid that we send a message before the container has required the number of assigned partitions, we use the waitForAssignment() method
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());

	}

	@After
	public void tearDown() {

		// stop the container
		container.stop();
	}
	
	
	@Test
	public void testSend() throws InterruptedException {
		
		// send the message
		String greeting = "Hello Spring Kafka Sender!!";
		
		sender.send(greeting);
		
		 // check that the message was received
		ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
		
		// Hamcrest Matchers to check the value
		assertThat(received,hasValue(greeting));
		
		// AssertJ Condition to check the key
		assertThat(received).has(key(null));
	}

}
