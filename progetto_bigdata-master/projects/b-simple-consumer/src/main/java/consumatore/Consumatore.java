package consumatore;

import java.util.*;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
public class Consumatore {

	private String topic;
	private KafkaConsumer<String, String> consumer;


	public Consumatore(String zookeeper, String groupId, String topic) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
        props.put("bootstrap.servers", "10.11.1.101:9092");
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		this.topic = topic;
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<String, String>(props);
		this.consumer.subscribe(Collections.singletonList(this.topic));

	}

	public void riceviMessaggi() {
		try {
			while (true) { 
				// the parameter we pass, poll() is a timeout interval and controls how long
				// poll() will block if data is not available in the consumer buffer. 
				// it will wait for the specified number of milliseconds for data to arrive from the broker
				ConsumerRecords<String, String> records = consumer.poll(0); 
				// poll() returns a list of records. 
				// each record contains the topic and partition the record came from, the offset of the record within the partition,
				// the key and the value of the record.
				for (ConsumerRecord<String, String> record : records) 
				{System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
				}
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
		}finally {
			this.consumer.close();
		}
	}
}
