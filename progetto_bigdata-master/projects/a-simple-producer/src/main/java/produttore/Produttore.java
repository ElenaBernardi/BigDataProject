package produttore;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Produttore {
	private KafkaProducer<String, String> producer;
	private final Properties properties = new Properties();

	public Produttore() {

		properties.put("bootstrap.servers", "10.11.1.101:9092");
		properties.put("key.serializer",
				  "org.apache.kafka.common.serialization.StringSerializer"); 
		properties.put("value.serializer",
				  "org.apache.kafka.common.serialization.StringSerializer");

		this.producer = new KafkaProducer<String,String>(properties);

	}

	public void invia(String messaggio,String topicName) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messaggio);
		producer.send(record);
	}

	public void chiudi() {
		producer.close();
	}
}