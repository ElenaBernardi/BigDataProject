package simpleFilter;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
	
		

	private KafkaProducer<String, String> producer;
	private final Properties properties = new Properties();

	public SimpleProducer(String nome) {
		properties.put("bootstrap.servers", "10.11.1.101:9092");
		properties.put("client.id", nome);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		this.producer = new KafkaProducer<String,String>(properties);
	}

	public void invia(String messaggio, String topicName) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, messaggio);
		producer.send(record);
		
		
	}

	public void chiudi() {
		producer.close();
	}
	
	@Override
	public String toString() {
		return "SimpleProducer name: "+properties.getProperty("client.id");
		
	}
	
	
}
