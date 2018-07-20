package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Producer {

	private	Properties properties = new Properties();
	private KafkaProducer<String, String> producer;

	public Producer(String broker){
		// Set properties used to configure the producer
		// Set the brokers (bootstrap servers)
		properties.setProperty("bootstrap.servers", broker);
		// Set how to serialize key/value pairs
		properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String,String>(properties);
	}

	public void send() throws IOException{
		Path pt=new Path("/input/dataset.csv");
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name","hdfs://localhost:54310");
		FileSystem fs = FileSystem.get(configuration);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		try {
			String line;
			line=br.readLine();
			System.out.println("############# inizio invio ###############");
			while (line != null){
				producer.send(new ProducerRecord<String, String>("test", line));
				line = br.readLine();
			}
			System.out.println("############## fine invio ################");
		} finally {
			// you should close out the BufferedReader
			br.close();
		}
	}
}
