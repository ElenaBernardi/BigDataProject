package app;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.*;

import scala.Tuple2;



public class App 
{
	public static void main( String[] args ) throws Exception
	{
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		HBaseTable hbase = new HBaseTable();
//		hbase.createTable();
//		Path pt=new Path("/input/dataset.csv");
//		Configuration configuration = new Configuration();
//		configuration.set("fs.default.name","hdfs://localhost:54310");
//		FileSystem fs = FileSystem.get(configuration);
//		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
//		try {
//			String line;
//			line=br.readLine();
//			int i=0;
//			while (line != null){
//				String[] s = line.split(",");
//				if (i>=100000 && i<200000) hbase.insertRecords(line);
//				System.out.println(s[ShazamCostants.GENRE]+" , "+s[ShazamCostants.SHAZAMID]);
//				i++;
////				hbase.insertRecords(line);
//				line = br.readLine();
//			}
//		} finally {
//			// you should close out the BufferedReader
//			br.close();
//		}
//		String c= "30978,04/12/2017 13:38:39,3501 20th Av, Valley AL 36854,Rogan@Pellentesque.org,TRAVSEA128F931546A,Pat Martino,El Hombre,Rap";
//		hbase.insertRecords(c);

		
		//##################################################
		String topic = "test";
		String group = "elena";
		String broker = "localhost:9092";

		
		Consumer c = new Consumer(broker, topic, group, hbase);
		Producer p = new Producer(broker);
	
//		p.send();
//		c.top10Track();
//		c.moreActiveUsers();
		c.findLocations();
	}
}
