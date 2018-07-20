package app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.soap.Text;

import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import app.ShazamCostants;
import org.apache.spark.api.java.Optional;

public class Consumer {

	private String topics;
	private JavaStreamingContext jssc;
	private SparkConf sparkConf;
	private static final Pattern COMMA = Pattern.compile(",");
	private HBaseTable hbase;

	Map<String, Object> kafkaParams = new HashMap<String, Object>();

	public Consumer(){}

	public Consumer(String broker, String topics, String group, HBaseTable hbase){
		kafkaParams.put("metadata.broker.list", "localhost:2181");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("group.id", group);
		//kafkaParams.put("auto.offset.reset","earliest");
		kafkaParams.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		this.topics = topics;
		this.hbase = hbase;
	}

	public JavaDStream<ConsumerRecord<String, String>> loadData(){
		this.sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		this.jssc = new JavaStreamingContext(sparkConf , Durations.seconds(2));
		this.jssc.checkpoint("/tmp");
		System.out.println("Initializing Shazam stream...");
		Set<String> topicsSet = new HashSet<String>(Arrays.asList(this.topics.split(",")));
		String[] topicsList = this.topics.split(",");
		for (String topic: topicsList) {
			topicsSet.add(topic);
		}
		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> stream =  KafkaUtils.createDirectStream(this.jssc,
				LocationStrategies.PreferConsistent(), 
				ConsumerStrategies.Subscribe(topicsSet, this.kafkaParams));

		return stream;
	}

	public void top10Track() throws InterruptedException {
		JavaDStream<ConsumerRecord<String, String>> shazamsStream = loadData();

		JavaDStream<String> shazamTrack = shazamsStream.
				map(shazam -> COMMA.split(shazam.value())).map(shazamArray -> shazamArray[ShazamCostants.TRACKNAME]);
		JavaPairDStream<String, Integer> trackMap = shazamTrack.mapToPair(track -> new Tuple2<>(track, 1))
				.reduceByKeyAndWindow((x,y) -> x+y, new Duration(2000));
		JavaPairDStream<Integer, String> change = trackMap.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
		JavaPairDStream<Integer, String> sortedTrack = change.transformToPair(trackRDD -> trackRDD.sortByKey(false));
		List<Tuple2<Integer, String>> top10 = new ArrayList<>();
		sortedTrack.foreachRDD(rdd -> {	List<Tuple2<Integer, String>> mostPopular = rdd.take(10); top10.addAll(mostPopular);});
		//stampa le track più popolari negli ultimi 2 secondi
		sortedTrack.print();
		//tiene traccia di tutti i dstream
		JavaPairDStream<Integer, String> solution = trackMap.updateStateByKey(COMPUTE_RUNNING_SUM).mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
				.transformToPair(trackRDD -> trackRDD.sortByKey(false));
		List<Tuple2<Integer, String>> top10Total = new ArrayList<>();

		solution.foreachRDD(rdd -> {List<Tuple2<Integer, String>> mostPopular = rdd.take(10); top10Total.addAll(mostPopular);});
		//salvataggio dei dati
		for(Tuple2<Integer, String> coppia :top10Total) {
			System.out.println("salvo i dati");
			hbase.insertRecordsTrack(coppia);}

		solution.print();

		jssc.start();
		jssc.awaitTermination();

	}

	private static Function2<List<Integer>, Optional<Integer>, Optional<Integer>>

	COMPUTE_RUNNING_SUM = (nums, current) -> {
		int sum = current.or(0);
		for (int i : nums) {
			sum += i;
		}
		return Optional.of(sum);
	};

	public void findLocations() throws InterruptedException {
		JavaDStream<ConsumerRecord<String, String>> shazamsStream = loadData();
		JavaDStream<String[]> shazamTrack = shazamsStream
				.map(shazam -> COMMA.split(shazam.value()));
		JavaPairDStream<String, String> couples = shazamTrack
				.mapToPair(shazamArray -> new Tuple2<>(shazamArray[ShazamCostants.TRACKNAME], shazamArray[ShazamCostants.TRACKID]));
		JavaPairDStream<Integer,Tuple2<String,String>> couplesLocation = couples.mapToPair(couple -> new Tuple2<>(couple, 1))
				.reduceByKeyAndWindow((x,y) -> x+y, new Duration(2000))
				.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
		// Then sort the track
		JavaPairDStream<Integer, Tuple2<String,String>> sortedTrack = couplesLocation.transformToPair(trackRDD -> trackRDD.sortByKey(false));
		// and return the 10 most populars
		List<Tuple2<Integer, Tuple2<String,String>>> top10 = new ArrayList<>();
		sortedTrack.foreachRDD(rdd -> {	List<Tuple2<Integer, Tuple2<String,String>>> mostPopular = rdd.take(10); top10.addAll(mostPopular);});
		//salvataggio dei dati
//		for(Tuple2<Integer, Tuple2<String, String>> coppia :top10) {
//			System.out.println("salvo i dati");
//			hbase.insertRecordsLocations(coppia);}
		sortedTrack.print();

		jssc.start();
		jssc.awaitTermination();
	}

	public void moreActiveUsers() throws InterruptedException {
		JavaDStream<ConsumerRecord<String, String>> shazamsStream = loadData();
		JavaDStream<String> users = shazamsStream.
				map(shazam -> COMMA.split(shazam.value())).map(shazamArray -> shazamArray[ShazamCostants.USER]);
		JavaPairDStream<String, Integer> userMap = users.mapToPair(track -> new Tuple2<>(track, 1))
				.reduceByKeyAndWindow((x,y) -> x+y, new Duration(2000));
		JavaPairDStream<Integer, String> solution = userMap.updateStateByKey(COMPUTE_RUNNING_SUM).mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
				.transformToPair(trackRDD -> trackRDD.sortByKey(false));
		List<Tuple2<Integer, String>> top10 = new ArrayList<>();

		solution.foreachRDD(rdd -> {List<Tuple2<Integer, String>> moreActive = rdd.take(10); top10.addAll(moreActive);});
		//salvataggio dei dati
		for(Tuple2<Integer, String> coppia :top10) {
			System.out.println("salvo i dati");
			hbase.insertRecordsUsers(coppia);}

		solution.print();
		jssc.start();
		jssc.awaitTermination();

	}
}
