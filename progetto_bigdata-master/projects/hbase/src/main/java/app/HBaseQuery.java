package app;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public class HBaseQuery { 
	private static final String TABS = "\t\t\t";

	public static void main( String[] args ) throws Exception
	{
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		//		App.getOneRecord("shazam","1");
		String table = "shazam";
		int choice = 0;
		while (choice != 6) {
			printMenu();
			choice = getChoice();

			switch (choice) {
			case 1:
				getGenre(table);
				break;
			case 2:
				getTrackOfGenre(table);
				break;
			case 3:
				top10Artists(table);
				break;
			case 6:
				System.out.println("Bye.");
				break;
			case -1:
				System.out.println("Please enter a positive number.");
				break;
			default:
				System.out.println("Invalid choice.");
				break;
			}
		}
	}

	private static void printMenu() {
		System.out.println();
		System.out.println("Scans:");
		System.out.println("  1. Generi");
		System.out.println("  2. Lista canzoni per generi");
		System.out.println("  3. Artisti");
		System.out.println("  6. Quit");
		System.out.println();
	}

	private static int getChoice() throws IOException {
		System.out.print("Choice? ");
		String line = readInputLine();
		int choice;
		try {
			choice = Integer.parseInt(line);
		} catch (NumberFormatException ex) {
			choice = -1;
		}
		return choice;
	}

	private static String readInputLine() throws IOException {
		Console console = System.console();
		String line;
		if (console == null) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			line = reader.readLine();
		} else {
			line = console.readLine();
		}
		return line;
	}
	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeper.quorum", "localhost");
		conf.set("hbase.zookeper.property.clientPort", "2181");
	}

	public void createTable() {

		Connection connection = null;
		Admin admin = null;

		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();

			String tableName = "shazam";
			System.out.println("creating table...");
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("sono dentro");
				HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
				hbaseTable.addFamily(new HColumnDescriptor("shazamId"));
				hbaseTable.addFamily(new HColumnDescriptor("shazam_info"));
				hbaseTable.addFamily(new HColumnDescriptor("user"));
				hbaseTable.addFamily(new HColumnDescriptor("track_info"));
				admin.createTable(hbaseTable);
				System.out.println("done!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				System.out.println("°°°°°°°°°°°°°° ERRORE °°°°°°°°°°°°");
				e2.printStackTrace();
			}
		}
	}

	public void insertRecordsTrack(Tuple2<Integer,String> coppia) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		String tableName = "shazam";

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			config.set(TableInputFormat.INPUT_TABLE, "shazam");
			Put put = new Put(Bytes.toBytes("row "+coppia._2));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("frequence"), Bytes.toBytes(""+coppia._1()+""));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("name"), Bytes.toBytes(coppia._2()));

			table.put(put); 
			System.out.println("inserito");

		} catch (Exception e) {
			System.out.println("############ ERRORE ################");
			e.printStackTrace();

		}
	}

	public void insertRecordsUsers(Tuple2<Integer,String> coppia) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		String tableName = "shazam";

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			config.set(TableInputFormat.INPUT_TABLE, "shazam");
			Put put = new Put(Bytes.toBytes("row "+coppia._2));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("frequence"), Bytes.toBytes(""+coppia._1()+""));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("name"), Bytes.toBytes(coppia._2()));

			table.put(put); 
			System.out.println("inserito");

		} catch (Exception e) {
			System.out.println("############ ERRORE ################");
			e.printStackTrace();

		}
	}

	public void insertRecordsLocations(Tuple2<Integer, Tuple2<String,String>> coppia) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		String tableName = "shazam";

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			config.set(TableInputFormat.INPUT_TABLE, "shazam");
			Put put = new Put(Bytes.toBytes("row "+coppia._2._2()));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("frequency"), Bytes.toBytes(""+coppia._1()+""));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("location"), Bytes.toBytes(coppia._2()._1()));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("name"), Bytes.toBytes(coppia._2()._2));

			table.put(put); 
			System.out.println("inserito");

		} catch (Exception e) {
			System.out.println("############ ERRORE ################");
			e.printStackTrace();

		}
	}

	public void insertRecords(String line) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		String tableName = "shazam";

		Connection connection = null;
		Table table = null;

		String[] array = line.split(",");

		try {

			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(tableName));
			config.set(TableInputFormat.INPUT_TABLE, "shazam");
			Put put = new Put(Bytes.toBytes(array[ShazamCostants.SHAZAMID]));
			put.addColumn(Bytes.toBytes("shazamId"), Bytes.toBytes("id"), Bytes.toBytes(""+array[ShazamCostants.SHAZAMID]));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("time"), Bytes.toBytes(""+array[ShazamCostants.TIME]));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("address"), Bytes.toBytes(""+array[ShazamCostants.ADDRESS]));
			put.addColumn(Bytes.toBytes("shazam_info"), Bytes.toBytes("city"), Bytes.toBytes(""+array[ShazamCostants.CITY]));
			put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes(""+array[ShazamCostants.USER]));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("trackId"), Bytes.toBytes(""+array[ShazamCostants.TRACKID]));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("artistName"), Bytes.toBytes(""+array[ShazamCostants.ARTIST]));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("name"), Bytes.toBytes(""+array[ShazamCostants.TRACKNAME]));
			put.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("genre"), Bytes.toBytes(""+array[ShazamCostants.GENRE]));

			table.put(put); 
			System.out.println("inserito");

		} catch (Exception e) {
			System.out.println("############ ERRORE ################");
			e.printStackTrace();

		}
	}

	public static Set<String> getGenre(String tableName) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(tableName));			 
		// Instantiating the Scan class
		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("genre"));
		//		scan.setFilter(new PageFilter(3));
		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);
		Set<String> set = new HashSet<String>();
		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			byte [] col = result.getValue(Bytes.toBytes("track_info"),Bytes.toBytes("genre"));                
			String s = Bytes.toString(col);
			set.add(s);
		}
		for(String g : set) {
			System.out.println("Genere: "+ g);
		}
		//closing the scanner
		scanner.close();
		return set;
	}

	public static void getTrackOfGenre(String tableName) throws IOException {
		Set<String> setGenre = getGenre(tableName);
		for(String genre : setGenre) {
			System.out.println("\n ->Genere = "+genre+ "<- °°°Lista canzoni con più Shazam°°° : ");
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "127.0.0.1");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf(tableName));			 
			// Instantiating the Scan class
			Scan scan = new Scan();
			Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(genre)));
			scan.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("genre"));
			scan.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("name"));
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			List<String> list = new ArrayList();

			for (Result result = scanner.next(); result != null; result = scanner.next()) {

				List<Cell> cells = result.listCells();
				for(Cell cell : cells) {
					String s = Bytes.toString(CellUtil.cloneRow(cell));
					list.add(s);
				}	
			}
			Map<String,Integer> map = new HashMap<String,Integer>();
			for(String g : list) {
				String key = getOneRecord(tableName, g);
				if(map.get(key) == null) {
					map.put(key, 1);
				}else {
					Integer sum = map.get(key);
					sum = sum+1;
					map.put(key, sum);
				}
			}
			order(map);

		}
	}

	public static String getOneRecord (String tableName, String rowKey) throws IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(tableName));			 
		Get get = new Get(rowKey.getBytes());
		//		get.addFamily("shazam_info".getBytes());
		get.addColumn("track_info".getBytes(), "name".getBytes());
		Result rs = table.get(get);
		String key=null;
		for(KeyValue kv : rs.raw()){
			key = new String(kv.getValue());
		}
		return key;
	}

	public static void order(Map<String,Integer> map) {

		List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();

		for(String chiave : map.keySet()){
			list.add(new Tuple2<String,Integer>(chiave, map.get(chiave)));
		}
		//ordino lista 
		list.sort((a,b)-> b._2().compareTo(a._2()));

		//prendo i primi 10 valori
		int i=0;
		List<Tuple2> tmp = new ArrayList<Tuple2>();
		for(Tuple2 ow : list){
			if(i == 10) break;
			tmp.add(ow);
			System.out.println("Frequenza: "+ow._2+"\t\t\tTraccia : " +ow._1);
			i=i+1;
		}
	}
	
	public static void orderArtists(Map<String,Integer> map) {

		List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();

		for(String chiave : map.keySet()){
			list.add(new Tuple2<String,Integer>(chiave, map.get(chiave)));
		}
		//ordino lista 
		list.sort((a,b)-> b._2().compareTo(a._2()));

		//prendo i primi 10 valori
		int i=0;
		List<Tuple2> tmp = new ArrayList<Tuple2>();
		for(Tuple2 ow : list){
			if(i == 10) break;
			tmp.add(ow);
			System.out.println("Frequenza: "+ow._2+"\t\t\tArtista : " +ow._1);
			i=i+1;
		}
	}
	public static Set<String> artist(String tableName) throws IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(tableName));			 
		// Instantiating the Scan class
		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(Bytes.toBytes("track_info"), Bytes.toBytes("artistName"));
		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);
		Set<String> set = new HashSet<String>();
		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next()) {
			byte [] col = result.getValue(Bytes.toBytes("track_info"),Bytes.toBytes("artistName"));                
			String s = Bytes.toString(col);
			set.add(s);
		}
		//closing the scanner
		scanner.close();
		return set;
	}

	public static void top10Artists(String tableName) throws IOException {
		Set<String> setArtist = artist(tableName);
		System.out.println(setArtist.size());
		Map<String,Integer> map = new HashMap<>();
		for(String artist : setArtist) {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "127.0.0.1");
			config.set("hbase.zookeeper.property.clientPort", "2181");

			Connection connection = ConnectionFactory.createConnection(config);
			Table table = connection.getTable(TableName.valueOf(tableName));			 
			// Instantiating the Scan class
			Scan scan = new Scan();
			Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(artist)));
			scan.setFilter(filter);
		
			ResultScanner scanner = table.getScanner(scan);
			Integer i = 0;
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				i = i+1;	
			}
			map.put(artist, i);
		}
		orderArtists(map);
	}
	
}

