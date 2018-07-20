package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public class HBaseTable { 

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
}

