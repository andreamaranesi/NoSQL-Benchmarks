package site.ycsb.db;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.*;

public class HBaseDBCustom extends DB {

	private HBaseClient client;

	/**
	 * Constructor which instantiates the HBaseClient
	 */
	public HBaseDBCustom() {
		this.client = new HBaseClient();
	}

	/**
	 * Method to initialize the client.
	 *
	 * @throws DBException
	 */
	@Override
	public void init() throws DBException {
		try {
			client.connect();
		} catch (Exception e) {
			throw new DBException("error occurred during init");
		}
	}

	/**
	 * This method reads the specific fields of a record in the database.
	 * 
	 * @param table  The name of the table
	 * @param key    The primary key to read on
	 * @param fields The list of fields to read, or null for all of them (default is
	 *               null)
	 * @param result A map of field/value pairs for the result
	 * @return The result of the operation.
	 */
	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		try {
			Map<String, String> record = client.read(table, key);
			result.putAll(StringByteIterator.getByteIteratorMap(record));
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	/**
	 * This method inserts a record into the database.
	 * 
	 * @param table  The name of the table
	 * @param key    The primary key of the record that will be insert
	 * @param values A map of field/value pairs to insert.
	 * @return The result of the operation.
	 */
	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		try {
			client.insert(table, key, StringByteIterator.getStringMap(values));
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	/**
	 * This method deletes a record from the database.
	 * 
	 * @param table The name of the table
	 * @param key   The primary key of the record that will be deleted
	 * @return The result of the operation.
	 */
	@Override
	public Status delete(String table, String key) {
		try {
			client.delete(table, key);
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	/**
	 * This method updates the values of a record in the database.
	 * 
	 * @param table  The name of the table
	 * @param key    The primary key of the record that will be updated
	 * @param values A map of field/value pairs to update in the record
	 * @return The result of the operation.
	 */
	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		try {
			client.update(table, key, StringByteIterator.getStringMap(values));
			return Status.OK;
		} catch (Exception e) {
			e.printStackTrace();
			return Status.ERROR;
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		throw new UnsupportedOperationException("Scan is not implemented.");
	}

	/**
	 * This method performs the cleanup after operations are done.
	 */
	@Override
	public void cleanup() {
		client.close();
	}

	/*
	 * Inner class to handle connection with the HBase database
	 */
	public class HBaseClient {

		private Connection connection;
		private Admin admin;

		/**
		 * Method to connect to the HBase cluster
		 */
		public void connect() throws IOException, DBException {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "hbase-docker");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			System.out.println("connecting...");
			connection = ConnectionFactory.createConnection(config);
			System.out.println("Connected");
			final TableName tName = TableName.valueOf("users");
			try (Admin admin = connection.getAdmin()) {
				if (!admin.tableExists(tName)) {
					throw new DBException("Table " + tName + " does not exists");
				}
			}
		}

		/**
		 * Method to perform an insert operation.
		 *
		 * @param table  The table to perform the operation on.
		 * @param key    The key of the new record
		 * @param values The values to insert.
		 */
		public void insert(String table, String key, Map<String, String> values) throws IOException {
			Table hTable = connection.getTable(TableName.valueOf(table));
			Put p = new Put(Bytes.toBytes(key)); // Convert key to bytes

			// Different columns are set based on the table being accessed
			switch (table) {
			case "users":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(values.get("field0")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(values.get("field1")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("created_at"), Bytes.toBytes(values.get("field2")));
				break;
			case "posts":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_id"), Bytes.toBytes(values.get("user_id")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(values.get("longContent")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("platform"), Bytes.toBytes(values.get("field2")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("posted_time"), Bytes.toBytes(values.get("field3")));
				break;
			case "comments":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("post_id"), Bytes.toBytes(values.get("post_id")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(values.get("longContent")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("commented_time"),
						Bytes.toBytes(values.get("field3")));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}

			hTable.put(p); // Put the data into the table
			hTable.close();
		}

		/**
		 * Method to perform a read operation from a given table and key.
		 *
		 * @param table The table to read from.
		 * @param key   The key of the record to read.
		 * @return A map containing the read values.
		 */
		public Map<String, String> read(String table, String key) throws IOException {
			Table hTable = connection.getTable(TableName.valueOf(table));
			Get g = new Get(Bytes.toBytes(key)); // Convert key to bytes

			Result result = hTable.get(g);

			Map<String, String> resultMap = new HashMap<>();

			// Different columns are read based on the table being accessed
			switch (table) {
			case "users":
				resultMap.put("username",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
				resultMap.put("email", Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
				resultMap.put("created_at",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("created_at"))));
				break;
			case "posts":
				resultMap.put("user_id",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("user_id"))));
				resultMap.put("content",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("content"))));
				resultMap.put("platform",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("platform"))));
				resultMap.put("posted_time",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("posted_time"))));
				break;
			case "comments":
				resultMap.put("post_id",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("post_id"))));
				resultMap.put("content",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("content"))));
				resultMap.put("commented_time",
						Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("commented_time"))));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}

			hTable.close();
			return resultMap;
		}

		/**
		 * Method to perform an update operation on a given table and key.
		 *
		 * @param table  The table to perform the operation on.
		 * @param key    The key of the record to update.
		 * @param values The values to update.
		 */
		public void update(String table, String key, Map<String, String> values) throws IOException {
			Table hTable = connection.getTable(TableName.valueOf(table));
			Put p = new Put(Bytes.toBytes(key)); // Convert key to bytes

			// Different columns are set based on the table being accessed
			switch (table) {
			case "users":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(values.get("field0")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(values.get("field1")));
				break;
			case "posts":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(values.get("longContent")));
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("platform"), Bytes.toBytes(values.get("field1")));
				break;
			case "comments":
				p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(values.get("longContent")));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}

			hTable.put(p); // Update the data
			hTable.close();
		}

		/**
		 * Method to perform a delete operation on a given table and key.
		 *
		 * @param table The table to delete from.
		 * @param key   The key of the record to delete.
		 */
		public void delete(String table, String key) throws IOException {
			Table hTable = connection.getTable(TableName.valueOf(table));
			Delete d = new Delete(Bytes.toBytes(key)); // Convert key to bytes
			hTable.delete(d); // Delete the data from the table
			hTable.close();
		}

		/**
		 * Close the connection to HBase
		 */
		public void close() {
			try {
				this.connection.close();
			} catch (IOException e) {
				e.printStackTrace(); // log the exception
			}
		}
	}
}
