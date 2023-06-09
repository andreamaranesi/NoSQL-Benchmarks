package site.ycsb.db.voltdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import org.voltdb.client.ProcCallException;
import org.voltdb.VoltTable;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.sql.Timestamp;

public class VoltDBCustom extends DB {
	private VoltDBClient client;

	/**
	 * Constructor to initialize the client.
	 */
	public VoltDBCustom() {
		ClientConfig config = new ClientConfig();
		Client client = ClientFactory.createClient(config);
		this.client = new VoltDBClient(client);
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
		} catch (IOException e) {
			throw new DBException(e);
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
		throw new IllegalArgumentException("Scan isn't implemented");
	}

	/**
	 * This method performs the cleanup after operations are done.
	 */
	@Override
	public void cleanup() {
		client.close();
	}

	/**
	 * Inner class to encapsulate the client interactions.
	 */
	public class VoltDBClient {

		private final Client client;

		/**
		 * Constructor for the inner class.
		 * 
		 * @param client The client to interact with.
		 */
		public VoltDBClient(Client client) {
			this.client = client;
		}

		/**
		 * Method to establish a connection to the client.
		 * 
		 * @throws IOException if connection fails.
		 */
		public void connect() throws IOException {
			this.client.createConnection("localhost");
		}

		/**
		 * Helper method to derive the procedure name.
		 * 
		 * @param operation The operation to be performed.
		 * @param table     The table to perform the operation on.
		 * @return The derived procedure name.
		 */
		private String getProcedureName(String operation, String table) {
			return operation + table.substring(0, 1).toUpperCase() + table.substring(1);
		}

		/**
		 * Method to perform an insert operation.
		 * 
		 * @param table  The table to perform the operation on.
		 * @param key    The primay key of the new record
		 * @param values The values to insert.
		 * @throws ProcCallException
		 * @throws IOException
		 */
		public void insert(String table, String key, Map<String, String> values) throws ProcCallException, IOException {
			String procedure = getProcedureName("Insert", table);
			if (table.equals("users")) {
				this.client.callProcedure(procedure, key, values.get("field0"), values.get("field1"),
						values.get("field3"));
			} else if (table.equals("posts")) {
				this.client.callProcedure(procedure, key, values.get("user_id"), values.get("longContent"),
						values.get("field2"), values.get("field3"));
			} else if (table.equals("comments")) {
				this.client.callProcedure(procedure, key, values.get("post_id"),
						values.get("longContent"), values.get("field3"));
			}
		}

		/**
		 * Method to perform a read operation from a given table and key.
		 * 
		 * @param table The table to read from.
		 * @param key   The primary key of the record to read.
		 * @return A map containing the read values.
		 * @throws ProcCallException
		 * @throws IOException
		 */
		public Map<String, String> read(String table, String key) throws ProcCallException, IOException {
			String procedure = getProcedureName("Select", table);
			ClientResponse response = this.client.callProcedure(procedure, key);
			VoltTable resultsTable = response.getResults()[0];
			Map<String, String> result = new HashMap<>();
			while (resultsTable.advanceRow()) {
				for (int i = 0; i < resultsTable.getColumnCount(); i++) {
					result.put(resultsTable.getColumnName(i), resultsTable.getString(i));
				}
			}
			return result;
		}

		/**
		 * Method to perform an update operation on a given table and key with provided
		 * values.
		 * 
		 * @param table  The table to update.
		 * @param key    The primary key of the record to update.
		 * @param values The values to update with.
		 * @throws ProcCallException
		 * @throws IOException
		 */
		public void update(String table, String key, Map<String, String> values) throws ProcCallException, IOException {
			String procedure = getProcedureName("Update", table);
			if (table.equals("users")) {
				this.client.callProcedure(procedure, values.get("field0"), values.get("field1"), key);
			} else if (table.equals("posts")) {
				this.client.callProcedure(procedure, values.get("longContent"), values.get("field1"), key);
			} else if (table.equals("comments")) {
				this.client.callProcedure(procedure, values.get("longContent"), key);
			}
		}

		/**
		 * Method to delete a record in a given table with a specified key.
		 * 
		 * @param table The table to delete from.
		 * @param key   The primary key of the record to delete.
		 * @throws ProcCallException
		 * @throws IOException
		 */
		public void delete(String table, String key) throws ProcCallException, IOException {
			String procedure = getProcedureName("Delete", table);
			this.client.callProcedure(procedure, key);
		}

		/**
		 * Method to close the client connection.
		 */
		public void close() {
			try {
				this.client.close();
			} catch (InterruptedException e) {
				e.printStackTrace(); // log the exception
			}
		}

	}

}
