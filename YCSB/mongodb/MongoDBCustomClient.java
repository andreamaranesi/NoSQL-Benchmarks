package site.ycsb.db;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.io.IOException;
import java.util.Arrays;

public class MongoDBCustomClient extends DB {
	private MongoDBClient client;

	/**
	 * Constructor to initialize the client.
	 */
	public MongoDBCustomClient() {
		this.client = new MongoDBClient();
	}

	/**
	 * Method to initialize the client.
	 * 
	 * @throws DBException
	 */
	@Override
	public void init() throws DBException {
		client.connect();
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

	/**
	 * Inner class to encapsulate the client interactions.
	 */
	public class MongoDBClient {
		private MongoClient mongoClient;
		private MongoDatabase db;

		/**
		 * Method to establish a connection to the client.
		 * 
		 */
		public void connect() {
			MongoCredential credential = MongoCredential.createCredential("root", "admin", "example".toCharArray());

			mongoClient = MongoClients.create(MongoClientSettings.builder()
					.applyToClusterSettings(
							builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27017))))
					.credential(credential).build());

			db = mongoClient.getDatabase("social_media");
		}

		/**
		 * Method to perform an insert operation.
		 * 
		 * @param table  The collection to perform the operation on.
		 * @param key    The _id of the new record
		 * @param values The values to insert.
		 */
		public void insert(String table, String key, Map<String, String> values) {
			MongoCollection<Document> collection = db.getCollection(table);

			Document doc = new Document("_id", key);
			if (table.equals("users")) {
				doc.append("username", values.get("field0")).append("email", values.get("field1")).append("created_at",
						values.get("field2"));
			} else if (table.equals("posts")) {
				doc.append("user_id", values.get("user_id")).append("content", values.get("longContent"))
						.append("platform", values.get("field1")).append("posted_time", values.get("field2"));
			} else if (table.equals("comments")) {
				doc.append("post_id", values.get("post_id")).append("content", values.get("longContent"))
						.append("commented_time", values.get("field3"));
			}

			collection.insertOne(doc);
		}

		/**
		 * Method to perform a read operation from a given table and key.
		 * 
		 * @param table The collection to read from.
		 * @param key   The _id of the record to read.
		 * @return A map containing the read values.
		 */
		public Map<String, String> read(String table, String key) {
			MongoCollection<Document> collection = db.getCollection(table);

			Document doc = collection.find(Filters.eq("_id", key)).first();
			if (doc == null) {
				throw new IllegalArgumentException("No document found with key: " + key);
			}

			Map<String, String> result = new HashMap<>();
			for (Map.Entry<String, Object> entry : doc.entrySet()) {
				result.put(entry.getKey(), entry.getValue().toString());
			}

			return result;
		}

		/**
		 * Method to perform an update operation on a given table and key with provided
		 * values.
		 * 
		 * @param table  The collection to update.
		 * @param key    The _id of the record to update.
		 * @param values The values to update with.
		 */
		public void update(String table, String key, Map<String, String> values) {
			MongoCollection<Document> collection = db.getCollection(table);

			Document updateDoc = new Document();
			if (table.equals("users")) {
				updateDoc.append("username", values.get("field0")).append("email", values.get("field1"));
			} else if (table.equals("posts")) {
				updateDoc.append("content", values.get("longContent")).append("platform", values.get("field1"));
			} else if (table.equals("comments")) {
				updateDoc.append("content", values.get("longContent"));
			}

			UpdateResult result = collection.updateOne(Filters.eq("_id", key), new Document("$set", updateDoc));
			if (result.getModifiedCount() == 0) {
				throw new IllegalArgumentException("No document found with key: " + key);
			}
		}

		/**
		 * Method to delete a record in a given table with a specified key.
		 * 
		 * @param table The table to delete from.
		 * @param key   The _id of the record to delete.
		 */
		public void delete(String table, String key) {
			MongoCollection<Document> collection = db.getCollection(table);

			DeleteResult result = collection.deleteOne(Filters.eq("_id", key));
			if (result.getDeletedCount() == 0) {
				throw new IllegalArgumentException("No document found with key: " + key);
			}
		}

		/**
		 * Method to close the client connection.
		 */
		public void close() {
			mongoClient.close();
		}
	}
}
