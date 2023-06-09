package site.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import java.util.concurrent.ThreadLocalRandom;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.Date;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class CassandraDBCustom extends DB {
	private CassandraClient client;

	// Constructor which instantiates the CassandraClient
	public CassandraDBCustom() {
		this.client = new CassandraClient();
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
		throw new IllegalArgumentException("Scan isn't implemented");
	}

	/**
	 * This method performs the cleanup after operations are done.
	 */
	@Override
	public void cleanup() {
		client.close();
	}

	/*
	 * Internal class to handle connection with the Cassandra database
	 */
	public class CassandraClient {

		private Cluster cluster;
		private Session session;
		private Properties keyTableMapping;
		private Map<String, PreparedStatement> insertStatements;
		private Map<String, PreparedStatement> updateStatements;
		private Map<String, PreparedStatement> deleteStatements;

		private File file;

		/**
		 * Method to connect to the Cassandra cluster
		 * 
		 */
		public void connect() {
			this.cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
			this.session = cluster.connect("social_media");
		}

		/**
		 * Method to perform an insert operation.
		 * 
		 * @param table  The table to perform the operation on.
		 * @param key    The primay key of the new record
		 * @param values The values to insert.
		 */
		public void insert(String table, String key, Map<String, String> values) {
			PreparedStatement statement;
			BoundStatement boundStatement;

			// Switch on the table name to handle specific insertions
			switch (table) {
			case "users":
				statement = session
						.prepare("INSERT INTO users (user_id, username, email, created_at) VALUES (?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				session.execute(
						boundStatement.bind(key, values.get("field0"), values.get("field1"), values.get("field2")));
				break;
			case "posts":
				statement = session.prepare(
						"INSERT INTO posts (post_id, user_id, content, platform, posted_time) VALUES (?,?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(key, values.get("user_id"), values.get("longContent"),
						values.get("field2"), values.get("field3")));

				break;
			case "comments":
				statement = session.prepare(
						"INSERT INTO comments (comment_id, post_id, content, commented_time) VALUES (?,?,?,?)");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(key, values.get("post_id"), values.get("longContent"),
						values.get("field3")));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}
		}

		/**
		 * Method to perform a read operation from a given table and key.
		 * 
		 * @param table The table to read from.
		 * @param key   The primary key of the record to read.
		 * @return A map containing the read values.
		 */
		public Map<String, String> read(String table, String key) {
			PreparedStatement statement;
			BoundStatement boundStatement;
			ResultSet resultSet;

			switch (table) {
			case "users":
				statement = session.prepare("SELECT * FROM users WHERE user_id = ?");
				boundStatement = new BoundStatement(statement);
				resultSet = session.execute(boundStatement.bind(key));
				break;
			case "posts":
				statement = session.prepare("SELECT * FROM posts WHERE post_id = ?");
				boundStatement = new BoundStatement(statement);
				resultSet = session.execute(boundStatement.bind(key));
				break;
			case "comments":
				statement = session.prepare("SELECT * FROM comments WHERE comment_id = ?");
				boundStatement = new BoundStatement(statement);
				resultSet = session.execute(boundStatement.bind(key));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}

			Row row = resultSet.one();
			if (row == null) {
				throw new IllegalArgumentException("Key " + key + " not found in table " + table);
			}

			Map<String, String> result = new HashMap<>();
			for (ColumnDefinitions.Definition definition : resultSet.getColumnDefinitions()) {
				String columnName = definition.getName();
				String value = row.getString(columnName);
				result.put(columnName, value);
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
		 */
		public void update(String table, String key, Map<String, String> values) {
			PreparedStatement statement;
			BoundStatement boundStatement;

			switch (table) {
			case "users":
				statement = session.prepare("UPDATE users SET username = ?, email = ? WHERE user_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(values.get("field0"), values.get("field1"), key));
				break;
			case "posts":
				statement = session.prepare("UPDATE posts SET content = ?, platform = ? WHERE post_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(values.get("longContent"), values.get("field1"), key));
				break;
			case "comments":
				statement = session.prepare("UPDATE comments SET content = ? WHERE comment_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(values.get("longContent"), key));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}
		}

		/**
		 * Method to delete a record in a given table with a specified key.
		 * 
		 * @param table The table to delete from.
		 * @param key   The primary key of the record to delete.
		 */
		public void delete(String table, String key) {
			PreparedStatement statement;
			BoundStatement boundStatement;

			switch (table) {
			case "users":
				statement = session.prepare("DELETE FROM users WHERE user_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(key));
				break;
			case "posts":
				statement = session.prepare("DELETE FROM posts WHERE post_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(key));
				break;
			case "comments":
				statement = session.prepare("DELETE FROM comments WHERE comment_id = ?");
				boundStatement = new BoundStatement(statement);
				session.execute(boundStatement.bind(key));
				break;
			default:
				throw new IllegalArgumentException("Invalid table " + table);
			}
		}

		/**
		 * Method to close the client connection.
		 */
		public void close() {
			session.close();
			cluster.close();
		}
	}
}
