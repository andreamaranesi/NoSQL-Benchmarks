package site.ycsb;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.generator.CounterGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianGenerator;
import site.ycsb.generator.NumberGenerator;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;

public class MultiTableWorkload extends Workload {

	// Array of key sequences for each table.
	private CounterGenerator[] keySequences;
	// Array of generators for each table.
	private NumberGenerator[] generators; // can be Zipfian or Uniform

	// Define table names and initialize their properties.
	private static final String[] TABLE_NAMES = { "users", "comments", "posts" };
	// Probabilities of selecting each table.
	private double[] TABLE_PROBABILITIES;
	// Percentage of records to insert into each table.
	private double[] TABLE_INSERT_PERCENTAGE;
	// The number of records in each table.
	private static final int[] TABLE_RECORDS = new int[TABLE_NAMES.length];

	// Define field properties.
	private int fieldCount = 10; // Number of fields in each record.
	private long fieldLength = 100; // Length of each field.
	// Generator for field lengths.
	// It generates a length between 1 and fieldLength
	private UniformLongGenerator fieldLengthGenerator;

	// Define Max and Min characters for a single longer field
	private long minContentChars = 500;
	private long maxContentChars = 1000;
	// It generates a length between minContentChars and maxContentChars
	private UniformLongGenerator longContentFieldLengthGenerator;

	// Flags to indicate whether to read/write all fields.
	private boolean readAllFields = true;
	private boolean writeAllFields = true;

	// Operation proportions.
	private double readProportion;
	private double updateProportion;
	private double insertProportion;
	private double deleteProportion;

	// Index of the current table and the number of records already inserted.
	private AtomicInteger currentTableIndex = new AtomicInteger(0);
	private AtomicInteger currentRecordCount = new AtomicInteger(0);

	// Prefixes for keys.
	private String prefix;
	private String prefix_run_insert; // Prefix for insert operations during "run" benchmark

	// Counter generators for insert sequences.
	private CounterGenerator[] insertSequences;

	// Counters for CRUD operations
	private int operationCount = 1000;
	private HashMap<String, AtomicInteger> operationCounters;

	// Added HashMap for keeping track of deleted keys.
	private Set<String> deletedKeys = new HashSet<>();

	/**
	 * This method is called once to set up the workload's state. It initializes
	 * generators, tables, operation counts, etc. based on the properties passed in
	 * using the file workloads/workload*
	 * 
	 * @param p
	 * @throws WorkloadException
	 */
	@Override
	public void init(Properties p) throws WorkloadException {

		super.init(p);

		// Parse properties from the given Properties object.

		int totalRecordCount = Integer.parseInt(p.getProperty("recordcount", "1000"));

		prefix = p.getProperty("prefix", "user");
		prefix_run_insert = p.getProperty("prefix_run_insert", "");

		String requestDistribution = p.getProperty("requestdistribution", "zipfian");

		System.out.println("requested distribution:" + requestDistribution);

		String[] tableProbabilitiesStr = p.getProperty("tableprobabilities", "0.3,0.4,0.3").split(",");
		TABLE_PROBABILITIES = new double[tableProbabilitiesStr.length];
		for (int i = 0; i < tableProbabilitiesStr.length; i++) {
			TABLE_PROBABILITIES[i] = Double.parseDouble(tableProbabilitiesStr[i]);
		}

		String[] tableInsertPercentagesStr = p.getProperty("tableinsertpercentage", "0.3,0.4,0.3").split(",");
		TABLE_INSERT_PERCENTAGE = new double[tableInsertPercentagesStr.length];
		for (int i = 0; i < tableInsertPercentagesStr.length; i++) {
			TABLE_INSERT_PERCENTAGE[i] = Double.parseDouble(tableInsertPercentagesStr[i]);
		}

		TABLE_RECORDS[0] = (int) (totalRecordCount * TABLE_INSERT_PERCENTAGE[0]); // users
		TABLE_RECORDS[1] = (int) (totalRecordCount * TABLE_INSERT_PERCENTAGE[1]); // comments
		TABLE_RECORDS[2] = totalRecordCount - TABLE_RECORDS[0] - TABLE_RECORDS[1]; // posts

		// Initialize key sequences, generators, and insert sequences for each
		// table.

		keySequences = new CounterGenerator[TABLE_NAMES.length];
		generators = new NumberGenerator[TABLE_NAMES.length];

		// Initialize generators based on the property.
		for (int i = 0; i < TABLE_NAMES.length; i++) {
			keySequences[i] = new CounterGenerator(0);
			if ("zipfian".equals(requestDistribution)) {
				generators[i] = new ZipfianGenerator(TABLE_RECORDS[i] - 1);
			} else if ("uniform".equals(requestDistribution)) {
				generators[i] = new UniformLongGenerator(0, TABLE_RECORDS[i] - 1);
			} else {
				throw new WorkloadException(requestDistribution + " is not supported");
			}
		}

		insertSequences = new CounterGenerator[TABLE_NAMES.length];
		for (int i = 0; i < TABLE_NAMES.length; i++) {
			insertSequences[i] = new CounterGenerator(TABLE_RECORDS[i]); // Starts from TABLE_RECORDS
		}

		// Parse field properties from the given Properties object.

		fieldCount = Integer.parseInt(p.getProperty("fieldcount", Integer.toString(fieldCount)));
		fieldLength = Long.parseLong(p.getProperty("fieldlength", Long.toString(fieldLength)));
		fieldLengthGenerator = new UniformLongGenerator(1, fieldLength);

		minContentChars = Long.parseLong(p.getProperty("mincontentchars", Long.toString(minContentChars)));
		maxContentChars = Long.parseLong(p.getProperty("maxcontentchars", Long.toString(maxContentChars)));
		longContentFieldLengthGenerator = new UniformLongGenerator(minContentChars, maxContentChars);

		// Parse operation proportions from the given Properties object.

		readProportion = Double.parseDouble(p.getProperty("readproportion", "0.5"));
		updateProportion = Double.parseDouble(p.getProperty("updateproportion", "0.5"));
		insertProportion = Double.parseDouble(p.getProperty("insertproportion", "0"));
		deleteProportion = Double.parseDouble(p.getProperty("deleteproportion", "0"));

		System.out.println("readProportion" + readProportion);
		System.out.println("updateProportion:" + updateProportion);
		System.out.println("insertProportion:" + insertProportion);
		System.out.println("deleteProportion:" + deleteProportion);

		// Parse operation counts from the given Properties object and initialize
		// AtomicIntegers.

		operationCount = Integer.parseInt(p.getProperty("operationcount", Integer.toString(operationCount)));

		operationCounters = new HashMap<>();
		operationCounters.put("read", new AtomicInteger((int) (operationCount * readProportion)));
		operationCounters.put("update", new AtomicInteger((int) (operationCount * updateProportion)));
		operationCounters.put("insert", new AtomicInteger((int) (operationCount * insertProportion)));
		operationCounters.put("delete", new AtomicInteger((int) (operationCount * deleteProportion)));

		// System.out.println("prova__1");
	}

	/**
	 * This method generates the next key for the given table. If incrementSequence
	 * is true, the next key in the sequence is generated; otherwise, a key based on
	 * the Zipfian distribution is generated.
	 * 
	 * @param tableIndex
	 * @param incrementSequence
	 * @return the key, which is the prefix concatenated with the generated value.
	 */
	private String nextKey(int tableIndex, boolean incrementSequence) {

		Long nextValue = incrementSequence ? keySequences[tableIndex].nextValue()
				: generators[tableIndex].nextValue().longValue();
		return prefix + nextValue;
	}

	/**
	 * This method generates field values for a record. It creates a map with
	 * `fieldCount` number of fields, each containing a random string of length
	 * determined by `fieldLengthGenerator`. Additionally, it adds a field named
	 * "longContent" that contains a random string of length between
	 * `minContentChars` and `maxContentChars`.
	 *
	 * @return the HashMap containing field values.
	 */
	private HashMap<String, ByteIterator> generateValues() {

		// Create a new HashMap to store the field values.
		HashMap<String, ByteIterator> values = new HashMap<>();
		for (int i = 0; i < fieldCount; i++) {
			// Generate a random string for each field and put it into the HashMap.
			long fieldLen = fieldLengthGenerator.nextValue();
			String fieldValue = getRandomString(fieldLen);
			values.put("field" + i, new StringByteIterator(fieldValue));
		}

		// Generate a random string for the longContent field and put it into the
		// HashMap.
		long longContentFieldLen = longContentFieldLengthGenerator.nextValue();
		String longContentFieldValue = getRandomString(longContentFieldLen);
		values.put("longContent", new StringByteIterator(longContentFieldValue));

		return values;
	}

	/**
	 * This method generates a random alphanumeric string of length n.
	 * 
	 * @param n
	 * @return the generated random string.
	 */
	private String getRandomString(long n) {
		// Define the character set for the random string.

		String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789" + "abcdefghijklmnopqrstuvxyz";

		// Initialize a StringBuilder to build the random string.

		StringBuilder sb = new StringBuilder();

		for (long i = 0; i < n; i++) {
			int index = (int) (AlphaNumericString.length() * Math.random());
			sb.append(AlphaNumericString.charAt(index));
		}

		return sb.toString();
	}

	/**
	 * This method is responsible for executing an "insert" operation. It generates
	 * the field values, selects the table, creates a new record, and inserts it
	 * into the selected table.
	 * 
	 * @param db
	 * @param threadstate
	 * @return the result of the insert operation.
	 */
	public boolean doInsert(DB db, Object threadstate) {
		// Generate field values
		HashMap<String, ByteIterator> values = generateValues();

		int currentIndex = currentTableIndex.get();
		int currentCount = currentRecordCount.get();

		// If the current record count reaches the limit for the current table, move to
		// the next table and reset the record count.
		if (currentCount >= TABLE_RECORDS[currentIndex]) {
			System.out.println("table=" + TABLE_NAMES[currentIndex] + ";record uploaded=" + currentRecordCount);
			if (currentTableIndex.compareAndSet(currentIndex, currentIndex + 1)) {
				// If the currentTableIndex is incremented successfully, reset the
				// currentRecordCount
				currentIndex++;
				currentRecordCount.set(0);
			}

		}

		// Due to the specified percentages for each table, the calculated number of
		// records for a given table may not account for all possible rows, especially
		// when the total record count is not perfectly divisible by the percentages. As
		// a result, there might be remaining rows unconsidered.
		// To ensure that every doInsert operation is always tied to a valid table, the
		// currentIndex is calculated modulo the length of the array. This prevents an
		// ArrayIndexOutOfBoundsException by wrapping around to the start of the array
		// when it exceeds the array size.
		currentIndex %= TABLE_NAMES.length;

		// Generate the key for the new record
		String key = nextKey(currentIndex, true);

		// Increment the record count
		currentRecordCount.incrementAndGet();

		// If the table is "posts" or "comments", add the additional pairs to the
		// HashMap
		if (TABLE_NAMES[currentIndex].equals("posts")) {
			// Generate a user_id using the proper generator
			String userId = nextKey(Arrays.asList(TABLE_NAMES).indexOf("users"), false);
			values.put("user_id", new StringByteIterator(userId));
		} else if (TABLE_NAMES[currentIndex].equals("comments")) {
			// Generate a post_id using the proper generator
			String postId = nextKey(Arrays.asList(TABLE_NAMES).indexOf("posts"), false);
			values.put("post_id", new StringByteIterator(postId));
		}

		// Insert the new record to the database
		return db.insert(TABLE_NAMES[currentIndex], key, values).isOk();

	}

	/**
	 * This method selects a table index based on the defined probabilities.
	 * 
	 * @return the selected table index
	 */
	private int selectTableIndex() {
		// Generate a random number.
		double p = ThreadLocalRandom.current().nextDouble();
		double cumulativeProbability = 0.0;
		// Iterate through the table probabilities. If the random number is less than or
		// equal to the cumulative probability, return the current table index.
		for (int i = 0; i < TABLE_NAMES.length; i++) {
			cumulativeProbability += TABLE_PROBABILITIES[i];
			if (p <= cumulativeProbability) {
				return i;
			}
		}
		return TABLE_NAMES.length - 1; // Fallback, shouldn't happen
	}

	/**
	 * This function provides the next operation to be performed in the
	 * doTransaction method.
	 * 
	 * @return The operation type to be performed next.
	 */
	private String getNextOperation() {
		// Compute total remaining operations
		int totalRemainingOperations = operationCounters.values().stream().mapToInt(AtomicInteger::get).sum();

		if (totalRemainingOperations == 0) {
			throw new RuntimeException("No operations remaining.");
		}

		// Select a random point within the range of total remaining operations
		double randomPoint = ThreadLocalRandom.current().nextDouble() * totalRemainingOperations;

		// Iterate over the operations map, accumulating the count until it surpasses
		// the random point
		double cumulativeProbability = 0.0;
		for (Map.Entry<String, AtomicInteger> operationEntry : operationCounters.entrySet()) {
			cumulativeProbability += operationEntry.getValue().get();
			if (randomPoint <= cumulativeProbability) {
				// Decrement the counter for the selected operation
				operationEntry.getValue().decrementAndGet();
				return operationEntry.getKey();
			}
		}

		throw new RuntimeException("Unexpected error. No operation selected.");
	}

	/**
	 * This method executes a transaction, which could be a "read", "update",
	 * "insert", or "delete" operation. The type of operation depends on the
	 * remaining operation counts.
	 * 
	 * @param db
	 * @param threadstate
	 * @return true if the transaction was successful.
	 */
	public boolean doTransaction(DB db, Object threadstate) {

		// Select table for the transaction
		int tableIndex = selectTableIndex();
		String tableName = TABLE_NAMES[tableIndex];

		// Generate the key for the transaction
		String key;
		do {
			key = nextKey(tableIndex, false);
		} while (deletedKeys.contains(key));

		// Initialize a HashMap to hold the result of a "read" operation.
		HashMap result = new HashMap<>();

		// System.out.println("table="+tableName+";key="+key);

		Status status;

		String selectedOperation = getNextOperation();

		switch (selectedOperation) {
		default:
			status = Status.ERROR;
			break;
		case "read":
			status = db.read(tableName, key, null, result);
			break;
		case "update":
			HashMap values = generateValues();
			status = db.update(tableName, key, values);
			break;
		case "insert":
			HashMap insertValues = generateValues();
			// Add the run-specific prefix for the key
			key = prefix_run_insert + prefix + insertSequences[tableIndex].nextValue();
			status = db.insert(tableName, key, insertValues);
			break;
		case "delete":
			deletedKeys.add(key);
			status = db.delete(tableName, key);
			break;
		}

		return status.isOk();
	}

}
