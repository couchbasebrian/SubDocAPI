package com.couchbase.support;

import rx.functions.Func1;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.subdoc.DocumentFragment;


// Brian Williams
// October 20, 2016
// This was developed with 2.3.4 Java SDK

public class SubdocConcurrencyTest {

	private String hostname = "10.10.10.10";
	private String bucketname = "BUCKETNAME";

	public static void main(String[] args) {
		System.out.println("Welcome to SubdocConcurrencyTest");

		SubdocConcurrencyTest tester = new SubdocConcurrencyTest();
		tester.doTest();

		System.out.println("Now leaving SubdocConcurrencyTest");

	}

	public static String documentId      = "myDocumentKey";
	public static String arrayMemberName = "myArray";

	private void doTest() {
		System.out.println("Beginning of test");

		// Create a Couchbase Environment 
		DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
		DefaultCouchbaseEnvironment environment = builder.build();

		System.out.println("Connecting to cluster");
		Cluster cluster = CouchbaseCluster.create(environment, hostname);

		System.out.println("Connecting to bucket");
		Bucket bucket = cluster.openBucket(bucketname);

		// Insert the starting document

		String jsonDocumentString = "{ \"" + arrayMemberName + "\" : [ ] }";
		JsonObject jsonObject = JsonObject.fromJson(jsonDocumentString);
		JsonDocument document = JsonDocument.create(documentId, jsonObject );
		bucket.upsert(document);

		// Do the test on the given document in the given bucket
		performTestsOnBucket(bucket, documentId);

		// Now look at the document and count the array size
		JsonDocument finalDocument = bucket.get(documentId);

		JsonArray finalArray = finalDocument.content().getArray(SubdocConcurrencyTest.arrayMemberName);

		System.out.println("final array size:" + finalArray.size());

		System.out.println("Closing bucket");
		bucket.close();
		bucket = null;

		System.out.println("Disconnecting from cluster");
		cluster.disconnect();
		cluster = null;

		System.out.println("Shutting down the environment");
		environment.shutdown();
		environment = null;

		builder = null;

		System.out.println("End of test");

	}

	private void performTestsOnBucket(Bucket bucket, String documentId) {

		final int threadsToUse = 5;
		Thread[] threadList = new Thread[threadsToUse];

		SingleDocumentArrayAppendThread[] myTestObject = new SingleDocumentArrayAppendThread[threadsToUse];

		System.out.println("Creating the tester thread objects");

		// Create all of the objects and threads
		for (int i = 0; i < threadsToUse; i++) {
			SingleDocumentArrayAppendThread eachThread = new SingleDocumentArrayAppendThread(bucket, documentId);
			threadList[i] = new Thread(eachThread);
			myTestObject[i] = eachThread;
		}

		System.out.println("Starting the tester thread objects");

		// start them
		for (int i = 0; i < threadsToUse; i++) {
			threadList[i].start();
		}

		// 1 Sleep a bit
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			System.out.println("Exception while sleeping");
		}

		System.out.println("Stopping the tester thread objects");

		// stop them
		for (int i = 0; i < threadsToUse; i++) {
			myTestObject[i].stopRunning();
		}

		// 2 Sleep a bit
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			System.out.println("Exception while sleeping");
		}

		System.out.println("Printing Results");

		int grandTotalAttempts = 0, grandTotalSuccess = 0;

		// Report the results
		for (int i = 0; i < threadsToUse; i++) {
			myTestObject[i].displayResults();
			grandTotalAttempts += myTestObject[i].getAppendsAttempted();
			grandTotalSuccess += myTestObject[i].getAppendsSuccessful();
		}

		System.out.println("Grand totals: " + grandTotalSuccess + "/" + grandTotalAttempts);;

	} // performTestsOnBucket()

} // class SubdocConcurrencyTest 


class SingleDocumentArrayAppendThread implements Runnable {

	private boolean keepRunning = true;
	private Bucket bucketReference = null;
	private long actualStartTime = 0, actualEndTime = 0;
	private long appendsAttempted = 0;
	private long appendsSuccessful = 0;
	private int exceptionCount = 0;
	private String documentId;

	public long getAppendsAttempted() {
		return appendsAttempted;
	}

	public long getAppendsSuccessful() {
		return appendsSuccessful;
	}

	public SingleDocumentArrayAppendThread(Bucket aBucket, String aDocumentId) {
		bucketReference = aBucket;
		documentId = aDocumentId;
	}

	public void stopRunning() { keepRunning = false; }

	public long getTotalRunTime() { return actualEndTime - actualStartTime; }

	public void run() {

		boolean createParents = false;

		actualStartTime = System.currentTimeMillis();
		actualEndTime = System.currentTimeMillis(); // just in case

		while (keepRunning) {

			try {
				appendsAttempted++;

				OperationResult op = new OperationResult();

				// do a subdoc array insert
				int valueToInsert= 100;

				boolean useOriginalVersion = false;

				if (useOriginalVersion) {
					bucketReference.async().mutateIn(documentId).arrayAppend(SubdocConcurrencyTest.arrayMemberName, valueToInsert, createParents ).execute().map( 
							new Func1<DocumentFragment<Mutation>, OperationResult>() { 
								public OperationResult call(DocumentFragment<Mutation> doc) { 
									//Success and pass thru! 
									op.setStatus(OperationResult.UPSERT_SUCCESS); 
									return op; 
								}

							}).onErrorReturn(new Func1<Throwable, OperationResult>() { 
								public OperationResult call(Throwable e) { 
									op.setError(e); 
									op.setStatus(OperationResult.UPSERT_FAILURE ); 
									return op; 
								} 
							}).toBlocking();
				}
				else {
					bucketReference.mutateIn(documentId).arrayAppend(SubdocConcurrencyTest.arrayMemberName, valueToInsert, createParents).execute();
				}

				appendsSuccessful++;
			}
			catch (Exception e) {
				System.out.println(e);
				exceptionCount++;
			}

		}
		actualEndTime = System.currentTimeMillis();

	} // run() method

	public void displayResults() {
		System.out.println("I ran for " + getTotalRunTime() 
				+ "ms, attempted " 
				+ appendsAttempted 
				+ " operations and " 
				+ appendsSuccessful 
				+ " of those succeeded.  Exceptions:"
				+ exceptionCount);
	}

} // SingleDocumentArrayAppendThread


// EOF
