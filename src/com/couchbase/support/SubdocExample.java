package com.couchbase.support;

import rx.functions.Action0;
import rx.functions.Action1;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;


// August 31, 2016
// This is for Couchbase Java SDK 2.3.2

// Web resources:
// http://blog.couchbase.com/2016/february/subdocument-couchbase-java-sdk
// http://developer.couchbase.com/documentation/server/current/sdk/java/handling-error-conditions.html

// Illustrate special set of Sub-Document Operation Errors

// Broadly speaking the ways to fail are
// * Error Handling - Logging
// * Error Handling - Failing - onError(), onNext()
// * Error Handling - Retry - retry()
// * Error Handling - Fallback - onErrorReturn(), onErrorResumeNext()


public class SubdocExample {

	public static void main(String[] args) {

		System.out.println("Welcome to SubdocExample");
		
		SubdocExample se = new SubdocExample();

		Cluster cluster = CouchbaseCluster.create("10.100.10.1");
		Bucket bucket = cluster.openBucket("BUCKETNAME");

		se.doExample(bucket);

		bucket.close();
		cluster.disconnect();
		
		System.out.println("Now leaving SubdocExample");

	} // main()

	boolean allDone = false;
	
	SubdocExample() {

	}

	void doExample(Bucket b) {

		
		// Start off with a basic doc
		String jsonDocumentString = "{ \"message\" : \"hello\" }";
		JsonObject jsonObject = com.couchbase.client.java.document.json.JsonObject.fromJson(jsonDocumentString);
		int randomNumber = (int) (Math.random() * 1000);
		String documentKey = "subdocExample" + randomNumber;		
		int documentExpiry = 60;
		JsonDocument jsonDocument = JsonDocument.create(documentKey, documentExpiry, jsonObject);
		
		try {
			b.insert(jsonDocument);
			System.out.println("Successfully inserted the document " + documentKey);
			
			AsyncBucket b2 = b.async();
			
			// Now add a member
			JsonObject arrayToInsert = JsonObject.empty();
			b2.mutateIn(documentKey).insert(".", arrayToInsert, false).execute().doOnError( new Action1<Throwable>() {
				public void call(Throwable t) {
					System.out.println("Throwable when inserting array" + t);
				}
			}
			).doOnCompleted(new Action0() {
				public void call() {
					getAndPrint(b, documentKey);
					System.out.println("Complete");	
					allDone = true;
				}
			}).subscribe();
			
			
		}
		catch (com.couchbase.client.java.error.DocumentAlreadyExistsException dae) {
			System.out.println("The document already existed in the bucket");
		}
		catch (Exception e){
			System.out.println("Caught exception: " + e.toString());
			e.printStackTrace();
		}
		
		while (!allDone) {
			System.out.println("Waiting to be done");
			try {
				Thread.sleep(1000);
			}
			catch(Exception e) {
				System.out.println("Interrupted while sleeping");
			}
		}

	} // doExample()

	static void getAndPrint(Bucket b, String docId) {
		JsonDocument foo = b.get(docId);
		JsonObject jo = foo.content();
		String contentString = jo.toString();
		System.out.println(docId + " : " + contentString);
	}
	
} // SubdocExample

// EOF
