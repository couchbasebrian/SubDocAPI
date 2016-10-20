package com.couchbase.support;

public class OperationResult {

	boolean upsertSuccess;
	Throwable myError;
	
	protected static final boolean UPSERT_SUCCESS = true;
	protected static final boolean UPSERT_FAILURE = false;

	public void setStatus(boolean upsertSuccessBool) {
		//String ch = upsertSuccessBool ? "." : "x";
		upsertSuccess = upsertSuccessBool;
		//System.out.print(ch);
	}

	public void setError(Throwable e) {
		myError = e;
	}

}
