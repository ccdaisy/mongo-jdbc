//PullAllFunctionHandler.java
package com.mongodb.jdbc.function.update;

import java.util.List;

import com.mongodb.DBObject;

public class PullAllFunctionHandler extends AbstractUpdateFunctionHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		this.internalAddDBObject(updateSet, this.getName(), column,
				((List) parameters).toArray());

	}

	@Override
	public String getName() {
		return "$pullAll";
	}

}
