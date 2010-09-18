//PushFunctionHandler.java
package com.mongodb.jdbc.function.update;

import java.util.List;

import com.mongodb.DBObject;

public class PushFunctionHandler extends AbstractUpdateFunctionHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		this.internalAddDBObject(updateSet, this.getName(), column,
				((List) parameters).get(0));

	}

	@Override
	public String getName() {
		return "$push";
	}

}
