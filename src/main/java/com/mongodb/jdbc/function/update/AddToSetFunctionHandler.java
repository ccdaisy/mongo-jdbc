//AddToSetFunctionHandler.java
package com.mongodb.jdbc.function.update;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class AddToSetFunctionHandler extends AbstractUpdateFunctionHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handle(String column, DBObject updateSet,
			Object parameters) {
		this.internalAddDBObject(updateSet, this.getName(), column,
				new BasicDBObject("$each", ((List)parameters).toArray()));

	}

	@Override
	public String getName() {
		return "$addToSet";
	}

}
