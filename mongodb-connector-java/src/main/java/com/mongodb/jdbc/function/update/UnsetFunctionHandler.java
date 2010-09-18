//UnsetFunctionHandler.java
package com.mongodb.jdbc.function.update;

import com.mongodb.DBObject;

public class UnsetFunctionHandler extends AbstractUpdateFunctionHandler {

	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		this.internalAddDBObject(updateSet, getName(), column, parameters);
	}

	@Override
	public String getName() {
		return "$unset";
	}

}
