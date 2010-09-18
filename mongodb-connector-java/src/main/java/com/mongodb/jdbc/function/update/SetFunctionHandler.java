//SetFunctionHandler.java
package com.mongodb.jdbc.function.update;

import com.mongodb.DBObject;

public class SetFunctionHandler extends AbstractUpdateFunctionHandler {

	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		this.internalAddDBObject(updateSet, getName(), column, parameters);

	}

	@Override
	public String getName() {
		return "$set";
	}

}
