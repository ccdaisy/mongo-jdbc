//AddToSetFunctionHandler.java
package com.mongodb.jdbc.function.update;

import java.util.List;

import com.mongodb.DBObject;

public class IncFunctionHandler extends AbstractUpdateFunctionHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		Long incValue = 0l;
		if (((List) parameters).size() == 0) {
			incValue = 1l;
		} else {
			incValue = (Long)((List) parameters).get(0);
		}
		this.internalAddDBObject(updateSet, getName(), column, incValue);
	}

	@Override
	public String getName() {
		return "$inc";
	}

}
