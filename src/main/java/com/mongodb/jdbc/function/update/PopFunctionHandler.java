package com.mongodb.jdbc.function.update;

import java.util.List;

import com.mongodb.DBObject;

public class PopFunctionHandler extends AbstractUpdateFunctionHandler {

	@SuppressWarnings("unchecked")
	@Override
	public void handle(String column, DBObject updateSet, Object parameters) {
		Long popValue = 0l;
		if (((List) parameters).size() == 0) {
			popValue = 1l;
		} else {
			popValue = (Long) ((List) parameters).get(0);
		}
		this.internalAddDBObject(updateSet, getName(), column, popValue);

	}

	@Override
	public String getName() {
		return "$pop";
	}

}
