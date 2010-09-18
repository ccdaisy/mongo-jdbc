//AbstractUpdateFunctionHandler.java
package com.mongodb.jdbc.function.update;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public abstract class AbstractUpdateFunctionHandler {

	public abstract String getName();

	public abstract void handle(String column, DBObject updateSet,
			Object parameters);

	protected void internalAddDBObject(DBObject object, String criteria,
			String column, Object value) {
		if (criteria.startsWith("$")) {
			Object subObject = object.get(criteria);
			if (subObject != null && subObject instanceof DBObject) {
				((DBObject) subObject).put(column, value);
			} else {
				DBObject newCriteriaObject = new BasicDBObject(column, value);
				object.put(criteria, newCriteriaObject);
			}
		}
	}
}
