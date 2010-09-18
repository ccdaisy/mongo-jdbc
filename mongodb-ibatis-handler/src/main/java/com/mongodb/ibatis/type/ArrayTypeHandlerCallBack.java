package com.mongodb.ibatis.type;

import java.lang.reflect.Array;
import java.sql.SQLException;

import com.ibatis.sqlmap.client.extensions.ParameterSetter;
import com.ibatis.sqlmap.client.extensions.ResultGetter;
import com.ibatis.sqlmap.client.extensions.TypeHandlerCallback;
import com.mongodb.BasicDBList;

public class ArrayTypeHandlerCallBack implements TypeHandlerCallback {

	public Object getResult(ResultGetter getter) throws SQLException {
		Object array = getter.getObject();
		Object retObject;
		if(array != null && array instanceof BasicDBList){
			BasicDBList dblist = (BasicDBList) array;
			int size = dblist.size();
			if(size > 0){
				retObject = Array.newInstance(dblist.get(0).getClass(), size);
				System.arraycopy(dblist.toArray(), 0, retObject, 0, size);
				return retObject;
			}else {
				return null;
			}
		
		} else {
			return null;
		}
	}

	public void setParameter(ParameterSetter setter, Object parameter)
			throws SQLException {
		setter.setObject(parameter);
		
	}

	public Object valueOf(String s) {
		// TODO Auto-generated method stub
		return null;
	}


}
