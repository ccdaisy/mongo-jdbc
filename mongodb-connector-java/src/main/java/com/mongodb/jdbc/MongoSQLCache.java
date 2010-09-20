package com.mongodb.jdbc;

import net.sf.jsqlparser.statement.Statement;

import com.mongodb.utils.LRUCache;

public class MongoSQLCache {
	public static Integer CACHE_SIZE = 30;
	private static LRUCache<Integer, Statement> sqlCache = new LRUCache<Integer, Statement>(CACHE_SIZE);
	
	public static void putStatement(Integer hashcode, Statement statement){
		sqlCache.put(hashcode, statement);
	}
	
	public static Statement getStatement(Integer hashcode){
		return sqlCache.get(hashcode);
	}
}
