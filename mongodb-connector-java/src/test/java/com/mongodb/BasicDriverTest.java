package com.mongodb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.mongodb.jdbc.MongoDriver;

import junit.framework.TestCase;

public class BasicDriverTest extends TestCase {
	Connection mongoConnection;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		MongoDriver driver = new MongoDriver();
		mongoConnection = driver.connect("jdbc:mongodb://10.241.14.44:27017",
				new Properties());
	}

	public void testBasic() throws SQLException {
		Statement statement = mongoConnection.createStatement();
		int affected = statement
				.executeUpdate("insert into test.test (a, b, c, d, e) values(12345,'abcd',123,'aaa','xxx')");
		System.out
				.println("Inset one row into test.test, affected:" + affected);
		affected = statement
				.executeUpdate("update test.test set e='ppp' where a=12345");
		System.out.println("Update one row into test.test, affected:"
				+ affected);
		ResultSet result = statement.executeQuery("SELECT * FROM test.test");
		while (result.next()) {
			System.out.println("a:" + result.getInt("a") + ", b:"
					+ result.getString("b") + ", c:" + result.getInt("c")
					+ ", d:" + result.getString("d") + ", e:"
					+ result.getString("e"));
		}
	}

	@Override
	protected void tearDown() throws Exception {
		mongoConnection.close();
		super.tearDown();
	}

}
