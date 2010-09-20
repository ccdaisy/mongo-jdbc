// MongoDriver.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.jdbc;

import java.sql.*;
import java.util.*;
import com.mongodb.*;
import com.mongodb.utils.StringUtils;

public class MongoDriver implements Driver {

    static final String PREFIX = "mongodb://";

    public MongoDriver(){
    }

    public boolean acceptsURL(String url){
        return url.startsWith( PREFIX );
    }
    
    public Connection connect(String url, Properties info)
        throws SQLException {
    	Boolean upsert = true;
    	Boolean multiUpdate = true;
    	Boolean autoRetry = false;
    	Integer connectTimeout = 0;
    	Integer socketTimeout = 0;
    	Integer writeWaitTime = 0;
    	String database = "";
    	
        if ( info != null && info.size() > 0 ){
        	upsert = Boolean.valueOf(info.getProperty("upsert","true"));
        	multiUpdate = Boolean.valueOf(info.getProperty("multiUpdate","true"));
        	autoRetry = Boolean.valueOf(info.getProperty("autoRetry","false"));
        	connectTimeout = Integer.valueOf(info.getProperty("connectTimeout","0"));
        	socketTimeout = Integer.valueOf(info.getProperty("socketTimeout","0"));
        	writeWaitTime = Integer.valueOf(info.getProperty("writeWaitTime","0"));
        }
        if ( url.startsWith( PREFIX ) )
            url = url.substring( PREFIX.length() );
        if ( url.indexOf("/") >= 0) {
        	database = StringUtils.splitAtLast(url, "/")[1];
        }
        try {
            DBAddress addr = new DBAddress( url );
            MongoOptions options = new MongoOptions();
            options.upsert = upsert;
            options.multiUpdate = multiUpdate;
            options.autoConnectRetry = autoRetry;
            options.connectTimeout = connectTimeout;
            options.socketTimeout = socketTimeout;
            options.writeWaitTime = writeWaitTime;
            options.database = database;
            MongoConnection connection = new MongoConnection();
            connection.set_m(new Mongo(addr,options));
            return connection;
        }
        catch ( java.net.UnknownHostException uh ){
            throw new MongoSQLException( "bad url: " + uh );
        }
    }
    
    public int getMajorVersion(){
        return 0;
    }
    public int getMinorVersion(){
        return 1;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info){
        throw new UnsupportedOperationException( "getPropertyInfo doesn't work yet" );
    }

    public boolean jdbcCompliant(){
        return false;
    }

    public static void install(){
        // NO-OP, handled in static
    }

    static {
        try {
            DriverManager.registerDriver( new MongoDriver() );
        }
        catch ( SQLException e ){
            throw new RuntimeException( e );
        }
    }
}
