// MongoOptions.java

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

package com.mongodb;


/**
 * Various settings for the driver
 */
/**
 * @author ccdaisy
 *
 */
public class MongoOptions {

    public MongoOptions(){
        reset();
    }

    public void reset(){
        connectTimeout = 0;
        socketTimeout = 0;
        autoConnectRetry = false;
    }


    /**
       connect timeout in milliseconds. 0 is default and infinite
     */
    public int connectTimeout;

    /**
       socket timeout.  0 is default and infinite
     */
    public int socketTimeout;
    
    /**
       this controls whether or not on a connect, the system retries automatically 
    */
    public boolean autoConnectRetry;
    
	/**
	 * whether insert if the record does not exist
	 */
	public boolean upsert;
	/**
	 * whether update multiple records when key match
	 */
	public boolean multiUpdate;
	/**
	 * max wait time for the write result (getlasterror)
	 */
	public int writeWaitTime;
	/**
	 * 
	 */
	public String database;
	
	
    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append( "connectTimeout: " ).append( connectTimeout ).append( " " );
        buf.append( "socketTimeout: " ).append( socketTimeout ).append( " " );
        buf.append( "autoConnectRetry: " ).append( autoConnectRetry ).append( " " );
        return buf.toString();
    }
    
}
