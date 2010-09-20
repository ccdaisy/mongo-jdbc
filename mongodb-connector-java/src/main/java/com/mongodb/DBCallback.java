// DBCallback.java

package com.mongodb;

import java.util.*;
import java.util.logging.*;

import org.bson.*;
import org.bson.types.*;

public class DBCallback extends BasicBSONCallback {
    
    public static interface Factory {
        public DBCallback create(String db, String collection);
    }

    static class DefaultFactory implements Factory {
        public DBCallback create(String db, String collection){
            return new DBCallback(db, collection);
        }
    }

    public static Factory FACTORY = new DefaultFactory();

    public DBCallback(String db, String collection){
    	this._db = db;
    	this._collection = collection;
    }

    public void gotDBRef( String name , String ns , ObjectId id ){
//        if ( id.equals( Bytes.COLLECTION_REF_ID ) )
//            cur().put( name , _collection );
//        else
//            cur().put( name , new DBPointer( (DBObject)cur() , name , _db , ns , id ) );
    }
    
    public void objectStart(boolean array, String name){
//        _lastName = name;
//        _lastArray = array;
        super.objectStart( array , name );
    }
    
    public Object objectDone(){
        BSONObject o = (BSONObject)super.objectDone();
//        if ( ! _lastArray && 
//             o.containsKey( "$ref" ) && 
//             o.containsKey( "$id" ) ){
//            return cur().put( _lastName , new DBRef( _db, o ) );
//        }
        
        return o;
    }
    
    public BSONObject create(){
        return _create( null );
    }
    
    public BSONObject create( boolean array , List<String> path ){
        if ( array )
            return new BasicDBList();
        return _create( path );
    }

    private DBObject _create( List<String> path ){
    
        if ( _collection != null && _collection.equals( "$cmd" ) )
            return new CommandResult();
        return new BasicDBObject();
    }

    DBObject dbget(){
        DBObject o = (DBObject)get();
        return o;
    }
    
    public void reset(){
//        _lastName = null;
//        _lastArray = false;
        super.reset();
    }

//	private String _lastName;
//    private boolean _lastArray = false;
    final String _collection;
    final String _db;
    
    static final Logger LOGGER = Logger.getLogger( "com.mongo.DECODING" );
}
