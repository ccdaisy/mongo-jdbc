// OutMessage.java

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

import static org.bson.BSON.REF;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BSONEncoder;
import org.bson.io.PoolOutputBuffer;
import org.bson.types.ObjectId;

class OutMessage extends BSONEncoder {

    static AtomicInteger ID = new AtomicInteger(1);
    
    static ThreadLocal<OutMessage> TL = new ThreadLocal<OutMessage>(){
        protected OutMessage initialValue(){
            return new OutMessage();
        }
    };

    static OutMessage get( int op ){
        OutMessage m = TL.get();
        m.reset( op );
        return m;
    }

    static void newTL(){
        TL.set( new OutMessage() );
    }
    
    static OutMessage query( int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        OutMessage out = get( 2004 );
        out._appendQuery( options , ns , numToSkip , batchSize , query , fields );
        return out;
    }

    OutMessage(){
        set( _buffer );
    }
    
    private void _appendQuery( int options , String ns , int numToSkip , int batchSize , DBObject query , DBObject fields ){
        writeInt( options );
        writeCString( ns );

        writeInt( numToSkip );
        writeInt( batchSize );
        
        putObject( query );
        if ( fields != null )
            putObject( fields );

    }

    private void reset( int op ){
        done();
        _buffer.reset();
        set( _buffer );
        
        _id = ID.getAndIncrement();

        writeInt( 0 ); // will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( op );
    }

    void prepare(){
        _buffer.writeInt( 0 , _buffer.size() );
    }
    

    protected void putDBPointer( String name , String ns , ObjectId oid ){
        _put( REF , name );
        
        _putValueString( ns );
        _buf.writeInt( oid._time() );
        _buf.writeInt( oid._machine() );
        _buf.writeInt( oid._inc() );
    }


    void append( String db , WriteConcern c ){

        _id = ID.getAndIncrement();

        int loc = size();

        writeInt( 0 ); // will set this later
        writeInt( _id );
        writeInt( 0 ); // response to
        writeInt( 2004 );
        _appendQuery( 0 , db + ".$cmd" , 0 , -1 , c.getCommand() , null );
        _buf.writeInt( loc , size() - loc );
    }

    void pipe( OutputStream out )
        throws IOException {
        _buffer.pipe( out );
    }

    int size(){
        return _buffer.size();
    }

    byte[] toByteArray(){
        return _buffer.toByteArray();
    }
    
    private PoolOutputBuffer _buffer = new PoolOutputBuffer();
    private int _id;
}
