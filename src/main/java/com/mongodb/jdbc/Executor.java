// Executor.java

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

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.drop.*;

import com.mongodb.*;
import com.mongodb.jdbc.function.update.AbstractUpdateFunctionHandler;
import com.mongodb.jdbc.function.update.UpdateFunctionMap;

public class Executor {

    static final boolean D = true;

    Executor( DB db , String sql )
        throws MongoSQLException {
        _db = db;
        _sql = sql;
        _statement = parse( sql );

        if ( D ) System.out.println( sql );
    }

    @SuppressWarnings("unchecked")
	void setParams( List params ){
        _pos = 1;
        _params = params;
    }

    @SuppressWarnings("unchecked")
	DBCursor query()
        throws MongoSQLException {
        if ( ! ( _statement instanceof Select ) )
            throw new IllegalArgumentException( "not a query sql statement" );
        
        Select select = (Select)_statement;
        if ( ! ( select.getSelectBody() instanceof PlainSelect ) )
            throw new UnsupportedOperationException( "can only handle PlainSelect so far" );
        
        PlainSelect ps = (PlainSelect)select.getSelectBody();
        if ( ! ( ps.getFromItem() instanceof Table ) )
            throw new UnsupportedOperationException( "can only handle regular tables" );
        
        DBCollection coll = getCollection( (Table)ps.getFromItem() );

        BasicDBObject fields = new BasicDBObject();
        for ( Object o : ps.getSelectItems() ){
            SelectItem si = (SelectItem)o;
            if ( si instanceof AllColumns ){
                if ( fields.size() > 0 )
                    throw new UnsupportedOperationException( "can't have * and fields" );
                break;
            }
            else if ( si instanceof SelectExpressionItem ){
                SelectExpressionItem sei = (SelectExpressionItem)si;
                fields.put( toFieldName( sei.getExpression() ) , 1 );
            }
            else {
                throw new UnsupportedOperationException( "unknown select item: " + si.getClass() );
            }
        }
        
        // where
        DBObject query = parseWhere( ps.getWhere() );
        
        // done with basics, build DBCursor
        if ( D ) System.out.println( "\t" + "table: " + coll );
        if ( D ) System.out.println( "\t" + "fields: " + fields );
        if ( D ) System.out.println( "\t" + "query : " + query );
        DBCursor c = coll.find( query , fields );
        
		// limit
		Limit limit = ps.getLimit();
		if (limit != null) {
			c.limit((int) limit.getRowCount()).skip((int) limit.getOffset());

		}
        
        { // order by
            List orderBylist = ps.getOrderByElements();
            if ( orderBylist != null && orderBylist.size() > 0 ){
                BasicDBObject order = new BasicDBObject();
                for ( int i=0; i<orderBylist.size(); i++ ){
                    OrderByElement o = (OrderByElement)orderBylist.get(i);
                    order.put( o.getColumnReference().toString() , o.isAsc() ? 1 : -1 );
                }
                c.sort( order );
            }
        }

        return c;
    }

    int writeop()
        throws MongoSQLException {
        
        if ( _statement instanceof Insert )
            return insert( (Insert)_statement );
        else if ( _statement instanceof Update )
            return update( (Update)_statement );
        else if ( _statement instanceof Drop )
            return drop( (Drop)_statement );

        throw new RuntimeException( "unknown write: " + _statement.getClass() );
    }
    
    @SuppressWarnings("unchecked")
	int insert( Insert in )
        throws MongoSQLException {

        if ( in.getColumns() == null )
            throw new MongoSQLException.BadSQL( "have to give column names to insert" );
        
        DBCollection coll = getCollection( in.getTable() );
        if ( D ) System.out.println( "\t" + "table: " + coll );
        
        if ( ! ( in.getItemsList() instanceof ExpressionList ) )
            throw new UnsupportedOperationException( "need ExpressionList" );
        
        BasicDBObject o = new BasicDBObject();

        List valueList = ((ExpressionList)in.getItemsList()).getExpressions();
        if ( in.getColumns().size() != valueList.size() )
            throw new MongoSQLException.BadSQL( "number of values and columns have to match" );

        for ( int i=0; i<valueList.size(); i++ ){
            o.put( in.getColumns().get(i).toString() , toConstant( (Expression)valueList.get(i) ) );

        }

        coll.insert( o );        
        return 1; // TODO - this is wrong
    }

    @SuppressWarnings("unchecked")
	int update( Update up )
        throws MongoSQLException {
        
        DBObject query = parseWhere( up.getWhere() );
        
        BasicDBObject mod = new BasicDBObject();
        for ( int i=0; i<up.getColumns().size(); i++ ){
            String criteria = "";
            String k = up.getColumns().get(i).toString();
            Expression v = (Expression)(up.getExpressions().get(i));
            Object updateSet = toConstant(v);
            Object params;
            if(updateSet instanceof Function) {
            	Function updateFunc = (Function)updateSet;
            	criteria = updateFunc.getName().toLowerCase();
            	params = new ArrayList<Object>();
            	if(updateFunc.getParameters() != null){
            		for(Object o:updateFunc.getParameters().getExpressions()){
            			((List)params).add(toConstant((Expression)o));
            		}
            	}
            }else if (updateSet == null) {
            	criteria = "unset";
            	params = 1;
            }else {
            	criteria = "set";
            	params = updateSet;
            }
            AbstractUpdateFunctionHandler handler = 
            	UpdateFunctionMap.handlerMap.get(criteria);
            handler.handle(k, mod, params);
        }


        DBCollection coll = getCollection( up.getTable() );
        if( D ) System.out.println(mod);
        coll.update( query , mod, true, true);
        return 1; // TODO
    }


    int drop( Drop d ){
        DBCollection c = _db.getCollection( d.getName() );
        c.drop();
        return 1;
    }

    // ---- helpers -----

    String toFieldName( Expression e ){
        if ( e instanceof StringValue )
            return e.toString();
        if ( e instanceof Column )
            return e.toString();
        throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass() + " into field name" );
    }

    Object toConstant( Expression e ){
        if ( e instanceof StringValue )
            return ((StringValue)e).getValue();
        else if ( e instanceof DoubleValue )
            return ((DoubleValue)e).getValue();
        else if ( e instanceof LongValue )
            return ((LongValue)e).getValue();
        else if ( e instanceof NullValue )
            return null;
        else if ( e instanceof JdbcParameter )
            return _params.get( _pos++ );
        else if (e instanceof Function)
        	return (Function)e;
                 
        throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass().getName() + " into constant " );
    }


    DBObject parseWhere( Expression e ){
        BasicDBObject o = new BasicDBObject();
        if ( e == null )
            return o;
        
        if ( e instanceof EqualsTo ){
            EqualsTo eq = (EqualsTo)e;
            o.put( toFieldName( eq.getLeftExpression() ) , toConstant( eq.getRightExpression() ) );
        }
        else {
            throw new UnsupportedOperationException( "can't handle: " + e.getClass() + " yet" );
        }

        return o;
    }

    Statement parse( String s )
        throws MongoSQLException {
        s = s.trim();
        
        try {
            return (new CCJSqlParserManager()).parse( new StringReader( s ) );
        }
        catch ( Exception e ){
            e.printStackTrace();
            throw new MongoSQLException.BadSQL( s );
        }
        
    }

    // ----
    
    DBCollection getCollection( Table t ){
        return _db.getCollection( t.toString() );
    }

    final DB _db;
    final String _sql;
    final Statement _statement;
    
    List _params;
    int _pos;
}
