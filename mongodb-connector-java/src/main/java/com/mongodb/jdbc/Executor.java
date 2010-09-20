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

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.QueryOperators;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.jdbc.function.update.AbstractUpdateFunctionHandler;
import com.mongodb.jdbc.function.update.UpdateFunctionMap;

public class Executor {

    static final boolean D = true;

    Executor( Mongo m , String sql )
        throws MongoSQLException {
        _m = m;
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
        
//        Table fromTable = (Table) ps.getFromItem();
//        String[] names = StringUtils.splitAtFirst(fromTable.toString(), ".");
//        
//        DB db = this.getDB(names[0]);
//        DBCollection coll = db.getCollection(names[1]);
        
        Table fromTable = (Table) ps.getFromItem();
        String tableName = fromTable.toString();
        String dbName = _m.options.database;
        String namespace;
        if (dbName.equals("")){
        	namespace = tableName;
        } else {
        	namespace = new StringBuilder()
        	.append(dbName)
        	.append('.')
        	.append(tableName)
        	.toString();
        }
        
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
        if ( D ) System.out.println( "\t" + "namespace: " + namespace );
        if ( D ) System.out.println( "\t" + "fields: " + fields );
        if ( D ) System.out.println( "\t" + "query : " + query );
        DBCursor c = _m.find(namespace, query , fields );
        
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
//        else if ( _statement instanceof Drop )
//            return drop( (Drop)_statement );

        throw new RuntimeException( "unknown write: " + _statement.getClass() );
    }
    
    @SuppressWarnings("unchecked")
	int insert( Insert in )
        throws MongoSQLException {

        if ( in.getColumns() == null )
            throw new MongoSQLException.BadSQL( "have to give column names to insert" );
        
        Table fromTable = (Table) in.getTable();
        String tableName = fromTable.toString();
        String dbName = _m.options.database;
        String namespace;
        if (dbName.equals("")){
        	namespace = tableName;
        } else {
        	namespace = new StringBuilder()
        	.append(dbName)
        	.append('.')
        	.append(tableName)
        	.toString();
        }
        
        if ( D ) System.out.println( "\t" + "namespace: " + namespace );
        
        if ( ! ( in.getItemsList() instanceof ExpressionList ) )
            throw new UnsupportedOperationException( "need ExpressionList" );
        
        BasicDBObject o = new BasicDBObject();

        List valueList = ((ExpressionList)in.getItemsList()).getExpressions();
        if ( in.getColumns().size() != valueList.size() )
            throw new MongoSQLException.BadSQL( "number of values and columns have to match" );

        for ( int i=0; i<valueList.size(); i++ ){
            o.put( in.getColumns().get(i).toString() , toConstant( (Expression)valueList.get(i) ) );

        }

        WriteResult result = _m.insert(namespace, new BasicDBObject[]{o},
        		new WriteConcern(1, _m.options.writeWaitTime) );   
        return result.getN();
    }

    @SuppressWarnings("unchecked")
	int update( Update up )
        throws MongoSQLException {
        
        
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


        DBObject query = parseWhere( up.getWhere() );
        
        Table fromTable = (Table) up.getTable();
        String tableName = fromTable.toString();
        String dbName = _m.options.database;
        String namespace;
        if (dbName.equals("")){
        	namespace = tableName;
        } else {
        	namespace = new StringBuilder()
        	.append(dbName)
        	.append('.')
        	.append(tableName)
        	.toString();
        }
        
        if( D ) System.out.println(mod);
        WriteResult result = _m.update(namespace, query, mod, _m.options.upsert,
        		_m.options.multiUpdate, new WriteConcern(1, _m.options.writeWaitTime));
        return result.getN();
    }

//    int drop( Drop d ){
//    	String[] names = StringUtils.splitAtFirst(d.getName(), ".");
//    	DB db = this.getDB(names[0]);
//    	DBCollection coll = db.getCollection(names[1]);
//    	coll.drop();
//    	return 1;
//    }

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
        this.fillDBObjectWithExpression(o, e);
        if( D ) System.out.println(o);
        return o;
    }
    
    @SuppressWarnings("unchecked")
	void fillDBObjectWithExpression(BasicDBObject o, Expression e){
    	if(e instanceof AndExpression){
    		fillDBObjectWithExpression(o, ((AndExpression) e).getLeftExpression());
    		fillDBObjectWithExpression(o, ((AndExpression) e).getRightExpression());
    	}else if(e instanceof EqualsTo){
    		EqualsTo expression = (EqualsTo) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
    		o.put(column, value);
    	}else if(e instanceof NotEqualsTo){
    		NotEqualsTo expression = (NotEqualsTo) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
			this.internalPutDBObjet(o, column, QueryOperators.NE, value);
    	}else if(e instanceof InExpression){
    		InExpression expression = (InExpression) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		ArrayList valueList = (ArrayList) ((ExpressionList)expression.getItemsList())
    		.getExpressions();
    		Object[] value = new Object[valueList.size()];
    		for (int i = 0; i < value.length; i++) {
				value[i] = toConstant((Expression) valueList.get(i));
			}
    		if(expression.isNot()){
    			this.internalPutDBObjet(o, column, QueryOperators.NIN, value);
    		}else {
    			this.internalPutDBObjet(o, column, QueryOperators.IN, value);
    		}
    	}else if(e instanceof GreaterThan){
    		GreaterThan expression = (GreaterThan) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
			this.internalPutDBObjet(o, column, QueryOperators.GT, value);
    	}else if(e instanceof GreaterThanEquals){
    		GreaterThanEquals expression = (GreaterThanEquals) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
			this.internalPutDBObjet(o, column, QueryOperators.GTE, value);
    	}else if(e instanceof MinorThan){
    		MinorThan expression = (MinorThan) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
			this.internalPutDBObjet(o, column, QueryOperators.LT, value);
    	}else if(e instanceof MinorThanEquals){
    		MinorThanEquals expression = (MinorThanEquals) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		Object value = toConstant(expression.getRightExpression());
			this.internalPutDBObjet(o, column, QueryOperators.LTE, value);
    	}else if(e instanceof Parenthesis){
    		fillDBObjectWithExpression(o, ((Parenthesis) e).getExpression());
    	}else if(e instanceof IsNullExpression){
    		IsNullExpression expression = (IsNullExpression) e;
    		String column = ((Column)expression.getLeftExpression()).getColumnName();
    		if (expression.isNot()){
        		this.internalPutDBObjet(o, column, QueryOperators.EXISTS, true);
    		}else {
        		this.internalPutDBObjet(o, column, QueryOperators.EXISTS, false);
    		}
    	}else if(e instanceof OrExpression){
    		BasicDBObject[] orObjects = new BasicDBObject[2];
    		orObjects[0] = new BasicDBObject();
    		orObjects[1] = new BasicDBObject();
    		fillDBObjectWithExpression(orObjects[0], ((OrExpression) e).getLeftExpression());
    		fillDBObjectWithExpression(orObjects[1], ((OrExpression) e).getRightExpression());
    		o.put("$or", orObjects);
    	}else if(e instanceof ExistsExpression){
            throw new UnsupportedOperationException( "can't handle: " + e.getClass() + " yet" );
    	}
    }
    
    void internalPutDBObjet(DBObject o, String columnName, String criteria, Object value){
    	Object subObject = o.get(columnName);
    	if(subObject == null){
    		subObject = new BasicDBObject(criteria, value);
    		o.put(columnName, subObject);
    	}else if (subObject instanceof DBObject){
    		((DBObject) subObject).put(criteria, value);
    	}    	
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


    final Mongo _m;
    final String _sql;
    final Statement _statement;
    
    @SuppressWarnings("unchecked")
	List _params;
    int _pos;
}
