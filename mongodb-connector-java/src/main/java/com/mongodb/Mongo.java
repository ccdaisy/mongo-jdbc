package com.mongodb;

import java.util.Collection;
import java.util.Iterator;

import org.bson.BSONObject;
import org.bson.types.ObjectId;

import com.mongodb.utils.Bytes;
import com.mongodb.utils.JSON;
import com.mongodb.utils.StringUtils;

public class Mongo {
	
	public Mongo(){
		this.options = new MongoOptions();
	}
	public Mongo(ServerAddress address, MongoOptions options) {
		connector = new DBTCPConnector(address, options);
		this.options = options;
	}

	public DBCursor find(String namespace, DBObject res, DBObject keys) {
		DBCursor cursor = new DBCursor(this, namespace, res, keys);

		return cursor;
	}

	Iterator<DBObject> _find(String namespace, DBObject ref, DBObject fields,
			int numToSkip, int batchSize, int options) {

		if (ref == null)
			ref = new BasicDBObject();

		if (SHOW)
			System.out
					.println("find: " + namespace + " " + JSON.serialize(ref));

		OutMessage query = OutMessage.query(options, namespace, numToSkip,
				batchSize, ref, fields);

		Response res = connector.call(namespace, query, 2);

		String[] names = StringUtils.splitAtFirst(namespace, ".");

		if (res.size() == 0)
			return null;

		if (res.size() == 1) {
			BSONObject foo = res.get(0);
			MongoException e = MongoException.parse(foo);
			if (e != null && !names[2].equals("$cmd"))
				throw e;
		}
		return res.iterator();
	}

	public WriteResult insert(String namespace, DBObject[] arr,
			WriteConcern concern) {

		if (SHOW) {
			for (DBObject o : arr) {
				System.out.println("save:  " + namespace + " "
						+ JSON.serialize(o));
			}
		}
		for (int i = 0; i < arr.length; i++) {
			DBObject o = arr[i];
			if ((o.get("_id")) == null) {
				o.put("_id", ObjectId.get());
			}
			Object id = o.get("_id");
			if (id instanceof ObjectId) {
				((ObjectId) id).notNew();
			}
		}
		WriteResult last = null;
		int cur = 0;
		while (cur < arr.length) {
			OutMessage om = OutMessage.get(2002);

			om.writeInt(0); // reserved
			om.writeCString(namespace);

			for (; cur < arr.length; cur++) {
				DBObject o = arr[cur];
				int sz = om.putObject(o);
				if (sz > Bytes.MAX_OBJECT_SIZE)
					throw new IllegalArgumentException("object too big: " + sz);

				if (om.size() > (4 * 1024 * 1024)) {
					cur++;
					break;
				}
			}
			last = this.connector.say(namespace, om, concern);
		}
		return last;
	}

	public WriteResult update(String namespace, DBObject q, DBObject o,
			boolean upsert, boolean multi, WriteConcern concern) {

		if (SHOW)
			System.out
					.println("update: " + namespace + " " + JSON.serialize(q));

		OutMessage om = OutMessage.get(2001);
		om.writeInt(0); // reserved
		om.writeCString(namespace);

		int flags = 0;
		if (upsert)
			flags |= 1;
		if (multi)
			flags |= 2;
		om.writeInt(flags);

		om.putObject(q);
		om.putObject(o);

		return connector.say(namespace, om, concern);
	}

	public WriteResult remove(String namespace, DBObject o, WriteConcern concern) {

		if (SHOW)
			System.out
					.println("remove: " + namespace + " " + JSON.serialize(o));

		OutMessage om = OutMessage.get(2006);

		om.writeInt(0); // reserved
		om.writeCString(namespace);

		Collection<String> keys = o.keySet();

		if (keys.size() == 1 && keys.iterator().next().equals("_id")
				&& o.get(keys.iterator().next()) instanceof ObjectId)
			om.writeInt(1);
		else
			om.writeInt(0);

		om.putObject(o);

		return connector.say(namespace, om, concern);
	}
	
	public void close() {
		this.connector.close();
	}
	
	
	public final MongoOptions options;
	DBTCPConnector connector;
	private boolean SHOW = true;

	
}
