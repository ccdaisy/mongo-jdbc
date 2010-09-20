// DBTCPConnector.java

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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.utils.Bytes;
import com.mongodb.utils.StringUtils;

class DBTCPConnector implements DBConnector {

	static Logger _logger = Logger.getLogger(Bytes.LOGGER.getName() + ".tcp");
	static Logger _createLogger = Logger.getLogger(_logger.getName()
			+ ".connect");

	public DBTCPConnector(ServerAddress addr, MongoOptions options) throws MongoException {
		this.options = options;
		_checkAddress(addr);

		_createLogger.info(addr.toString());

		if (addr.isPaired()) {
			_allHosts = new ArrayList<ServerAddress>(addr.explode());
			_createLogger.info("switching to replica set mode : " + _allHosts
					+ " -> " + _curAddress);
		} else {
			try {
				_set(addr,options);
			} catch (IOException e) {
				throw new MongoException("can't switch address");
			}
			_allHosts = null;
		}

	}

	public DBTCPConnector(Mongo m, MongoOptions options , ServerAddress... all) throws MongoException {
		this(m,options, Arrays.asList(all));
	}

	public DBTCPConnector(Mongo m, MongoOptions options , List<ServerAddress> all)
			throws MongoException {
		this.options = options;
		_checkAddress(all);

		_allHosts = new ArrayList<ServerAddress>(all); // make a copy so it
														// can't be modified

		_createLogger.info(all + " -> " + _curAddress);
	}

	private static ServerAddress _checkAddress(ServerAddress addr) {
		if (addr == null)
			throw new NullPointerException("address can't be null");
		return addr;
	}

	private static ServerAddress _checkAddress(List<ServerAddress> addrs) {
		if (addrs == null)
			throw new NullPointerException("addresses can't be null");
		if (addrs.size() == 0)
			throw new IllegalArgumentException(
					"need to specify at least 1 address");
		return addrs.get(0);
	}

	/**
	 * Start a "request".
	 * 
	 * A "request" is a group of operations in which order matters. Examples
	 * include inserting a document and then performing a query which expects
	 * that document to have been inserted, or performing an operation and then
	 * using com.mongodb.Mongo.getLastError to perform error-checking on that
	 * operation. When a thread performs operations in a "request", all
	 * operations will be performed on the same socket, so they will be
	 * correctly ordered.
	 */
	public void requestStart() {
		// _threadPort.get().requestStart();
	}

	/**
	 * End the current "request", if this thread is in one.
	 * 
	 * By ending a request when it is safe to do so the built-in connection-
	 * pool is allowed to reassign requests to different sockets in order to
	 * more effectively balance load. See requestStart for more information.
	 */
	public void requestDone() {
		// _threadPort.get().requestDone();
	}

	public void requestEnsureConnection() {
		// _threadPort.get().requestEnsureConnection();
	}

	WriteResult _checkWriteError(String db, WriteConcern concern) throws MongoException {

		CommandResult e = _port.runCommand(db, concern.getCommand());

		Object foo = e.get("err");
		if (foo == null)
			return new WriteResult(e, concern);

		int code = -1;
		if (e.get("code") instanceof Number)
			code = ((Number) e.get("code")).intValue();
		String s = foo.toString();
		if (code == 11000 || code == 11001 || s.startsWith("E11000")
				|| s.startsWith("E11001"))
			throw new MongoException.DuplicateKey(code, s);
		throw new MongoException(code, s);
	}

	public WriteResult say(String namespace, OutMessage m, WriteConcern concern)
			throws MongoException {
		String[] names = StringUtils.splitAtFirst(namespace, ".");
		try {
			_port.say(m);
			if (concern.callGetLastError()) {
				return _checkWriteError(names[0], concern);
			} else {
				return new WriteResult(names[0], _port, concern);
			}
		} catch (IOException ioe) {
			_error(ioe);

			if (concern.raiseNetworkErrors())
				throw new MongoException.Network("can't say something", ioe);

			CommandResult res = new CommandResult();
			res.put("ok", false);
			res.put("$err", "NETWORK ERROR");
			return new WriteResult(res, concern);
		} catch (MongoException me) {
			throw me;
		} catch (RuntimeException re) {
			throw re;
		}

	}

	public Response call(String namespace, OutMessage m) throws MongoException {
		return call(namespace, m, 2);
	}

	public Response call(String namespace, OutMessage m, int retries)
			throws MongoException {

		Response res = null;
		try {
			res = _port.call(m, namespace);
		} catch (IOException ioe) {
			String collName = StringUtils.splitAtFirst(namespace, ".")[2];
			boolean shoulRetry = _error(ioe)
					&& !collName.equals("$cmd") && retries > 0;
			if (shoulRetry) {
				return call(namespace, m, retries - 1);
			}
			throw new MongoException.Network("can't call something", ioe);
		} catch (RuntimeException re) {
			throw re;
		}

		ServerError err = res.getError();

		if (err != null && err.isNotMasterError()) {
			_pickCurrent();
			if (retries <= 0) {
				throw new MongoException(
						"not talking to master and retries used up");
			}
			return call(namespace, m, retries - 1);
		}

		return res;
	}

	public ServerAddress getAddress() {
		return _curAddress;
	}

	public List<ServerAddress> getAllAddress() {
		return _allHosts;
	}

	public String getConnectPoint() {
		return _curAddress.toString();
	}

	boolean _error(Throwable t) throws MongoException {
		if (_allHosts != null) {
			_logger.log(Level.WARNING, "replica set mode, switching master", t);
			_pickCurrent();
		}
		return true;
	}

	/**
	 * @return next to try
	 */
	@SuppressWarnings("unchecked")
	ServerAddress _addAllFromSet(DBObject o) {
		Object foo = o.get("hosts");
		if (!(foo instanceof List))
			return null;

		String primary = (String) o.get("primary");

		ServerAddress primaryAddress = null;

		synchronized (_allHosts) {
			for (Object x : (List) foo) {
				try {
					String s = x.toString();

					ServerAddress a = new ServerAddress(s);
					if (!_allHosts.contains(a))
						_allHosts.add(a);

					if (s.equals(primary)) {
						int i = _allHosts.indexOf(a);
						primaryAddress = _allHosts.get(i);
					}
				} catch (UnknownHostException un) {
					_logger.severe("unknown host [" + un + "]");
				}
			}
		}

		return primaryAddress;
	}

	void _pickInitial() throws MongoException {
		if (_curAddress != null)
			return;

		// we need to just get a server to query for ismaster
		_pickCurrent();

		try {
			_logger.info("current address beginning of _pickInitial: "
					+ _curAddress);

			DBObject im = isMasterCmd();

			ServerAddress other = _addAllFromSet(im);

			if (_isMaster(im))
				return;

			if (other != null) {
				_set(other, this.options);
				im = isMasterCmd();
				_addAllFromSet(im);
				if (_isMaster(im))
					return;

				_logger.severe("primary given was wrong: " + other
						+ " going to scan");
			}

			synchronized (_allHosts) {
				Collections.shuffle(_allHosts);
				for (ServerAddress a : _allHosts) {
					if (_curAddress == a)
						continue;

					_logger.info("remote [" + _curAddress + "] -> [" + a + "]");
					_set(a, this.options);

					im = isMasterCmd();
					_addAllFromSet(im);
					if (_isMaster(im))
						return;

					if (_allHosts.size() == 2)
						_logger.severe("switched to: " + a
								+ " but isn't master");
				}

				throw new MongoException("can't find master");
			}
		} catch (Exception e) {
			_logger.log(Level.SEVERE,
					"can't pick initial master, using random one", e);
		}
	}

	private void _pickCurrent() throws MongoException {
		if (_allHosts == null)
			throw new MongoException(
					"got master/slave issue but not in master/slave mode on the client side");

		synchronized (_allHosts) {
			Collections.shuffle(_allHosts);
			for (int i = 0; i < _allHosts.size(); i++) {
				ServerAddress a = _allHosts.get(i);
				if (a == _curAddress)
					continue;

				if (_curAddress != null) {
					_logger.info("switching from [" + _curAddress + "] to ["
							+ a + "]");
				}

				try {
					_set(a, this.options);
				} catch (IOException e) {
					throw new MongoException("can't switch address");
				}
				return;
			}
		}

		throw new MongoException("couldn't find a new host to swtich too");
	}

	private boolean _set(ServerAddress addr,MongoOptions options) throws IOException {
		if (_curAddress == addr)
			return false;
		_curAddress = addr;
		this._port = new DBPort(addr._addr,options);
		return true;
	}

	public String debugString() {
		StringBuilder buf = new StringBuilder("DBTCPConnector: ");
		if (_allHosts != null)
			buf.append("replica set : ").append(_allHosts);
		else
			buf.append(_curAddress).append(" ").append(_curAddress._addr);

		return buf.toString();
	}

	DBObject isMasterCmd() {
		String namespace = "admin.$cmd";
		Iterator<DBObject> i = _m._find(namespace, _isMaster, null, 0, 1, 0);
		if (i == null || !i.hasNext())
			throw new MongoException("no result for ismaster query?");
		DBObject res = i.next();
		if (i.hasNext())
			throw new MongoException("what's going on");

		return res;
	}

	boolean _isMaster(DBObject res) {
		Object x = res.get("ismaster");
		if (x == null)
			throw new IllegalStateException("ismaster shouldn't be null: "
					+ res);

		if (x instanceof Boolean)
			return (Boolean) x;

		if (x instanceof Number)
			return ((Number) x).intValue() == 1;

		throw new IllegalArgumentException("invalid ismaster [" + x + "] : "
				+ x.getClass().getName());
	}

	public void close() {
		_port.close();
	}

	private DBPort _port;
	private ServerAddress _curAddress;
	private final List<ServerAddress> _allHosts;
	private Mongo _m;
	final MongoOptions options;

	private final static DBObject _isMaster = new BasicDBObject("ismaster", 1);

}
