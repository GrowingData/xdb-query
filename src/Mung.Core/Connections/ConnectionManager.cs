using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion


using System.Collections;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Mung.Core {
	public class ConnectionNotFoundException : KeyNotFoundException {
		public ConnectionNotFoundException(string name)
			: base(name) {
		}
	}

	public class ConnectionSettings {

		public List<ConnectionDefinition> connections;
	}

	public class ConnectionDefinition {
		public string name;
		public string provider;
		public string connection_string;
	}

	public class ConnectionManager : IEnumerable<ConnectionDefinition> {
		private readonly object _lockConnections = new object();
		private ConnectionSettings _settings = null;

		public ConnectionSettings Settings { get { return _settings; } }

		private Dictionary<string, ConnectionDefinition> _connectionsByName;


		MungDataConnection _local;

		public ConnectionManager() {

			// Create a connection for our local in memory connection
			var mung = new ConnectionDefinition() {
				connection_string = "Data Source=mung.db",
				name = "mung",
				provider = "SQLite"
			};

			_local = MungConnectionFactory.FromProvider(mung.provider, mung.name, mung.connection_string);

			Reload();
		}


		public MungDataConnection Connection {
			get {
				return _local;
			}
		}

		public void SetMungConnection(ConnectionDefinition connection) {
			_connectionsByName["mung"] = connection;
		}

		public List<MungDataConnection> Match(string pattern) {
			var wildcard = new Wildcard(pattern);
			return _connectionsByName
				.Values
				.Where(x => wildcard.IsMatch(x.name))
				.Select(jc => MungConnectionFactory.FromProvider(jc.provider, jc.name, jc.connection_string))
				.ToList();
		}


		public MungDataConnection this[string name] {
			get {
				if (name == "mung") {
					return _local;
				}
				lock (_lockConnections) {
					if (_connectionsByName == null) {
						var ex = new InvalidOperationException("Connection manager has not been initialized, call Reload()");
						MungLog.LogException("ConnectionManager.Get", ex);
						throw ex;
					}
					ConnectionDefinition jc;
					if (_connectionsByName.TryGetValue(name, out jc)) {
						return MungConnectionFactory.FromProvider(jc.provider, jc.name, jc.connection_string);
					}
					MungLog.LogException("ConnectionManager.Get", new ConnectionNotFoundException(name));
					return null;
				}
			}
		}
		//public void TestConnections() {

		//	Parallel.ForEach(_settings.connections, cn => {
		//		if (cn.name != "mung") {
		//			using (var b = MungConnectionFactory.FromProvider(cn.provider, cn.name, cn.connection_string)) {
		//				if (b != null) {
		//					// The connection is valid
		//					_connectionsByName[b.Name] = cn;
		//				} else {
		//					MungLog.LogEvent(LogSeverity.errors, "ConnectionManager.Load", String.Format("Unable to connect to: {0}, connection will be unavailble", cn.name));
		//				}
		//			}
		//		}
		//	});
		//}


		public void Reload() {
			lock (_lockConnections) {
				_connectionsByName = new Dictionary<string, ConnectionDefinition>();

				if (!File.Exists(PathManager.ConnectionsFile)) {
					throw new FileNotFoundException(string.Format("Unable to find connections.json\r\n\tExpected path: '{0}'.", PathManager.ConnectionsFile));
				}


				string json = File.ReadAllText(PathManager.ConnectionsFile);
				_settings = JsonConvert.DeserializeObject<ConnectionSettings>(json);

				foreach (var cn in _settings.connections) {
					_connectionsByName[cn.name] = cn;
				}

			}
		}

		public IEnumerator<ConnectionDefinition> GetEnumerator() {
			return _connectionsByName.Values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return _connectionsByName.Values.GetEnumerator();
		}

	}
}