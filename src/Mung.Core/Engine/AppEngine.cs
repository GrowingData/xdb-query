#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Mung.Core {
	public static class AppEngine {
		private delegate void InitializationTask();

		public static void Initialize() {

			using (var perf = AppEngine.Time("AppEngine.Initialize")) {
				_connections = new ConnectionManager();


				//InitializeMonetDb();

				LoadConnections();
			}
		}

		private static void LoadConnections() {
			using (var perf = AppEngine.Time("AppEngine.LoadConnections")) {
				//_connections.TestConnections();
				foreach (var cn in _connections) {
					MungLog.LogEvent(LogSeverity.info, "Connections", string.Format("Connected to: {0}", cn.name));
				}
			}
		}

		//private static void InitializeMonetDb() {
		//	using (var perf = AppEngine.Time("AppEngine.InitializeMonetDb")) {

		//		var monetSettings = _connections.Settings.connections.FirstOrDefault(x => x.name == "mung");

		//		try {
		//			// Test the connection and start a new process if required
		//			using (var b = MungConnectionFactory.FromProvider(monetSettings.provider, monetSettings.name, monetSettings.connection_string)) {
		//				Connections.SetMungConnection(monetSettings);
		//			}
		//		} catch (Exception ex) {
		//			using (var perf2 = AppEngine.Time("AppEngine.InitializeMonetDb.StartProcess")) {
		//				_process = new MonetProcess();
		//				_process.Start();

		//				// Make sure that the connection works!
		//				using (var b = MungConnectionFactory.FromProvider(monetSettings.provider, monetSettings.name, monetSettings.connection_string)) {
		//					Connections.SetMungConnection(monetSettings);
		//				}
		//			}
		//		}


		//	}
		//}


		private static ConnectionManager _connections;
		//private static MonetProcess _process;

		public static ConnectionManager Connections {
			get {
				if (_connections == null) {
					throw new Exception("Please call \"WebEngine.Initialize\" prior to accessing connections");
				}
				return _connections;
			}
		}

		public static void Terminate() {

			using (var perf = AppEngine.Time("AppEngine.Terminate")) {
				//if (_process != null) {
				//	_process.Stop();
				//}
			}

		}

		private static IDictionary _loggingPersistence = new Hashtable();

		//public static MungDataConnection Warehouse { get { return Connections["mung"]; } }


		public static MungTimer Time(string name) {
			return new MungTimer(name, _loggingPersistence);
		}


	}
}