#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//
// This Source Code Form is subject to the terms of the Apache 
// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;

using Mung.Core.Connections;

namespace Mung.Core.Connections.Vendors {
	public class SQLiteMungConnection : AdoDotNetConnection {
		const string PARAM_SCHEMA_NAME = "schema_name";
		const string PARAM_TABLE_NAME = "table_name";

		private SQLiteConnection _realConnection;

		public SQLiteMungConnection(string name, SQLiteConnection connection)
			: base(name, connection) {
			_realConnection = connection;
		}

		protected override string ParameterPlaceholder(string parameterName) {
			return ":" + parameterName;
		}
		protected override long DoBulkLoad(string schema, string table, IMungDataContext context) {
			using (var perf = AppEngine.Time("SQLiteMungConnection.DoBulkLoad")) {
				string qualifiedName = table;
				if (!string.IsNullOrEmpty(schema)) {
					qualifiedName = schema + "." + table;
				}

				var copy = new SQLiteBulkInsert(_realConnection, qualifiedName);
				long rows = copy.WriteToServer(context.Reader);

				return rows;
			}
		}

		public override bool TableExists(string schema, string table) {
			using (var perf = AppEngine.Time("SQLiteMungConnection.TableExists")) {
				var schemaTable = _realConnection.GetSchema("Tables");

				var row = schemaTable.AsEnumerable()
					.FirstOrDefault(x => x.Field<string>("TABLE_NAME") == table
						&& (string.IsNullOrEmpty(schema) || x.Field<string>("TABLE_SCHEMA") == schema));

				if (row == null) {
					return false;
				} else {
					return true;
				}
			}

		}
	}
}
