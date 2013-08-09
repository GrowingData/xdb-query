#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//
// This Source Code Form is subject to the terms of the Apache 
// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Collections;
using System.Collections.Generic;

namespace Mung.Core.Connections {
	public abstract class AdoDotNetConnection : MungDataConnection {
		const string PARAM_SCHEMA_NAME = "schema_name";
		const string PARAM_TABLE_NAME = "table_name";

		protected DbConnection _connection;

		public AdoDotNetConnection(string name, DbConnection connection) {
			_connection = connection;
			_name = name;
			Open();
		}

		public override void Dispose() {
			_connection.Dispose();
		}

		protected virtual string MetaColumnsSql {
			get {
				return @"
				    SELECT table_catalog, table_schema, table_name, column_name, data_type
					FROM information_schema.columns
					ORDER BY table_catalog, table_schema, table_name, ordinal_position
				";
			}
		}

		private bool Open() {
			using (var perf = AppEngine.Time("AdoDotNetConnection.OpenConnection")) {
				int retries = 0;
				while (_connection.State != ConnectionState.Open && retries < 10) {
					try {
						_connection.Open();

					} catch (Exception ex) {
						MungLog.LogException("AdoDotNetConnection.OpenConnection", ex);

						retries++;
						System.Threading.Thread.Sleep(100);
					}
				}

				if (_connection.State != ConnectionState.Open) {
					MungLog.LogException("AdoDotNetConnection.OpenConnection", new InvalidOperationException("Connection failed after 10 retries"));
					return false;

				}
			}
			return true;
		}


		public override IMungDataContext MetaTables() {
			var sql = @"
				SELECT table_schema, table_name
				FROM information_schema.tables
				ORDER BY table_schema, table_name
				";

			return Execute(sql, null);
		}

		public override IMungDataContext MetaColumns(string schema, string table) {

			var sql = string.Format(@"
				    SELECT table_schema, table_name, column_name, data_type
					FROM information_schema.columns
					WHERE	(table_schema = {0} OR {0} IS NULL)
					AND		table_name = {1}
					ORDER BY table_catalog, table_schema, table_name, ordinal_position
				",
				 this.ParameterPlaceholder("table_schema"),
				 this.ParameterPlaceholder("table_name"));

			var parameters = new Dictionary<string, object>() {
				{ParameterPlaceholder(PARAM_SCHEMA_NAME), schema},
				{ParameterPlaceholder(PARAM_TABLE_NAME), table},
			};

			return Execute(sql, parameters);
		}

		public override bool SchemaExists(string schema) {
			using (var perf = AppEngine.Time("AdoDotNetConnection.SchemaExists")) {
				if (string.IsNullOrEmpty(schema)) {
					return true;
				}
				var sql = string.Format(@"
				    SELECT 1
					FROM information_schema.schemata
					WHERE	table_schema = {0}
				",
					 this.ParameterPlaceholder(PARAM_SCHEMA_NAME));

				var parameters = new Dictionary<string, object>() {
				{ParameterPlaceholder(PARAM_SCHEMA_NAME), schema}
			};

				using (var ctx = Execute(sql, parameters, true)) {
					if (ctx.Read()) {
						return true;
					}
				}
			}
			return false;
		}

		public override bool CreateSchema(string schema) {
			var sql = string.Format("CREATE SCHEMA \"{0}\";", schema);
			using (var ctx = Execute(sql, null)) {
				ctx.Read();
			}
			return true;
		}

		public override bool TableExists(string schema, string table) {
			using (var perf = AppEngine.Time("AdoDotNetConnection.TableExists")) {

				var sql = string.Format(@"
				    SELECT 1
					FROM information_schema.tables
					WHERE	(table_schema = {0} OR {0} IS NULL)
					AND		table_name = {1}
				",
					 this.ParameterPlaceholder(PARAM_SCHEMA_NAME),
					 this.ParameterPlaceholder(PARAM_TABLE_NAME));

				var parameters = new Dictionary<string, object>() {
				{ParameterPlaceholder(PARAM_SCHEMA_NAME), schema},
				{ParameterPlaceholder(PARAM_TABLE_NAME), table},
			};

				using (var ctx = Execute(sql, parameters, true)) {
					if (ctx.Read()) {
						return true;
					}
				}
			}
			return false;
		}


		public override string CreateTable(string schema, string tableName, MungQuerySchema def) {
			using (var perf = AppEngine.Time("AdoDotNetConnection.CreateTable")) {
				var sql = CreateTableScript(schema, tableName, def);

				using (var ctx = Execute(sql, null, true)) {
					ctx.Read();
				}

				return QualifiedName(schema, tableName);
			}
		}

		public override bool DropTable(string schema, string table) {
			using (var perf = AppEngine.Time("AdoDotNetConnection.DropTable")) {
				var sql = string.Format(@"DROP TABLE {0};", QualifiedName(schema, table));
				using (var ctx = Execute(sql, null, true)) {
					ctx.Read();
				}
				return true;
			}

		}

		protected string QualifiedName(string schema, string table) {
			if (string.IsNullOrEmpty(schema)) {
				return string.Format("\"{0}\"", table);
			} else {
				return string.Format("\"{0}\".\"{1}\"", schema, table);
			}
		}

		protected string CreateTableScript(string schema, string tableName, MungQuerySchema reader) {
			string[] columnDefinitions = new string[reader.Columns.Count];
			for (var i = 0; i < reader.Columns.Count; i++) {
				columnDefinitions[i] = string.Format("\t{0} {1}", reader.Columns[i].Name, TypeConverter.ANSISqlType(reader.Columns[i].Type));
			}

			return string.Format("CREATE TABLE {0} (\r\n{1}\r\n);\r\n",
				QualifiedName(schema, tableName),
				string.Join(",\r\n", columnDefinitions));

		}


		public override IMungDataContext Execute(string sql, Dictionary<string, object> parameters, bool leaveOpen) {
			using (var perf = AppEngine.Time("AdoDotNetConnection.Execute")) {
				if (Open()) {
					return RelationalDataContext.Execute(_connection, sql, parameters, leaveOpen);
				} else {
					throw new Exception("Unable to connect to database");
				}
			}
		}



	}
}
