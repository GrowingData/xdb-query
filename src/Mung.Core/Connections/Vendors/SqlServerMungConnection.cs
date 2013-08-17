#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//
// This Source Code Form is subject to the terms of the Apache 
// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using System;
using Mung.Core.Connections;


namespace Mung.Core.Connections.Vendors {
	public class SqlServerMungConnection : AdoDotNetConnection {
		private SqlConnection _realConnection;

		public SqlServerMungConnection(string name, SqlConnection connection)
			: base(name, connection) {
			_realConnection = connection;
		}
		protected override string ParameterPlaceholder(string parameterName) {
			return "@" + parameterName;
		}

		protected string CreateTableScriptWithMungId(string schema, string tableName, MungQuerySchema reader) {
			List<string> defs = new List<string>();
			for (var i = 0; i < reader.Columns.Count; i++) {
				defs.Add(string.Format("\t{0} {1}", reader.Columns[i].Name, TypeConverter.SqlServerType(reader.Columns[i].Type)));
			}
			// Add it to the end, so the ordinal based column mapping in the SqlBulkCopy still 
			// works without having to add a column mapping object.
			defs.Add("mung_id INT NOT NULL IDENTITY(1,1)");

			return string.Format("CREATE TABLE {0} (\r\n{1}\r\n);\r\n",
				QualifiedName(schema, tableName),
				string.Join(",\r\n", defs));

		}



		public override bool EmptyTable(string schema, string tableName) {
			var tableExpression = tableName;
			if (!string.IsNullOrEmpty(schema)) {
				tableExpression = string.Format("{0}.{1}", schema, tableName);
			}

			var sql = string.Format(@"
				TRUNCATE TABLE {0}
				", tableExpression);

			Execute(sql, null);
			return true;

		}

		public override string CreateTable(string schema, string tableName, MungQuerySchema def) {
			using (var perf = AppEngine.Time("SqlServerMungConnection.CreateTable")) {
				var sql = CreateTableScriptWithMungId(schema, tableName, def);

				var qualified = QualifiedName(schema, tableName);
				// We also need to create a clustered index, since SQL Azure doesn't
				// support tables without one (yet!)

				var index = string.Format("\r\nCREATE CLUSTERED INDEX IX_{0} ON {1} (mung_id)",
					tableName,
					qualified
				);


				using (var ctx = Execute(sql + index, null, true)) {
					ctx.Read();
				}

				return qualified;
			}
		}


		protected override long DoBulkLoad(string schema, string table, IMungDataContext context) {
			using (var perf = AppEngine.Time("SqlServerMungConnection.CreateTable")) {
				string qualifiedName = table;
				if (!string.IsNullOrEmpty(schema)) {
					qualifiedName = schema + "." + table;
				}

				var copy = new SqlBulkCopy(_realConnection, SqlBulkCopyOptions.TableLock, null);
				copy.DestinationTableName = qualifiedName;
				copy.BatchSize = 1000;
				copy.BulkCopyTimeout = 9999999;
				copy.EnableStreaming = true;
				copy.NotifyAfter = 1000;


				long rows = 0;
				copy.SqlRowsCopied += (object sender, SqlRowsCopiedEventArgs e) => {
					rows = e.RowsCopied;
					CallBulkInsertRowsWritten(rows);
				};

				copy.WriteToServer(context.Reader);


				CallBulkInsertComplete(rows);


				return rows;
			}
		}

	}
}
