#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.IO;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;

namespace Mung.Core {

	public delegate void OnSchemaDiscoveredDelegate(MungQuerySchema schema);
	public delegate void OnBulkInsertRowsWrittenDelegate(long rows);
	public delegate void OnBulkInsertCompleteDelegate(long rows);

	public abstract class MungDataConnection : IDisposable {

		#region Events
		public event OnBulkInsertRowsWrittenDelegate OnBulkInsertRowsWritten;
		public event OnBulkInsertCompleteDelegate OnBulkInsertComplete;

		protected void CallBulkInsertRowsWritten(long rows) {
			if (OnBulkInsertRowsWritten != null) {
				OnBulkInsertRowsWritten(rows);
			}
		}

		protected void CallBulkInsertComplete(long rows) {
			if (OnBulkInsertComplete != null) {
				OnBulkInsertComplete(rows);
			}
		}
		#endregion


		protected string _name;

		public string Name { get { return _name; } }


		public abstract IMungDataContext MetaTables();
		public abstract IMungDataContext MetaColumns(string schema, string table);

		public abstract bool SchemaExists(string schema);
		public abstract bool CreateSchema(string schema);

		public abstract bool TableExists(string schema, string tableName);
		public abstract string CreateTable(string schema, string tableName, MungQuerySchema def);
		public abstract bool DropTable(string schema, string tableName);


		public abstract void Dispose();

		public abstract IMungDataContext Execute(string sql, Dictionary<string, object> parameters, bool leaveOpen);
		public IMungDataContext Execute(string sql, Dictionary<string, object> parameters) {
			return Execute(sql, parameters, false);
		}

		protected abstract string ParameterPlaceholder(string parameterName);


		protected abstract long DoBulkLoad(string schema, string name, IMungDataContext reader);


		#region virtual

		public virtual string Escape(string unescaped) {

			//https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/LoadingBulkData
			return unescaped
				.Replace("\\", "\\" + "\\")		// '\' -> '\\'
				.Replace("\"", "\\" + "\"");		// '"' -> '""'
		}

		public virtual string Serialize(object o) {
			if (o is DateTime) {
				return ((DateTime)o).ToString("yyyy-MM-dd HH':'mm':'ss");

			}
			if (o == DBNull.Value) {
				return "NULL";
			}

			if (o is string) {

				// Strings are escaped 
				return "\"" + Escape(o.ToString()) + "\"";

			}
			return o.ToString();

		}

		#endregion


		public string BulkLoad(string schema, string table, IMungDataContext reader) {
			try {
				if (!SchemaExists(schema)) {
					CreateSchema(schema);
				}

				// Don't drop the table just because its a bulk insert, as we will 
				// probably save time by keeping existing rows

				string actualTable = table;
				if (!TableExists(schema, table)) {
					actualTable = CreateTable(schema, table, reader.ImpliedSchema);
				}

				// The actual name of the table might change, since a temp table 
				// in SQL Server is "#<tablename>"

				MungLog.LogEvent(LogSeverity.info, "Connection.BulkLoad", 
					string.Format("Bulk load started for {0} on {1}", table, this._name));
				DoBulkLoad(schema, actualTable, reader);
				MungLog.LogEvent(LogSeverity.info, "Connection.BulkLoad",
					string.Format("Bulk load completed for {0} on {1}", table, this._name));

				return actualTable;
			} catch (Exception ex) {
				MungLog.LogException("BulkLoad", ex);
				return null;
			}
		}


		public DbParameter CreateParameter(DbCommand cmd, string name, object value) {
			var p = cmd.CreateParameter();
			p.ParameterName = this.ParameterPlaceholder(name);
			p.Value = value;
			return p;
		}

		public bool WriteText(RelationalDataContext reader, TextWriter output) {
			MungedDataWriter.Write(reader.Reader, output);
			return true;
		}


	}
}
