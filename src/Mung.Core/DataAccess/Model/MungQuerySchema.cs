#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;


namespace Mung.Core {
	/// <summary>
	/// A table sitting in the Mung Warehouse....
	/// </summary>
	public class MungQuerySchema {
		private List<MungColumn> _columns;

		public List<MungColumn> Columns { get { return _columns; } }

		public string GetColumnName(int index) {
			return _columns[index].Name;
		}

		// New virtual table
		public MungQuerySchema(MungedDataReader reader) {
			_columns = new List<MungColumn>();

			for (var i = 0; i < reader.ColumnCount; i++) {
				var c = new MungColumn(reader.ColumnNames[i], reader.ColumnTypes[i]);
				_columns.Add(c);
			}

		}


		public MungQuerySchema(IDataReader reader) {

			_columns = new List<MungColumn>();

			var schema = reader.GetSchemaTable();
			var columnDefinitions = new List<string>();
			foreach (DataRow row in schema.Rows) {
				var name = row["ColumnName"] as string;
				var dotNetType = row["DataType"] as Type;
				var mungType = TypeConverter.FromDotNetType(dotNetType);

				//var sqlType = TypeConverter.SqlServerType(mungType);

				var c = new MungColumn(name, mungType);
				_columns.Add(c);
			}

		}

		public void WriteHeader(Stream writer) {

			var headerBuffer = UTF8Encoding.UTF8.GetBytes(string.Join("\t", Columns.Select(_ => _.Name)) + "\n");
			var typesBuffer = UTF8Encoding.UTF8.GetBytes(string.Join("\t", Columns.Select(_ => _.Type.ToString())) + "\n");
			writer.Write(headerBuffer, 0, headerBuffer.Length);
			writer.Write(typesBuffer, 0, typesBuffer.Length);
		}
	}
}