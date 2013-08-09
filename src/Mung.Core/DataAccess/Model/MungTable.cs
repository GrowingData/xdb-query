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


namespace Mung.Core {
	/// <summary>
	/// A table sitting in the Mung Warehouse....
	/// </summary>
	public class MungTable {
		private MungSchema _schema;
		private string _name;
		private List<MungColumn> _columns;

		public string Name { get { return _name; } }
		public int ColumnCount { get { return _columns.Count; } }

		public string GetColumnName(int index) {
			return _columns[index].Name;
		}

		// New virtual table
		public MungTable() {
		}

		public MungTable(MungSchema schema, IDataReader reader) {
			_schema = schema;
			_columns = new List<MungColumn>();
			_name = reader["table_name"] as string;
			_columns.Add( new MungColumn(reader));

			while (reader.Read()) {
				if (_name != reader["alias"] as string) {
					break;
				}
				_columns.Add(new MungColumn(reader));
			}

		}
	}
}