#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//
// This Source Code Form is subject to the terms of the Apache 
// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;

namespace Mung.Core {
	/// <summary>
	/// A table sitting in the Mung Warehouse....
	/// </summary>
	public class MungSchema {
		private string _name;
		private List<MungTable> _tables;

		public string Name { get { return _name; } }

		public int TableCount { get { return _tables.Count; } }

		public MungTable GetTable(string name) {
			return _tables.FirstOrDefault(x => x.Name == name);
		}

		public MungSchema(IDataReader reader) {
			_tables = new List<MungTable>();
			_name = reader["schema_name"] as string;


			_tables.Add(new MungTable(this, reader));
			while (reader.Read()) {
				if (_name != reader["schema_name"] as string) {
					break;
				}

				_tables.Add(new MungTable(this, reader));
			}
		}
	}
}