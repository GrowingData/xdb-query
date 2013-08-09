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
	public class MungColumn {
		private string _name;
		private MungType _type;

		public string Name { get { return _name; } }
		public MungType Type { get { return _type; } }

		public MungColumn(string columnName, MungType type) {
			_name = columnName;
			_type = type;
        }

        public MungColumn(IDataReader reader) {
			_name = reader["column_name"] as string;
			var stringType = reader["column_type"] as string;


			switch (stringType) {
				case "bigint":
					_type = MungType.Integer;
					break;
				case "clob":
					_type = MungType.Varchar;
					break;
				case "timestamp":
					_type = MungType.DateTime;
					break;
				case "float":
					_type = MungType.Float;
					break;

				default:
					_type = MungType.Unknown;
					break;
			}
		}
	}
}