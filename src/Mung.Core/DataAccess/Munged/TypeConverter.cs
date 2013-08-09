#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using Newtonsoft.Json.Linq;

namespace Mung.Core {
	public enum MungType {
		Unknown,
		Integer,
		Varchar,
		DateTime,
		Float
	}
	public static class TypeConverter {

		public static MungType FromMungType(string type) {
			var parsed = MungType.Unknown;

			if (Enum.TryParse(type, out parsed)) {
				return parsed;
			}
			return MungType.Unknown;
		}

		public static MungType FromSqlType(string type) {
			if (type == "varchar" || type == "nvarchar") {
				return MungType.Varchar;
			}
			if (type == "tinyint" || type == "smallint" || type == "int" || type == "bigint") {
				return MungType.Integer;
			}
			if (type == "smalldatetime" || type == "datetime") {
				return MungType.DateTime;
			}
			if (type == "float" || type == "decimal" || type == "money") {
				return MungType.Float;
			}

			return MungType.Varchar;
		}

		public static MungType FromDotNetType(Type type) {
			if (type == typeof(string)) {
				return MungType.Varchar;
			}
			if (type == typeof(int) || type == typeof(long) || type == typeof(uint) || type == typeof(ulong)) {
				return MungType.Integer;
			}
			if (type == typeof(DateTime)) {
				return MungType.DateTime;
			}
			if (type == typeof(float) || type == typeof(double) || type == typeof(decimal)) {
				return MungType.Float;
			}

			return MungType.Varchar;
		}
		public static MungType FromJsonType(JTokenType type) {
			if (type == JTokenType.String) {
				return MungType.Varchar;
			}

			if (type == JTokenType.Float) {
				return MungType.Float;
			}

			if (type == JTokenType.Integer) {
				return MungType.Integer;
			}
			if (type == JTokenType.Date) {
				return MungType.DateTime;
			}
			return MungType.Varchar;
		}

		public static string MonetSqlType(MungType type) {
			if (type == MungType.DateTime) {
				return "TIMESTAMP";
			}
			if (type == MungType.Float) {
				return "FLOAT";
			}
			if (type == MungType.Integer) {
				return "BIGINT";
			}
			if (type == MungType.Varchar) {
				return "TEXT";
			}
			return "TEXT";

		}
		public static string ANSISqlType(MungType type) {
			if (type == MungType.DateTime) {
				return "TIMESTAMP";
			}
			if (type == MungType.Float) {
				return "FLOAT";
			}
			if (type == MungType.Integer) {
				return "BIGINT";
			}
			if (type == MungType.Varchar) {
				return "VARCHAR(8000)";
			}
			return "TEXT";

		}

		public static string SqlServerType(MungType type) {
			if (type == MungType.DateTime) {
				return "DATETIME";
			}
			if (type == MungType.Float) {
				return "FLOAT";
			}
			if (type == MungType.Integer) {
				return "BIGINT";
			}
			if (type == MungType.Varchar) {
				return "NVARCHAR(MAX)";
			}
			return "NVARCHAR(MAX)";

		}

	}
}
