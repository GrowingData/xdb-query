#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//
// This Source Code Form is subject to the terms of the Apache 
// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Data.SqlClient;
using System.Data.SQLite;
using Mung.Core.Connections.Vendors;
using Npgsql;

namespace Mung.Core {
	public static class MungConnectionFactory {

		public static MungDataConnection FromProvider(string provider, string name, string connectionString) {
			// Don't use the .Net provider names specifically, as they kinda tie us to
			// .net when we want to enable clients to be built in any language

			if (provider == "SqlServer") {
				return new SqlServerMungConnection(name, new SqlConnection(connectionString));
			}
			if (provider == "SQLite") {
				return new SQLiteMungConnection(name, new SQLiteConnection(connectionString));
			}

			if (provider == "PostgreSQL") {
				return new PostgreSqlMungConnection(name, new NpgsqlConnection(connectionString));
			}
			return null;
		}
	}
}
