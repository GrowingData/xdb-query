#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using Npgsql;
using System;
namespace Mung.Core.Connections.Vendors {
	public class PostgreSqlMungConnection : AdoDotNetConnection {
		private NpgsqlConnection _realConnection;

		public PostgreSqlMungConnection(string name, NpgsqlConnection connection)
			: base(name, connection) {
			_realConnection = connection;
		}

		protected override string ParameterPlaceholder(string parameterName) {
			return ":" + parameterName;
		}
		protected override long DoBulkLoad(string schema, string table, IMungDataContext context) {
			throw new NotImplementedException();
		}
	}
}
