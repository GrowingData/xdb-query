#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace Mung.Core {

	/// <summary>
	/// A disposable reference to a result set that is beign executed.
	/// This is to make it easier to return a IDbDataReader, which if returned
	/// normally makes it too easy to forget to close the Connection (since it
	/// can't be closed prior to the DataReader having read everything).
	/// </summary>
	public class RelationalDataContext : IDisposable, IMungDataContext {
		public IDbConnection Connection { get; private set; }
		public IDbCommand Command { get; private set; }
		public IDataReader Reader { get; private set; }

		private bool _leaveConnectionOpen = false;

		private RelationalDataContext(IDbConnection cn, string command, Dictionary<string, object> parameters, bool leaveConnectionOpen) {
			Connection = cn;
			_leaveConnectionOpen = leaveConnectionOpen;
			Execute(command, parameters);
		}

		public static IMungDataContext Execute(IDbConnection cn, string command, Dictionary<string, object> parameters) {
			return new RelationalDataContext(cn, command, parameters, false);
		}
		public static IMungDataContext Execute(IDbConnection cn, string command, Dictionary<string, object> parameters, bool leaveConnectionOpen) {
			return new RelationalDataContext(cn, command, parameters, leaveConnectionOpen);
		}

		private void Execute(string command, Dictionary<string, object> parameters) {

			Command = Connection.CreateCommand();
			Command.CommandText = command;
			if (parameters != null) {
				foreach (var kp in parameters) {
					var p = Command.CreateParameter();
					p.ParameterName = kp.Key;
					p.Value = kp.Value == null ? DBNull.Value : kp.Value;
					Command.Parameters.Add(p);
				}
			}
			Command.CommandTimeout = 0;
			Reader = Command.ExecuteReader();
		}

		public MungQuerySchema ImpliedSchema {
			get {
				return new MungQuerySchema(Reader);
			}
		}

		public object ValueAt(int index) {
			return Reader[index];
		}
		public bool Read() {
			return Reader.Read();
		}

		public void WriteRow(TextWriter warehouseStream) {
			MungedDataWriter.WriteRow(Reader, warehouseStream);
		}

		///// <summary>
		///// Writes the results in the reader to the specified text stream
		///// as a tab seperated text file.
		///// </summary>
		///// <param name="writer"></param>
		public void WriteStream(TextWriter writer) {
			MungedDataWriter.Write(Reader, writer);
		}

		///// <summary>
		///// Writes the results in the reader to the specified text stream
		///// as a tab seperated text file.
		///// </summary>
		///// <param name="writer"></param>
		//public void WriteStream(Stream output) {
		//	using (var sr = new StreamWriter(output)) {
		//		MungedDataWriter.Write(Reader, sr);
		//	}
		//}

		void IDisposable.Dispose() {

			if (Reader != null) {
				Reader.Dispose();
			}

			if (Command != null) {
				Command.Dispose();
			}
			if (!_leaveConnectionOpen) {
				if (Connection != null) {
					Connection.Dispose();
				}
			}
		}
	}
}
