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

namespace Mung.Core {
	public static class MungedDataWriter {


		public static void WriteException(Exception ex, TextWriter writer) {
			writer.Write("###ERROR###\r\n");
			writer.Write(ex.Message + "\r\n" + ex.StackTrace);

			MungLog.LogException("MungConnection.WriteText", ex);
		}


		/// <summary>
		/// Reads data from the data set and writes it to the supplied stream.
		/// </summary>
		/// <param name="reader">Open reader that hasn't been read from yet</param>
		/// <param name="writer"></param>
		/// <returns>Number of rows written</returns>
		public static int Write(IDataReader reader, TextWriter writer) {
			try {
				bool isFirst = true;
				int rowCount = 0;
				while (reader.Read()) {

					if (isFirst) {
						//MungLog.Log.LogEvent("MungedDataWriter.Write", "Retreiving...");
						// Recycle the same array so we're not constantly allocating

						List<string> names = new List<string>();
						List<MungType> types = new List<MungType>();

						for (var i = 0; i < reader.FieldCount; i++) {
							names.Add(reader.GetName(i));
							types.Add(TypeConverter.FromDotNetType(reader[i].GetType()));
						}

						writer.Write(string.Join("\t", names));
						writer.Write("\n");


						writer.Write(string.Join("\t", types.Select(x => x.ToString())));
						writer.Write("\n");
						isFirst = false;
					}
					WriteRow(reader, writer);

					rowCount++;
					//if (rowCount % 1000 == 0) {
					//	MungLog.Log.LogEvent("MungedDataWriter.Write", string.Format("Wrote {0}th row", rowCount));
					//}

				}
				writer.Flush();

				return rowCount;
			} catch (Exception ex) {
				MungLog.LogException("MungedData.Write", ex);
				return -1;
			}
		}

		public static void WriteRow(IDataReader reader, TextWriter writer) {
			for (var i = 0; i < reader.FieldCount; i++) {
				writer.Write(Serialize(reader[i]));
				writer.Write("\t");
			}
			writer.Write("\n");
		}

		public static int Write(DataTable table, TextWriter writer) {
			List<string> names = new List<string>();
			List<MungType> types = new List<MungType>();

			string[] values = new string[table.Columns.Count];

			foreach (DataColumn col in table.Columns) {
				names.Add(col.ColumnName);
				types.Add(TypeConverter.FromDotNetType(col.DataType));

			}
			writer.Write(string.Join("\t", names));
			writer.Write("\n");
			writer.Write(string.Join("\t", types.Select(x => x.ToString())));
			writer.Write("\n");

			for (var i = 0; i < table.Rows.Count; i++) {
				for (var c = 0; c < values.Length; c++) {
					values[c] = Serialize(table.Rows[i][c]);
				}
				writer.Write(string.Join("\t", values));
				writer.Write("\n");
			}

			return table.Rows.Count;
		}

		public static string Serialize(object o) {
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

		private static string Escape(string unescaped) {

			//https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/LoadingBulkData
			return unescaped
				.Replace("\\", "\\" +"\\")		// '\' -> '\\'
				.Replace("\"", "\\" + "\"");		// '"' -> '""'
		}




	}
}
