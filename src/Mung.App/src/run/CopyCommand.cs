#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Mung.Core;

namespace Mung.App {
	public class CopyCommand : IConsoleCommand {

		private string TableExpression(string schema, string tablename) {
			if (!string.IsNullOrEmpty(schema)) {
				return schema + "." + tablename;
			} else {
				return tablename;
			}
		}

		public int Execute(string[] args) {
			if (args.Length <= 4) {
				Console.Write(Description);
				return -1;
			}

			var sourceConnectionName = args[1];
			var sourceTableName = args[2];
			var destConnectionName = args[3];
			var destTableName = args[4];

			var sourceConnection = AppEngine.Connections[sourceConnectionName];
			if (sourceConnection == null) {
				Console.WriteLine(string.Format("Unable to find source connection \"{0}\".", sourceConnectionName));
				return -1;
			}

			var destConnection = AppEngine.Connections[destConnectionName];
			if (destConnection == null) {
				Console.WriteLine(string.Format("Unable to find source connection \"{0}\".", destConnectionName));
				return -1;
			}

			var sourceCommand = string.Format("SELECT * FROM {0}", sourceTableName);

			// Copy into a temporary table over the network
			var tempTableName = "_t" + Guid.NewGuid().ToString().Replace("-", "").ToLowerInvariant();
			MungQuerySchema schema = null;
			using (var reader = sourceConnection.Execute(sourceCommand, null)) {
				schema = reader.ImpliedSchema;
				destConnection.BulkLoad(null, tempTableName, reader);
			}

			string destSchemaName = null;
			if (destTableName.Contains(".")) {
				destSchemaName = destTableName.Split('.')[0];
				destTableName = destTableName.Split('.')[1];
			}

			// Check to see if the table exists, empty it if it does and create a new one
			// if it doesnt
			if (destConnection.TableExists(destSchemaName, destTableName)) {
				destConnection.EmptyTable(destSchemaName, destTableName);
			} else {
				destConnection.CreateTable(destSchemaName, destTableName, schema);
			}

			// Now do a really simple INSERT INTO for the local copy
			var insertCommand = string.Format(@"
				INSERT INTO {0} ({1})
					SELECT {1} FROM {2}",
						TableExpression(destSchemaName, destTableName),
						string.Join(",", schema.Columns.Select(x => x.Name)),
						tempTableName);

			destConnection.Execute(insertCommand, null);

			// Delete the temporary table
			destConnection.DropTable(null, tempTableName);

			return -1;
		}

		public static long RunScript(string scriptName, Dictionary<string, object> parameters) {
			using (var cmd = new MungQuery(scriptName)) {

				if (cmd.OutputConnection == null) {
					return -1;
				}
				using (var reader = cmd.Execute(parameters)) {
					if (reader != null) {
						reader.WriteStream(Console.Out);
						return 0;
					}
				}
				var name = Path.GetFileNameWithoutExtension(scriptName);
				MungLog.LogException("RunScript", new Exception(string.Format("\"{0}\" failed to run.", name)));
				return -1;
			}

		}

		public string Description {
			get {
				return
@"
Usage:
	mung copy <source_connection> <source_table> <dest_connection> <dest_table>:
	Copies the table from the source connection into the destination connection,
	initially copies the data to '_tmp_<dest_table>', then either truncates 
	<dest_table> (if it exists) or creates <dest_table>, and copies the data into
	<dest_table> from the local copy.  

	This means that even over a slow network, the actual time where an existing 
	table is locked or contains partial data is minimized.  

	Parameters are given in the format: <param1_name>=<value1> <param2_name>=<value2>
";
			}
		}



	}
}
