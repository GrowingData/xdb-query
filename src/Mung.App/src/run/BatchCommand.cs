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
	public class BatchCommand : IConsoleCommand {

		private string TableExpression(string schema, string tablename) {
			if (!string.IsNullOrEmpty(schema)) {
				return schema + "." + tablename;
			} else {
				return tablename;
			}
		}

		public int Execute(string[] args) {
			if (args.Length <= 1) {
				Console.Write(Description);
				return -1;
			}

			var fileName = args[1];

			var script = File.ReadAllLines(fileName);

			Parallel.ForEach(script, line => RunBatchCommand(line));

			return -1;
		}


		public static void RunBatchCommand(string line) {
			var args = line.Split(' ');
			try {
				var filteredArgs = Program.ParseOptions(args);
				var opName = filteredArgs[0];
				IConsoleCommand cc;

				if (Program.Operations.TryGetValue(opName, out cc)) {
					//Task.Run(() => {
					cc.Execute(filteredArgs);
					//});

				}

			} catch (Exception ex) {
				MungLog.LogException("app", ex);
				return;
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
