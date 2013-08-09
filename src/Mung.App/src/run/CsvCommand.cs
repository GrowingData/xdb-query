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
	public class CsvCommand : IConsoleCommand {

		public int Execute(string[] args) {
			if (args.Length <= 2) {
				Console.Write(Description);
				return -1;
			}

			var scriptPath = System.IO.Path.GetFullPath(args[1]);
			var tableName = Path.GetFileNameWithoutExtension(scriptPath);
			var connection = args[2];

			if (!File.Exists(scriptPath)) {
				MungLog.LogException("RunCommand.Execute",
					new FileNotFoundException(string.Format("Unable to find file to import at: \r\n\t{0}", scriptPath)));

				return -1;
			}

			// Check parameters that may have been passed in
			var parameters = new Dictionary<string, string>();
			for (var i = 3; i < args.Length; i++) {
				if (args[i].Contains("=")) {
					var parts = args[i].Split('=');
					parameters.Add(parts[0], parts[1]);
				}
			}

			var seperator = '\t';
			if (parameters.ContainsKey("-s")) {
				seperator = parameters["-s"][0]; 
			}
			var cn = AppEngine.Connections[connection];
			if (cn == null) {
				Console.WriteLine(string.Format("Unable to find connection \"{0}\".", connection));
				return -1;
			}

			using (var reader = CsvDataContext.Execute(scriptPath, seperator)) {
				cn.BulkLoad(null, tableName, reader);

			}


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
	mung csv <filepath> <connection> [<params>]: Loads the file given in <filepath> into 
	the connection specified by <connection>, using the file name as the table name.

	Parameters are given in the format: <param1_name>=<value1> <param2_name>=<value2>
";
			}
		}



	}
}
