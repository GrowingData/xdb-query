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
	public class RunCommand : IConsoleCommand {

		public int Execute(string[] args) {

			var scriptPath = System.IO.Path.GetFullPath(args[1]);

			if (!File.Exists(scriptPath)) {
				MungLog.LogException("RunCommand.Execute",
					new FileNotFoundException(string.Format("Unable to find script at: \r\n\t{0}", scriptPath)));

				return -1;
			}

			// Check parameters that may have been passed in
			var parameters = new Dictionary<string, object>();
			for (var i = 2; i < args.Length; i++) {
				if (args[i].Contains("=")) {
					var parts = args[i].Split('=');
					parameters.Add(parts[0], parts[1]);
				}
			}

			using (var perf = AppEngine.Time("ScriptCommand.Execute")) {

				DateTime start = DateTime.Now;
				//MungLog.LogEvent(LogSeverity.Info, "ScriptCommand", string.Format("{0} Started", args[1]));
				RunScript(scriptPath, parameters);
				//MungLog.LogEvent(LogSeverity.Info, "ScriptCommand", string.Format("{0} Completed ({1}ms)", args[1], DateTime.Now.Subtract(start).TotalMilliseconds));

			}
			return -1;
		}

		public static long RunScript(string scriptName, Dictionary<string, object> parameters) {
			using (var cmd = new MungQuery(scriptName)) {

				//if (cmd.OutputConnection == null) {
				//	return -1;
				//}
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
@"	run <filename> [<params>]: Runs the query found in filename, writing output to stdout.
		Parameters are given in the format: <param1_name>=<value1> <param2_name>=<value2>";
			}
		}



	}
}
