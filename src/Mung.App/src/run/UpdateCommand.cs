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
	public class UpdateCommand : IConsoleCommand {

		public int Execute(string[] args) {

			var scripts = Directory.GetFiles(PathManager.UserPath, args[1]);

			using (var perf = AppEngine.Time("ImportCommand")) {
				MungLog.LogEvent(LogSeverity.info, "ImportCommand", string.Format("Running {0} scripts...", scripts.Length));
				//Parallel.ForEach(scripts, script => {
				foreach (var script in scripts) {
					DateTime start = DateTime.Now;

					MungLog.LogEvent(LogSeverity.info, "ImportCommand", string.Format("Running script {0}", script));
					RunScript(script);

					MungLog.LogEvent(LogSeverity.info, "ImportCommand", string.Format("Running script {0} complete ({1}ms)", script, DateTime.Now.Subtract(start).TotalMilliseconds));
				}
				//});
			}



			return -1;
		}

		public static void RunScript(string scriptName) {
			var cmd = MungQuery.Parse(scriptName);

			if (cmd.OutputConnection == null) {
				MungLog.LogException("UpdateCommand", new Exception("Script: {0} does not have an @output(<connection_name>) specified.  Where do I put these results?"));
			}

			try {
				using (var reader = cmd.Execute()) {
					using (var cn = cmd.OutputConnection) {
						cn.BulkLoad(cmd.Project, cmd.Name, reader);
						MungLog.LogEvent(LogSeverity.info, "UpdateCommand.RunScript", "Success: " + scriptName);
					}
				}
			} catch (Exception ex) {
				MungLog.LogException("UpdateCommand.RunScript", ex);
				MungLog.LogException("UpdateCommand.RunScript", new Exception("Running of script " + scriptName + ", failed\r\n", ex));
			}


		}

		public string Description {
			get {
				return
@"	update <search>: Runs scripts matching <search>, updating destination tables.
		Where the @output(<connection>.<table>) is supplied in the query, data will be
		copied there.
		Where NO @output(<connection>.<table>) is mentioned, data will be imported  
		into <parent_dir>.<filename> using the MUNG connection.";
			}
		}



	}
}
