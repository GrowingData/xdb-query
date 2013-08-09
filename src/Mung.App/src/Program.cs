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
	class Program {
		delegate void OperationDelegate(string[] args);

		private static void PrintInstructions(Dictionary<string, IConsoleCommand> operations) {
			StringBuilder output = new StringBuilder();

			output.AppendLine("");
			output.AppendLine("");
			output.AppendLine("usage: mung [--log <performance|info|errors|none>]");
			output.AppendLine("            <command> [<args>]");
			output.AppendLine("\r\n\r\n");
			output.AppendLine("Available commands:\r\n");

			foreach (var kp in operations) {
				output.AppendLine(kp.Value.Description);
			}
			Console.WriteLine(output.ToString());
		}

		static string[] ParseOptions(string[] args) {
			var operations = new Dictionary<string, Action<string>>(){
				{
					"--log", (p) => {
						LogSeverity severity;
						if (Enum.TryParse<LogSeverity>(p, out severity)){
							MungLog.SetLogThreshold(severity);
						}
					}
				}
			};
			var filtered = new List<string>();
			for (var i = 0; i < args.Length; i++) {
				if (operations.ContainsKey(args[i])) {
					operations[args[i]](args[++i]);
				} else {
					filtered.Add(args[i]);
				}

			}
			return filtered.ToArray();
		}

		static void Main(string[] args) {
			// Make sure we have a valid command before we go and connect to stuff
			// which takes time.
			var operations = new Dictionary<string, IConsoleCommand>(){
				{"update", new UpdateCommand()},
				{"run", new RunCommand()},
				{"csv", new CsvCommand()},
			};

			if (args.Length == 0) {
				MungLog.LogException("Select command", new Exception("No command selected"));
				PrintInstructions(operations);
				return;
			}



			Console.TreatControlCAsInput = true;


			try {
				AppEngine.Initialize();
			} catch (AggregateException ex) {
				// If you are unable to initialize, get the hell out of dodge.
				foreach (var x in ex.InnerExceptions) {
					MungLog.LogException("AppEngine.Initialize", x);
				}
				return;
			} catch (FileNotFoundException ex) {
				MungLog.LogException("AppEngine.Initialize", ex);
				return;
			}




			Console.CancelKeyPress += (object sender, ConsoleCancelEventArgs e) => {
				AppEngine.Terminate();
				e.Cancel = false;
			};
			try {
				var filteredArgs = ParseOptions(args);

				var opName = filteredArgs[0];

				IConsoleCommand cc;

				if (operations.TryGetValue(opName, out cc)) {

					//Task.Run(() => {
					cc.Execute(filteredArgs);
					//});

				} else {
					MungLog.LogException("Select command", new Exception(string.Format("Unknown command: {0}", opName)));
					PrintInstructions(operations);
				}

				// Quit 
				AppEngine.Terminate();
			} catch (Exception ex) {
				MungLog.LogException("app", ex);
				return;
			}
		}
	}
}
