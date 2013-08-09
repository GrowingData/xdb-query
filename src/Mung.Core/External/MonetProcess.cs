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
using System.Diagnostics;

using System.Threading;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace Mung.Core {
	public class MonetProcess {
		private string _monetPath;
		private string _monetBinPath;
		private string _monetDataPath;

		private Process _process;
		private bool _hasErrorOutput=false;
		private bool _hasOutput = false;
		

		public MonetProcess() {

			string json = File.ReadAllText(PathManager.SettingsFile);
			Dictionary<string, string> settings = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);

			_monetPath = PathManager.Path + @"\MonetDB5-Win";
			_monetBinPath = _monetPath + @"\bin";



			_monetDataPath = Path.GetFullPath(settings["data-path"]);

			

		}

		public bool Start() {
			string server = string.Format(@"{0}\bin\mserver5.exe", _monetPath);
			string args = string.Format(" --set \"prefix={0}\" --config=\"{2}\\goose.conf\" --set \"exec_prefix={0}\" --dbpath={1}",
				_monetPath, _monetDataPath, PathManager.ConfigPath);

			string path = string.Format(@"{0}\bin;{0}\lib;{0}\lib\MonetDB5", _monetPath);

			var env = new ProcessStartInfo(server, args);
			env.EnvironmentVariables["PATH"] = path;
			env.CreateNoWindow = true;
			env.RedirectStandardOutput = true;
			env.RedirectStandardError = true;
			env.WorkingDirectory = _monetPath;
			env.UseShellExecute = false;


			_process = new Process();
			_process.StartInfo = env;
			_process.EnableRaisingEvents = true;
			_process.ErrorDataReceived += _process_ErrorDataReceived;
			_process.OutputDataReceived += _process_OutputDataReceived;
			_process.Start();

			_process.BeginErrorReadLine();
			_process.BeginOutputReadLine();

			while (!_hasOutput && !_hasErrorOutput) {
				Thread.Sleep(1);
			}

			return !_hasErrorOutput;
		
		}

		void _process_OutputDataReceived(object sender, DataReceivedEventArgs e) {
			Console.WriteLine("Monet: " + e.Data);
			_hasOutput=true;
		}

		void _process_ErrorDataReceived(object sender, DataReceivedEventArgs e) {
			Console.WriteLine("Monet Error: " + e.Data);
			_hasErrorOutput = true;
		}
		public bool Stop() {
			_process.Kill();
			return true;
		}

	}
}
