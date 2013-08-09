#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.IO;
using System.Diagnostics;


namespace Mung.Core {
	public enum LogSeverity {
		performance = 0,
		info = 1,
		errors = 2,
		none = 3
	}

	public static class MungLog {


		private static LogSeverity _consoleSeverityThreshold = LogSeverity.info;

		private static readonly object _lockEventLog = new object();
		private static readonly object _lockExceptionLog = new object();

		private static DateTime _initTime;

		static MungLog() {

			_initTime = DateTime.Now;
		}
		public static void SetLogThreshold(LogSeverity sev) {
			
			_consoleSeverityThreshold = sev;
		}


		public static void LogEvent(LogSeverity severity, string where, string message) {

			lock (_lockEventLog) {

				string output = string.Format("T+{0}s\t{1}\t{2}{3}",
					(int)DateTime.Now.Subtract(_initTime).TotalSeconds,
					where,
					message,
					Environment.NewLine);

				File.AppendAllText(PathManager.EventLogPath, output);

				if ((int)severity >= (int) _consoleSeverityThreshold) {
					Console.Write(output);
				}
				Debug.Write(output);
			}

		}
		public static void LogException(string where, Exception ex) {

			lock (_lockExceptionLog) {
				string output = string.Format("ERROR: T+{0}s\t{1}{2}{3}{2}{4}{2}------------{2}",
					(int)DateTime.Now.Subtract(_initTime).TotalSeconds,
					where,
					Environment.NewLine,
					ex.Message,
					ex.StackTrace);

				File.AppendAllText(PathManager.ExceptionLogPath, output);

				if ((int) LogSeverity.errors >= (int)_consoleSeverityThreshold) {
					Console.Error.WriteLine(string.Format("Error: {0}\r\n{1}", ex.Message, ex.StackTrace));
				}

				
				Debug.Write(output);
			}

		}
	}
}
