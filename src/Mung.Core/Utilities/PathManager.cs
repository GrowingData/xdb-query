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
using System.IO;

namespace Mung.Core {
	/// <summary>
	/// Summary description for PathManager
	/// </summary>
	public static class PathManager {
		public static string ConfigPath {
			get {
				return System.IO.Path.Combine(new string[] { Path, "config" });
			}
		}

		public static string SettingsFile {
			get {
				return System.IO.Path.Combine(new string[] { ConfigPath, "server.json" });
			}
		}

		//public static string BeaconsFile {
		//	get {
		//		return System.IO.Path.Combine(new string[] { ConfigPath, "beacons.json" });
		//	}
		//}

		public static string ConnectionsFile {
			get {
				return System.IO.Path.Combine(new string[] { UserPath, "connections.json" });
			}
		}

		public static string Path {
			get {
				// Walk the current path backwards until we find a "config" directory
				DirectoryInfo d = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);
				DirectoryInfo config = null;
				while (d != null && config == null) {
					config = d.GetDirectories().FirstOrDefault(x => x.Name == "config");
					d = d.Parent;
				}
				if (d == null || config.Parent == null) {
					throw new Exception("Unable to find \"config\" path, quitting.");
				}
				return config.Parent.FullName;
			}
		}

		private static string _userPath = null;
		public static string UserPath {
			get {
				if (_userPath == null) {
					_userPath = System.IO.Path.GetFullPath(".");
				}

				return _userPath;

			}
			set {
				_userPath = value;
			}
		}


		private static bool _checkedLogPath = false;


		public static string LogPath {
			get {

				string path = System.IO.Path.Combine(new string[] { UserPath, "log" });
				if (!_checkedLogPath) {
					if (!Directory.Exists(path)) {
						Directory.CreateDirectory(path);
					}
					_checkedLogPath = true;
				}
				return path;
			}
		}


		public static string EventLogPath {
			get {
				return System.IO.Path.Combine(new string[] { LogPath, "events.log" });
			}
		}


		public static string ExceptionLogPath {
			get {
				return System.IO.Path.Combine(new string[] { LogPath, "exceptions.log" });
			}
		}
		public static void CreateDirectoryRecursive(string startPath, string urlPath) {
			var parts = urlPath.Split('/');

			for (var i = 1; i < parts.Length; i++) {
				var pathParts = new string[] { startPath }.Union(parts.Take(i));
				var newPath = System.IO.Path.Combine(pathParts.ToArray());

				if (!Directory.Exists(newPath)) {
					Directory.CreateDirectory(newPath);
				}
			}
		}


		public static string ProjectNameFromFilePath(string filePath) {

			var directoryPath = System.IO.Path.GetDirectoryName(filePath);

			return new DirectoryInfo(directoryPath).Name.ToLowerInvariant();
		}

	}
}
