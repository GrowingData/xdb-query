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
using System.Web;
using System.Collections;
using System.Diagnostics;

namespace Mung.Core {
	public class MungTimer : IDisposable {
		public const string PERSISTENCE_KEY = "mung-timer";

		public class Counter {
			public double TotalMilliseconds;
			public int Count;
			public string Stack;

			public void Add(MungTimer log) {
				Count++;
				TotalMilliseconds += log._time;


				Stack = Environment.StackTrace;

			}
		}

		public static double TotalTime(string counterName, IDictionary persistence) {
			Counter counter;
			var counters = PerformanceCounters(persistence);

			if (counters != null && counters.TryGetValue(counterName, out counter)) {
				return counter.TotalMilliseconds;
			} else {
				return 0;
			}
		}
		
		public static int Count(string counterName, IDictionary persistence) {
			Counter counter;
			var counters = PerformanceCounters(persistence);

			if (counters != null && counters.TryGetValue(counterName, out counter)) {
				return counter.Count;
			} else {
				return 0;
			}
		}


		public static Dictionary<string, Counter> PerformanceCounters(IDictionary persistence) {
			if (persistence != null) {
				return persistence[PERSISTENCE_KEY] as Dictionary<string, Counter>;
			} else {
				return null;
			}
		}

		private void Increment(MungTimer logger) {
			if (_persistence != null) {
				if (_persistence != null) {
					Dictionary<string, Counter> counters;
					if (_persistence.Contains(PERSISTENCE_KEY)) {
						counters = _persistence[PERSISTENCE_KEY] as Dictionary<string, Counter>;
					} else {
						counters = new Dictionary<string, Counter>();
						_persistence[PERSISTENCE_KEY] = counters;
					}

					if (counters.ContainsKey(logger._name)) {
						counters[logger._name].Add(logger);
					} else {
						counters[logger._name] = new Counter() {
							TotalMilliseconds = logger._time,
							Count = 1,
							Stack = Environment.StackTrace
						};
					}
				}

			}

		}


		private DateTime _startTime;
		private double _time;
		private string _name;
		private IDictionary _persistence;


		public MungTimer(string name, System.Collections.IDictionary persistence) {
			_persistence = persistence;
			_startTime = DateTime.UtcNow;
			_name = name;

		}

		public void Dispose() {
			_time = DateTime.UtcNow.Subtract(_startTime).TotalMilliseconds;
			Increment(this);

			MungLog.LogEvent(LogSeverity.performance, _name, string.Format("{0}ms", _time));
		}
	}
}
