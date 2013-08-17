﻿#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace Mung.Core {

	public class StartEnd {
		public int Start { get; private set; }
		public int End { get; private set; }
		public int Length { get { return End - Start; } }

		public StartEnd(int start, int end) {
			Start = start;
			End = end;
		}

		public string Replace(string body, string replaceWith) {

			//var fullStart = start - 2;
			//query = query.Remove(fullStart, 1 + i - fullStart);
			//query = query.Insert(fullStart, value);

			string replaced = body.Remove(Start, 1 + Length);
			return replaced.Insert(Start, replaceWith);

		}
	}

	/// <summary>
	/// A mung Query may correspond to a single connection,
	/// but may have multiple subqueries, each of which may 
	/// correspond to a different connection.  
	/// </summary>

	public class MungQuery : IDisposable {
		private const string KW_SUBQUERY_CONNECTION_START = "@[";
		private const string KW_SUBQUERY_CONNECTION_END = "]";
		private const string KW_SUBQUERY_QUERY_START = "(";
		private const string KW_SUBQUERY_QUERY_END = ")";

		private const string KW_TOLERANT_SUBQUERY_CONNECTION_START = "@?[";


		private const string KW_OUTPUT_CONNECTION_START = "@output[";
		private const string KW_OUTPUT_CONNECTION_END = "]";

		public string Identifier { get; private set; }


		public bool TolerantOfErrors { get; private set; }

		public List<MungQuery> SubQueries { get; private set; }
		public MungQuery Parent { get; private set; }

		/// <summary>
		///	Where this query has a parent query, track where it
		///	starts and finishes so it can be rewritten.
		/// </summary>
		public StartEnd ParentPosition { get; private set; }

		public MungDataConnection InputConnection { get; private set; }
		public MungDataConnection OutputConnection { get; private set; }

		public bool HasChildError { get; set; }
		public string RawQuery { get; private set; }
		public string ParsedQuery { get; private set; }
		public string RewrittenQuery { get; private set; }
		public string InputConnectionName { get; private set; }
		public string OutputConnectionName { get; private set; }
		public string Project { get; private set; }
		public string Name { get; private set; }

		public string TableName { get { return "_mung_" + Identifier; } }

		public bool RequiresTable { get; private set; }

		private object _sync = new object();

		public MungQuery(string filePath) {
			HasChildError = false;
			if (File.Exists(filePath)) {
				Identifier = Guid.NewGuid().ToString().Replace("-", "").ToLower();
				SubQueries = new List<MungQuery>();


				// Absolute path, which is all good
				Project = PathManager.ProjectNameFromFilePath(filePath);
				Name = Path.GetFileNameWithoutExtension(filePath);

				RawQuery = File.ReadAllText(filePath);


				Parse();
			} else {
				throw new FileNotFoundException(string.Format("Unable to find file, path needs to be absoulte (path: {0}).", filePath));
			}

		}

		public MungQuery(string project, string query) {
			HasChildError = false;
			Identifier = Guid.NewGuid().ToString().Replace("-", "").ToLower();
			SubQueries = new List<MungQuery>();

			RawQuery = query;
			Project = project;

			Parse();
		}

		/// <summary>
		/// Constructor for building a subquery @("query")
		/// </summary>
		/// <param name="project"></param>
		/// <param name="query"></param>
		/// <param name="position"></param>
		/// <param name="requiresTable"></param>
		private MungQuery(MungQuery parent, string query, StartEnd position, bool requiresTable, bool tolerant) {
			HasChildError = false;
			Parent = parent;
			Identifier = Guid.NewGuid().ToString().Replace("-", "").ToLower();
			SubQueries = new List<MungQuery>();

			RawQuery = query;
			Project = parent.Project;
			ParentPosition = position;
			RequiresTable = requiresTable;
			TolerantOfErrors = tolerant;

			Parse();
		}


		private string PreviousToken(int from) {
			var buffer = new StringBuilder();

			for (var i = from - 1; i >= 0; i--) {
				if (!char.IsWhiteSpace(RawQuery[i])) {
					buffer.Insert(0, RawQuery[i]);
				} else {
					return buffer.ToString();
				}
			}
			return buffer.ToString();
		}

		private void Parse() {
			var newQuery = new StringBuilder();
			var previousToken = new StringBuilder();

			for (var i = 0; i < RawQuery.Length; i++) {
				var c = RawQuery[i];

				// Sub query: @[<connection-expression>](<query>)
				if (LookAhead(KW_SUBQUERY_CONNECTION_START, RawQuery, ref i)) {
					i = ReadSubQuery(i, false);
				}


				// Fault tolerant sub query: @?[<connection-expression>](<query>)
				if (LookAhead(KW_TOLERANT_SUBQUERY_CONNECTION_START, RawQuery, ref i)) {
					i = ReadSubQuery(i, true);

				}

				// Output connection: @output[<connection-expression>]
				if (LookAhead(KW_OUTPUT_CONNECTION_START, RawQuery, ref i)) {
					OutputConnectionName = ReadUntil(KW_OUTPUT_CONNECTION_END, RawQuery, ref i);
					OutputConnection = AppEngine.Connections[OutputConnectionName];
				}

				newQuery.Append(RawQuery[i]);
			}
			ParsedQuery = newQuery.ToString();
			RewrittenQuery = ParsedQuery;

			if (InputConnection == null) {
				throw new Exception("Input connection not specified");
			}
		}



		private int ReadSubQuery(int i, bool faultTolerant) {
			var start = i - KW_SUBQUERY_CONNECTION_START.Length;

			// Lets check if before this is a "from" or "join"
			var last = PreviousToken(i - 1 - KW_SUBQUERY_CONNECTION_START.Length).ToLowerInvariant();
			var requiresTable = last == "join" || last == "from";

			// Did we just see a @[<connection-expression>]
			InputConnectionName = ReadUntil(KW_SUBQUERY_CONNECTION_END, RawQuery, ref i);
			InputConnection = AppEngine.Connections[InputConnectionName];

			// Now read the actual query...
			var atQuery = ReadUntilBalanced(KW_SUBQUERY_QUERY_END, KW_SUBQUERY_QUERY_START, RawQuery, ref i);

			// Lets try to work out if this is a file?
			if (File.Exists(atQuery)) {
				// Ok, our query is actually a file reference to another script
				// so lets load it up!
				atQuery = File.ReadAllText(atQuery);
			}

			var sub = new MungQuery(this, atQuery, new StartEnd(start, i), requiresTable, faultTolerant);
			SubQueries.Add(sub);
			return i;
		}

		public void RewriteQuery(string identified, string newQuery) {
			lock (_sync) {
				RewrittenQuery = RewrittenQuery.Replace(identified, newQuery);
			}

		}


		public IMungDataContext Execute() {
			return Execute(null, null);
		}


		public IMungDataContext Execute(Dictionary<string, object> parameters) {
			return Execute(null, parameters);
		}


		public IMungDataContext Execute(MungQuery parent, Dictionary<string, object> parameters) {


			if (SubQueries.Count > 0) {
				using (var kids = AppEngine.Time("MungQuery.Execute.Children")) {

					Parallel.ForEach(SubQueries, child => {
						child.Execute(this, parameters);
					});
				}
				// If one of our sub queries has an error, then there is nothing
				// that we can do, so return before trying to execute the query.
				if (HasChildError) {
					return null;
				}

			}


			if (parent == null) {
				using (var kids = AppEngine.Time("MungQuery.Execute.Parent")) {
					// No parent, so we can just execute the text against the connection
					return InputConnection.Execute(RewrittenQuery, parameters);
				}

			} else {
				// If the parent query is not null, then we won't actually be returning anything
				// but we might be re-writing the query, or creating a temp table, bulk inserting into it
				// then updating the reference.
				try {
					if (this.RequiresTable) {
						// Execute the query and insert the results into a new temp table
						// and update the parent query to point to it.

						using (var ctx = InputConnection.Execute(RewrittenQuery, parameters)) {
							parent.InputConnection.BulkLoad(null, TableName, ctx);
							parent.RewriteQuery(Identifier, TableName);
							return null;
						}
					} else {
						// When it doesn't require a table, we can just rewrite the query with
						// actual values, which is pretty awesome
						using (var ctx = InputConnection.Execute(RewrittenQuery, parameters)) {
							var values = new List<string>();

							while (ctx.Read()) {
								var value = ctx.ValueAt(0);
								var serialized = parent.InputConnection.Serialize(value);
								values.Add(serialized);
							}
							var data = string.Join(", ", values);

							parent.RewriteQuery(Identifier, "(" + data + ")");

							return null;
						}

					}
				} catch (Exception ex) {
					if (TolerantOfErrors) {
						MungLog.LogException("MungQuery.Execute(SubQuery)", new Exception(" (Continuing): \r\n" + FailedQueryMessage(ex, RewrittenQuery)));
						parent.RewriteQuery(Identifier, "NULL");
						return null;
					} else {

						MungLog.LogException("MungQuery.Execute(SubQuery)", new Exception(FailedQueryMessage(ex, RewrittenQuery)));
						parent.HasChildError = true;
						return null;
					}
				}


			}

		}

		public string FailedQueryMessage(Exception ex, string rewrittenQuery) {
			return string.Format(
@"
{0}
------------------
{1}
------------------
", ex.Message, rewrittenQuery.Replace("\n", "\n\t"));
		}


		/// <summary>
		/// Returns true when "searchFor" starts a position "pos" within
		/// "searchIn" and increments "pos" by the length of searchFor.  If
		/// searchFor does not exist, "pos" will be untouched.
		/// </summary>
		/// <param name="searchFor"></param>
		/// <param name="searchIn"></param>
		/// <param name="pos"></param>
		/// <returns></returns>
		protected bool LookAhead(string searchFor, string searchIn, ref int pos) {
			if (searchFor.Length + pos >= searchIn.Length) {
				return false;
			}
			for (var i = 0; i < searchFor.Length; i++) {
				if (searchFor[i] != searchIn[i + pos]) {
					return false;
				}
			}
			pos += searchFor.Length;
			return true;
		}

		protected string ReadUntil(string searchFor, string searchIn, ref int pos) {
			StringBuilder sb = new StringBuilder();

			while (pos < searchIn.Length - searchFor.Length) {
				if (searchIn.Substring(pos, searchFor.Length) == searchFor) {
					return sb.ToString();
				}

				sb.Append(searchIn[pos]);
				pos++;
			}

			// Didn't find it.
			return null;
		}

		/// <summary>
		/// Lets you find everything between say "{" and "}", except that we allow
		/// ")" if they are proceeded by a "(" (exceptFor)
		/// </summary>
		/// <param name="searchFor"></param>
		/// <param name="exceptFor"></param>
		/// <param name="searchIn"></param>
		/// <param name="pos"></param>
		/// <returns></returns>
		protected string ReadUntilBalanced(string searchFor, string exceptFor, string searchIn, ref int pos) {
			StringBuilder sb = new StringBuilder();
			int balancer = 0;
			while (pos < searchIn.Length - searchFor.Length) {

				if (balancer == 0 && searchIn.Substring(pos, searchFor.Length) == searchFor) {
					return sb.ToString();
				}

				if (searchIn.Substring(pos, searchFor.Length) == exceptFor) {
					balancer++;
				}

				sb.Append(searchIn[pos]);
				pos++;
			}

			// Didn't find it.
			return null;
		}

		public void Dispose() {
			foreach (var child in SubQueries) {
				child.Dispose();
			}


			if (Parent != null && this.RequiresTable) {
				try {
					if (Parent.InputConnectionName == null) {
						throw new Exception(string.Format("Unable to clean up table '{0}', as no connection is specified", TableName));
					}
					using (var cn = AppEngine.Connections[Parent.InputConnectionName]) {
						cn.DropTable(null, TableName);
					}
				} catch (Exception ex) {
					MungLog.LogException("MungQuery.Dispose", ex);
				}
			}

		}

	}
}
