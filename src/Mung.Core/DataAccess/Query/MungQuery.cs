#region Copyright (C) Mung.IO
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
		private const string KW_SUBQUERY = "@(";
		private const string KW_TOLERANT_SUBQUERY = "@?(";
		private const string KW_INPUT = "@input(";
		private const string KW_OUTPUT = "@output(";

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
		public string ConnectionName { get; private set; }
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




		private enum ParseState {
			None = 0,
			InAtQuery = 1
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

			var bracketCount = 0;
			var state = ParseState.None;
			var start = -1;
			var tolerant = false;

			for (var i = 0; i < RawQuery.Length; i++) {
				var c = RawQuery[i];

				switch (state) {
					case ParseState.InAtQuery:
						if (c == '(') {
							bracketCount++;
							break;
						}
						if (c == ')') {
							bracketCount--;
							if (bracketCount == 0) {
								var atQuery = RawQuery.Substring(start, i - start);

								// Lets try to work out if this is a file?
								if (File.Exists(atQuery)) {
									// Ok, our query is actually a file reference to another script
									// so lets load it up!
									atQuery = File.ReadAllText(atQuery);
								}


								// We want to look back a little to find out what the context for this
								// subquery is, as if its a JOIN or a FROM, then we need to actually
								// put the results into a temp table
								var last = PreviousToken(start - 1 - KW_SUBQUERY.Length).ToLowerInvariant();
								var requiresTable = last == "join" || last == "from";

								var sub = new MungQuery(this, atQuery, new StartEnd(start, i), requiresTable, tolerant);

								SubQueries.Add(sub);
								newQuery.Append(sub.Identifier);
								state = ParseState.None;
							}
						}
						break;
					case ParseState.None:
						if (LookAhead(KW_SUBQUERY, RawQuery, ref i)) {
							// This is the starting point for an @(<subquery>)
							start = i;
							bracketCount = 1;
							state = ParseState.InAtQuery;

							break;
						}
						if (LookAhead(KW_TOLERANT_SUBQUERY, RawQuery, ref i)) {
							// This is the starting point for an @(<subquery>)
							start = i;
							bracketCount = 1;
							state = ParseState.InAtQuery;
							tolerant = true;
							break;

						}

						if (LookAhead(KW_INPUT, RawQuery, ref i)) {
							InputConnection = ReadConnection(ref i);
							break;
						}
						if (LookAhead(KW_OUTPUT, RawQuery, ref i)) {
							OutputConnection = ReadConnection(ref i);
							break;
						}

						newQuery.Append(RawQuery[i]);
						break;
				}
			}
			ParsedQuery = newQuery.ToString();
			RewrittenQuery = ParsedQuery;

			if (InputConnection == null) {
				throw new Exception("Input connection not specified");
				//InputConnection = AppEngine.Warehouse;
			}
		}

		public void RewriteQuery(string identified, string newQuery) {
			lock (_sync) {
				RewrittenQuery = RewrittenQuery.Replace(identified, newQuery);
			}

		}

		private MungDataConnection ReadConnection(ref int i) {
			int usingStart = i;
			// Lets get the connection that we are supposed to be using
			while (RawQuery[i] != ')' && i < RawQuery.Length) {
				i++;
			}
			ConnectionName = RawQuery.Substring(usingStart, i - usingStart).Trim();

			var connection = AppEngine.Connections[ConnectionName];
			if (connection == null) {
				throw new InvalidOperationException(string.Format("Unknown connection \"{0}\".  Please add it to your connections.json file.", ConnectionName));
			}
			return connection;
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



		public void Dispose() {
			foreach (var child in SubQueries) {
				child.Dispose();
			}


			if (Parent != null && this.RequiresTable) {
				try {
					if (Parent.ConnectionName == null){
						throw new Exception(string.Format("Unable to clean up table '{0}', as no connection is specified", TableName));
					}
					using (var cn = AppEngine.Connections[Parent.ConnectionName]) {
						cn.DropTable(null, TableName);
					}
				} catch (Exception ex) {
					MungLog.LogException("MungQuery.Dispose", ex);
				}
			}

		}

	}
}
