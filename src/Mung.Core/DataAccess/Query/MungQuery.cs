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

		public static MungQuery Parse(string filePath) {
			var query = new MungQuery(filePath);

			if (query.InputConnectionName == null && query.SubQueries.Count==1) {
				// If no input connection is set, then this query isn't really
				// a query, as we have no context, so return the child which will
				// have it all set properly.

				var sub = query.SubQueries[0];
				sub.Parent = null;
				sub.ParentPosition = null;
				return sub;
			}

			return query;
		}

		private MungQuery(string filePath) {
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

		private MungQuery(string project, string query) {
			HasChildError = false;
			Identifier = Guid.NewGuid().ToString().Replace("-", "").ToLower();
			SubQueries = new List<MungQuery>();

			RawQuery = query;
			Project = project;

			Parse();



		}

		/// <summary>
		/// Constructor for building a subquery @[{connection}]("query")
		/// </summary>
		/// <param name="project"></param>
		/// <param name="query"></param>
		/// <param name="position"></param>
		/// <param name="requiresTable"></param>
		private MungQuery(MungQuery parent, string query, StartEnd position, bool requiresTable, bool tolerant, string connectionName) {
			HasChildError = false;
			Parent = parent;
			Identifier = Guid.NewGuid().ToString().Replace("-", "").ToLower();
			SubQueries = new List<MungQuery>();

			RawQuery = query;
			Project = parent.Project;
			ParentPosition = position;
			RequiresTable = requiresTable;
			TolerantOfErrors = tolerant;

			InputConnectionName = connectionName;
			InputConnection = AppEngine.Connections[connectionName];
			if (InputConnection == null) {
				throw new Exception("Unable to find connection with name: " + connectionName);
			}

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
				if (RawQuery.LookAhead(KW_SUBQUERY_CONNECTION_START, ref i)) {
					var sub = ReadSubQuery(i, false);
					i = sub.ParentPosition.End + 1;
					newQuery.Append(sub.Identifier);

				}


				// Fault tolerant sub query: @?[<connection-expression>](<query>)
				if (RawQuery.LookAhead(KW_TOLERANT_SUBQUERY_CONNECTION_START, ref i)) {
					var sub = ReadSubQuery(i, true);
					i = sub.ParentPosition.End + 1;
					newQuery.Append(sub.Identifier);

				}

				// Output connection: @output[<connection-expression>]
				if (RawQuery.LookAhead(KW_OUTPUT_CONNECTION_START, ref i)) {
					OutputConnectionName = RawQuery.ReadUntil(KW_OUTPUT_CONNECTION_END, ref i);
					OutputConnection = AppEngine.Connections[OutputConnectionName];
				}
				if (i < RawQuery.Length) {
					newQuery.Append(RawQuery[i]);
				}
			}
			ParsedQuery = newQuery.ToString();
			RewrittenQuery = ParsedQuery;

			if (InputConnection == null && Parent != null) {
				throw new Exception("Input connection not specified");
			}
		}



		private MungQuery ReadSubQuery(int i, bool faultTolerant) {
			var start = i - KW_SUBQUERY_CONNECTION_START.Length;

			// Lets check if before this is a "from" or "join"
			var last = PreviousToken(i - 1 - KW_SUBQUERY_CONNECTION_START.Length).ToLowerInvariant();
			var requiresTable = last == "join" || last == "from";

			// Did we just see a @[<connection-expression>]
			string cnPattern = RawQuery.ReadUntil(KW_SUBQUERY_CONNECTION_END, ref i);

			// Wait a second here, does this match multiple connections?
			var connections = AppEngine.Connections.Match(cnPattern);
			if (connections.Count > 1) {
				// chose a host for this query randomly


			} else {
			
			}


			//InputConnection = AppEngine.Connections[InputConnectionName];

			// Now read the actual query...
			var atQuery = RawQuery.FindBetweenBalanced(KW_SUBQUERY_QUERY_START, KW_SUBQUERY_QUERY_END, ref i);

			// Lets try to work out if this is a file?
			if (File.Exists(atQuery)) {
				// Ok, our query is actually a file reference to another script
				// so lets load it up!
				atQuery = File.ReadAllText(atQuery);
			}

			var sub = new MungQuery(this, atQuery, new StartEnd(start, i), requiresTable, faultTolerant, cnPattern);
			SubQueries.Add(sub);


			return sub;
		}

		public void RewriteQuery(string identifier, string newQuery) {
			lock (_sync) {
				RewrittenQuery = RewrittenQuery.Replace(identifier, newQuery);
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


			if (parent==null) {
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


		public void Dispose() {
			foreach (var child in SubQueries) {
				child.Dispose();
			}


			if (Parent != null && this.RequiresTable) {
				try {
					if (Parent.InputConnectionName == null) {
						throw new Exception(string.Format("Unable to clean up table '{0}', as no connection is specified", TableName));
					}
					Parent.InputConnection.DropTable(null, TableName);
					//using (var cn = AppEngine.Connections[Parent.InputConnectionName]) {
					//	cn.DropTable(null, TableName);
					//}
				} catch (Exception ex) {
					MungLog.LogException("MungQuery.Dispose", ex);
				}
			}

		}

	}
}
