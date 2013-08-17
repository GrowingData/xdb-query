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


	/// <summary>
	/// A class to rewrite a query that needs to be distributed across
	/// multiple connections, with results being unioned in the parent
	/// context.
	/// </summary>

	public static class MungDistributedQuery {

		/// <summary>
		/// Take a query that looks like:
		/// -------------
		///		@[mung-sqlite-*](
		///			SELECT * FROM tbl
		///		)
		/// -------------
		/// And Rewrite it to look like:
		/// 
		///		SELECT * 
		///		FROM (@[mung-sqlite-01](
		///			SELECT * FROM tbl
		///		)
		///		
		///		UNION ALL 
		///		
		///		SELECT * 
		///		FROM (@[mung-sqlite-02](
		///			SELECT * FROM tbl
		///		)
		///		
		///		UNION ALL 
		///		
		///		SELECT * 
		///		FROM (@[mung-sqlite-03](
		///			SELECT * FROM tbl
		///		)
		///		
		///		UNION ALL 
		///		
		///		SELECT * 
		///		FROM (@[mung-sqlite-04](
		///			SELECT * FROM tbl
		///		)
		/// </summary>
		/// <param name="query"></param>
		/// <returns></returns>
		public static string Rewrite(string query) {
			return query;
		
		}

	}
}
