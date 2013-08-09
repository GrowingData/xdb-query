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
using System.Text.RegularExpressions;

namespace Mung.Core {

	public static class StringHelpers {

		public static string Between(this string search, string prefix, string suffix) {

			int posA = search.IndexOf(prefix);
			if (posA > -1) {
				posA += prefix.Length;
				int posB = search.IndexOf(suffix, posA);
				if (posB > posA) {

					return search.Substring(posA, posB - posA);
				}
			}
			return null;

		}

		/// <summary>
		/// Gets all the instances of a string that are between to substrings.
		/// E.g. "Hello {0}, how are you today {1}".Betweens("{", "}") will return ["0", "1"]
		/// </summary>
		/// <param name="search"></param>
		/// <param name="prefix"></param>
		/// <param name="suffix"></param>
		/// <returns></returns>
		public static List<string> Betweens(this string search, string prefix, string suffix) {
			List<string> output = new List<string>();

			int posA = search.IndexOf(prefix);
			while (posA > -1) {
				posA += prefix.Length;
				int posB = search.IndexOf(suffix, posA);
				if (posB > posA) {

					output.Add( search.Substring(posA, posB - posA));
				}
				if (posB + suffix.Length + 1 >= search.Length) {
					break;
				}
				posA = search.IndexOf(prefix, posB + suffix.Length+ 1);
			}
			return output;

		}


		const string HTML_TAG_PATTERN = "<.*?>";

		public static string StripHTML(this string inputString) {
			return Regex.Replace
			  (inputString, HTML_TAG_PATTERN, string.Empty);
		}


		public static string StripNonAlpha(this string inputString) {
			if (inputString == null) {
				return null;
			}
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < inputString.Length; i++) {
				char c = inputString[i];
				if (Char.IsLetter(c)) {
					sb.Append(c);
				}
			}
			return sb.ToString();
		}
	}
}
