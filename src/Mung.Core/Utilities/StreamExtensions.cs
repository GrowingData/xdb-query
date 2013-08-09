#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Mung.Core {
	public static class StreamExtensions {

		public static string ReadLineUTF8(this Stream stream) {
			List<byte> bytes = new List<byte>();
			int current;
			while ((current = stream.ReadByte()) != -1 && current != (int)'\n') {
				if (bytes.Count == 0
					&& current == 0xEF
					&& stream.ReadByte() == 0xBB
					&& stream.ReadByte() == 0xBF) {
					// UTF8 marker (http://en.wikipedia.org/wiki/Byte_order_mark)
					continue;
				}

				if (current != (int)'\r') {
					byte b = (byte)current;
					bytes.Add(b);
				}
			}
			return Encoding.UTF8.GetString(bytes.ToArray());
		}
	}
}
