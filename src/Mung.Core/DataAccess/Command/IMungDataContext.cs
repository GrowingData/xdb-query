using System;
using System.IO;
using System.Data.Common;
using System.Data;


namespace Mung.Core {
	public interface IMungDataContext : IDisposable {
		IDataReader Reader { get; }

		void WriteRow(TextWriter warehouseStream);
		void WriteStream(TextWriter writer);
		MungQuerySchema ImpliedSchema { get; }

		bool Read();

		object ValueAt(int index);
	}
}
