using System;
using System.IO;
using System.Text;
using System.Data;
using LumenWorks.Framework.IO.Csv;


namespace Mung.Core {
	public class CsvDataContext : IMungDataContext {

		public IDataReader Reader { get { return _reader;  } }

		private CsvReader _reader = null;
		private StreamReader _stream;

		private bool _readTypes = false;
		private string[] _types;

		private CsvDataContext(string path, char seperator) {
			_stream = new StreamReader(File.OpenRead(path));
			_reader = new CsvReader(_stream, true, seperator);


		}

		public static CsvDataContext Execute(string path, char seperator) {
			return new CsvDataContext(path, seperator);
		}


		public void WriteRow(TextWriter warehouseStream) {
			MungedDataWriter.WriteRow(Reader, warehouseStream);
		}

		///// <summary>
		///// Writes the results in the reader to the specified text stream
		///// as a tab seperated text file.
		///// </summary>
		///// <param name="writer"></param>
		public void WriteStream(TextWriter writer) {
			MungedDataWriter.Write(Reader, writer);
		}


		public bool Read() {
			if (!_readTypes) {
				if (_reader.FieldCount == 1) {
					MungLog.LogEvent(LogSeverity.errors, "CsvDataContext", "Warning: The CSV Reader was only able to find 1 field in the first row, is your field seperator correct?");
				}
				_types = new string[_reader.FieldCount];
				for (var i = 0; i < _reader.FieldCount; i++) {
					_types[i] = _reader[i];
				}

				_readTypes = true;

				_reader.ReadNextRecord();
			}
			return _reader.ReadNextRecord();
		}

		public MungQuerySchema ImpliedSchema {
			get {
				MungQuerySchema schema = new MungQuerySchema(_reader);

				return schema;
			}
		}

		
		public void Dispose() {
			if (_stream != null) {
				_stream.Dispose();
			}
		}

		public object ValueAt(int index) {
			return _reader[index];
		}
	}
}
