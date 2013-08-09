#region Copyright (C) Mung.IO
// Copyright (C) 2013-2013 Mung.IO
// http://mung.io
//

// License, v. 2.0. If a copy of the APL was not distributed with this
// file, You can obtain one at http://www.apache.org/licenses/
#endregion

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Data.Common;

using System.IO;

namespace Mung.Core {
	public class MetaDataNotReadException : InvalidOperationException {
		public MetaDataNotReadException(string message)
			: base(message) {
		}
	}
	public class MetaDataException : InvalidOperationException {
		public MetaDataException(string message)
			: base(message) {
		}
	}

	public class DataNotReadException : InvalidOperationException {
		public DataNotReadException(string message)
			: base(message) {
		}
	}
	public class ColumnNotFoundException : KeyNotFoundException {
		public ColumnNotFoundException(string columnName)
			: base(columnName) {
		}
	}
	public class ReadPassedEofException : KeyNotFoundException {
		public ReadPassedEofException(string message)
			: base(message) {
		}
	}
	public class InvalidRowException : InvalidOperationException {
		public InvalidRowException(int rowNumber, int columnNumber, string line)
			: base(string.Format("Error reading file: \r\nRow:	{0}\r\nColumn: {1}\r\nLine: {2}", rowNumber, columnNumber, line)) {
		}
	}

	public class MungedDataReader : IDisposable {
		private readonly StreamReader _reader;
		private readonly Stream _rawStream;

		private readonly bool _valid;
		private bool _eof;
		private bool _hasRead;
		private int _lineNumber;

		private char _fieldSeperator = '\t';

		private bool _isError;
		private string _errorMessage;

		public bool IsError { get { return _isError; } }
		public string ErrorMessage { get { return _errorMessage; } }



		private List<string> _names;
		private Dictionary<string, int> _namesToIndexes;
		private string[] _rowData;

		private List<MungType> _types;

		public MungedDataReader(Stream stream) {
			_rawStream = stream;
			_valid = ReadMetaData();
			_reader = new StreamReader(stream);
		}

		public MungedDataReader(Stream stream, char fieldSeperator) {
			_rawStream = stream;
			_valid = ReadMetaData();
			_reader = new StreamReader(stream);
			_fieldSeperator = fieldSeperator;
		}

		public int LineNumber { get { return _lineNumber; } }

		public List<MungType> ColumnTypes {
			get {
				return _types;
			}
		}

		public List<string> ColumnNames {

			get {
				return _names;
			}
		}

		public int ColumnCount {
			get {
				return _names.Count;
			}
		}

		private void CheckMetaData() {
			if (_names == null || _namesToIndexes == null) {
				throw new MetaDataNotReadException("Metadata has not been read, please check the error log.");
			}
		}
		private void CheckDataRead() {
			if (!_hasRead || _rowData == null) {
				throw new DataNotReadException("Unable to read row without calling Read()");
			}
		}

		public MungQuerySchema Schema { get { return new MungQuerySchema(this); } }

		public string this[string columnName] {
			get {
				
				CheckMetaData();
				CheckDataRead();


				var idx = -1;

				Debug.Assert(_namesToIndexes != null, "_namesToIndexes != null");
				if (_namesToIndexes.TryGetValue(columnName, out idx)) {
					Debug.Assert(_rowData != null, "_rowData != null");
					return _rowData[idx];
				}

				throw new ColumnNotFoundException(columnName);
			}
		}

		public string[] Row { get { return _rowData; } }

		public string this[int idx] {
			get {
				CheckDataRead();

				Debug.Assert(_rowData != null);

				if (idx >= 0 && idx < ColumnCount) {
					return _rowData[idx];
				}

				throw new ColumnNotFoundException(string.Format("Column index {0} not found", idx));
			}
		}


		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public static MungedDataReader Open(string filepath) {

			return new MungedDataReader(File.OpenRead(filepath));
		}

		public bool Read() {
			CheckMetaData();
			Debug.Assert(_reader != null);

			if (_eof) {
				throw new ReadPassedEofException("Unable to read past the end of the file");
			}

			var row = _reader.ReadLine();
			_lineNumber++;
			if (string.IsNullOrEmpty(row)) {
				_eof = true;
				return false;
			}

			_rowData = row.Split(_fieldSeperator);

			if (_rowData.Length != ColumnCount) {
				var columns = _rowData.Length;
				_rowData = null;
				throw new InvalidRowException(_lineNumber, columns, row);
			}
			_hasRead = true;

			return true;
		}


		private bool ReadMetaData() {
			Debug.Assert(_rawStream != null);

			var namesLine = _rawStream.ReadLineUTF8();
			if (namesLine == "###ERROR###") {
				_isError = true;

				_errorMessage = new StreamReader(_rawStream).ReadToEnd();
                return false;
            }

			var typesLine = _rawStream.ReadLineUTF8();

			_lineNumber = 1;

			if (string.IsNullOrEmpty(namesLine)) {
				MungLog.LogException("MungedDataReader.ReadMetaData", new MetaDataException("Column names line is empty"));
				return false;
			}
			if (string.IsNullOrEmpty(typesLine)) {
				MungLog.LogException("MungedDataReader.ReadMetaData", new MetaDataException("Type names line is empty"));
				return false;
			}


			var names = namesLine.Split('\t');
			var types = typesLine.Split('\t');

			if (names.Length != types.Length) {
				MungLog.LogException("MungedDataReader.ReadMetaData",
					new MetaDataException("Length of Column Names != Length of Types"));
				return false;
			}

			_names = new List<string>();
			_namesToIndexes = new Dictionary<string, int>();

			for (var i = 0; i < names.Length; i++) {
				var name = names[i].Trim() ;

				// Check to make sure we dont have an invalid name
				if (string.IsNullOrEmpty(name)) {
					MungLog.LogException("MungedDataReader.ReadMetaData",
						new MetaDataException(string.Format("Column names at index {0} is empty or null name", i)));
					return false;
				}
				_names.Add(name);
				_namesToIndexes[name] = i;
			}


			_types = new List<MungType>();
			foreach (var t in types) {
				var parsed = TypeConverter.FromMungType(t);
				if (parsed == MungType.Unknown) {
					MungLog.LogException("MungedDataReader.ReadMetaData", new MetaDataException("Unknown type: " + t));
					_types = null;
					return false;
				}
				_types.Add(parsed);
			}

			return true;
		}


		private void Dispose(bool disposing) {
			if (!disposing || _reader == null) {
				return;
			}
			_reader.Close();
			_reader.Dispose();
		}
	}
}