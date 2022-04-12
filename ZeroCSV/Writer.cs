using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroCSV
{
    public abstract class Writer: IDisposable
    {
        public class WriteLineEventArgs : EventArgs
        {
            /// <summary>
            /// 文件编号
            /// </summary>
            public int FileNum { get;}
            /// <summary>
            /// 数据行编号
            /// </summary>
            public long FileRowNum { get; }
            /// <summary>
            /// 源数据行编号
            /// </summary>
            public long SourceRowNum { get; }
            /// <summary>
            /// 数据行字符串
            /// </summary>
            public string FileRowStr { get; }
            /// <summary>
            /// 文件前缀()
            /// </summary>
            public string FileNamePrefix { get; }
            public WriteLineEventArgs(string fileNamePrefix, int fileNum, long fileRowNum, long sourceRowNum, string rowStr)
            {
                this.FileNamePrefix = fileNamePrefix;
                this.FileRowStr = rowStr.Trim();
                this.FileNum = fileNum;
                this.FileRowNum = fileRowNum;
                this.SourceRowNum = sourceRowNum;
            }
        }
        public delegate void WriteLineHandler(WriteLineEventArgs args);
        public delegate void WriteFileEndHandler(WriteLineEventArgs args);
        public delegate void DisposedHandler();

        private string _ColSeparator = ",";
        private string _LineTerminator = "\r\n";
        private string _StrColQuote = "\"";
        private string[] _ColNames = null;
        private string[] _ColTypeNames = null;
        private bool _ColTypeNamesLoadingFinished = false;
        private Encoding _UseEncoding = Encoding.Default;
        private System.IO.DirectoryInfo _SaveDir = null;
        private string _SaveFilePrefix = "";
        private long _SingleFileRecordLimit = 0;
        private string _DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";
        private bool _NumberDisplayToStr = false;

        private long fileRowNum = 0;
        private long sourceRowNum = 0;
        private string lastRowStr = "";
        private int fileNum = 0;
        private int fileWriteEndNum = 0;
        private int colCount = 0;
        private bool isFirst = true;
        private bool hasDateTimeFormat = true;
        private bool hasCsvWriteHandler = false;
        private byte[] headerBytes = null;
        private System.IO.FileStream fileStream = null;
        private static object fileNumLock = new object();
        private static object rowNumLock = new object();

        #region -- public properties --
        protected string[] ColNames { get { return _ColNames; } set { _ColNames = value; } }
        public string ColSeparator { get { return _ColSeparator; } set { _ColSeparator = value; } }
        public string LineTerminator { get { return _LineTerminator; } set { _LineTerminator = value; } }
        public string StrColQuote { get { return _StrColQuote; } set { _StrColQuote = value; } }
        public Encoding UseEncoding { get { return _UseEncoding; } set { _UseEncoding = value; } }
        public System.IO.DirectoryInfo SaveDir { get { return _SaveDir; } set { _SaveDir = value; } }
        public string SaveFilePrefix { get { return _SaveFilePrefix; } set { _SaveFilePrefix = value; } }
        public long SingleFileRecordLimit { get { return _SingleFileRecordLimit; } set { if (value > -1) { _SingleFileRecordLimit = value; } } }
        public string DateTimeFormat { get { return _DateTimeFormat; } set { if (!string.IsNullOrEmpty(value)) { _DateTimeFormat = value; } } }
        public bool NumberDisplayToStr { get { return _NumberDisplayToStr; } set { _NumberDisplayToStr = value; } }
        public WriteLineHandler OnWriteLineHandler { get; set; }
        public WriteFileEndHandler OnWriteFileEndHandler { get; set; }
        public DisposedHandler OnDisposedHandler { get; set; }
        #endregion

        private System.IO.FileStream GetFileStream()
        {
            bool isNewFile = false;
            if (fileStream != null)
            {
                if (SingleFileRecordLimit > 0)
                {
                    if(fileRowNum >= SingleFileRecordLimit)
                    {
                        fileStream.Close();
                        fileStream.Dispose();
                        fileWriteEndNum = fileNum;
                        if (OnWriteFileEndHandler != null)
                        {
                            try
                            {
                                OnWriteFileEndHandler(new WriteLineEventArgs(SaveFilePrefix, fileNum, fileRowNum, sourceRowNum, lastRowStr));
                            }
                            catch { }
                        }
                        lock (fileNumLock)
                        {
                            fileNum++;
                        }
                        lock (rowNumLock)
                        {
                            fileRowNum = 0;
                        }
                        string fileName = System.IO.Path.Combine(SaveDir.FullName, SaveFilePrefix + "-" + fileNum + ".csv");
                        fileStream = new System.IO.FileStream(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, 1024, false);
                        isNewFile = true;
                    }
                }
            }
            else
            {
                lock (fileNumLock)
                {
                    fileNum++;
                }
                string fileName = System.IO.Path.Combine(SaveDir.FullName, SaveFilePrefix + "-" + fileNum + ".csv");
                fileStream = new System.IO.FileStream(fileName, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read, 1024, false);
                isNewFile = true;
            }
            if (isNewFile)
            {
                fileStream.Write(headerBytes, 0, headerBytes.Length);
            }
            return fileStream;
        }
        private string ColsToStr(object[] cols)
        {
            return ColsToStr(cols, false);
        }
        private string ColsToStr(object[] cols, bool isHeader)
        {
            int len = cols.Length;
            if (isHeader)
            {
                string[] t = new string[cols.Length];
                int n = 0;
                while (n < len)
                {
                    t[n] = ColToStr(cols[n], "String");
                    n++;
                }
                return string.Join(ColSeparator, t) + LineTerminator;
            }
            if (!_ColTypeNamesLoadingFinished)
            {
                _ColTypeNamesLoadingFinished = true;
                _ColTypeNames = new string[len];
                int n = 0;
                while (n < len)
                {
                    Type type = cols[n].GetType();
                    bool isNullable = Nullable.GetUnderlyingType(type) != null;
                    if (isNullable)
                    {
                        type = type.GetGenericTypeDefinition();
                    }
                    _ColTypeNames[n] = type.Name;
                    n++;
                }
            }
            string[] s = new string[cols.Length];
            int i = 0;
            while (i < len)
            {
                s[i] = ColToStr(cols[i], _ColTypeNames[i]);
                i++;
            }
            return string.Join(ColSeparator, s) + LineTerminator;
        }
        private void Write(string line)
        {
            if (_disposed) { return; }
            lastRowStr = line;
            if (isFirst)
            {
                isFirst = false;
                fileRowNum = 0;
                fileNum = 0;
                headerBytes = UseEncoding.GetBytes(ColsToStr(ColNames, true));
                if (SaveDir == null)
                {
                    SaveDir = new System.IO.DirectoryInfo(System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CsvFiles"));
                }
                if (!SaveDir.Exists)
                {
                    SaveDir.Create();
                }
                if (string.IsNullOrEmpty(SaveFilePrefix))
                {
                    SaveFilePrefix = DateTime.Now.ToString("yyyyMMdd");
                }
                hasCsvWriteHandler = OnWriteLineHandler != null;
            }
            var stream = GetFileStream();
            byte[] buffer = UseEncoding.GetBytes(line);
            stream.Write(buffer, 0, buffer.Length);
            //stream.Flush();
            lock (rowNumLock)
            {
                fileRowNum++;
                sourceRowNum++;
            }
            if (hasCsvWriteHandler)
            {
                try
                {
                    OnWriteLineHandler(new WriteLineEventArgs(SaveFilePrefix, fileNum, fileRowNum, sourceRowNum, lastRowStr));
                }
                catch { }
            }
        }


        protected void Write(object[] cols)
        {
            if (cols == null || cols.Length < 1) { return; }
            int myCount = cols.Length;
            if (myCount < 1) { return; }
            if (colCount == 0)
            {
                colCount = _ColNames != null ? _ColNames.Length : 0;
                if (colCount < 1) { return; }
            }
            if (myCount != colCount) { return; }
            Write(ColsToStr(cols));
        }
        protected virtual string ColToStr(object value, string typeName)
        {
            string reval = "";
            if (value is null || value is DBNull) { return reval; }
            switch (typeName)
            {
                case "String":
                    reval = value.ToString();
                    bool hasColSeparator = reval.IndexOf(ColSeparator) > -1;
                    bool hasStrColBoundaryChars = reval.IndexOf(StrColQuote) > -1;
                    if (hasColSeparator || hasStrColBoundaryChars || reval.IndexOf(LineTerminator) > -1)
                    {
                        if (hasStrColBoundaryChars)
                        {
                            reval = reval.Replace(StrColQuote, StrColQuote + StrColQuote);
                        }
                        reval = StrColQuote + reval + StrColQuote;
                    }
                    break;
                case "DateTime":
                    if (hasDateTimeFormat)
                    {
                        reval = Convert.ToDateTime(value).ToString(DateTimeFormat);
                    }
                    else
                    {
                        reval = value.ToString();
                    }
                    break;
                case "Decimal":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Double":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Single":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Int64":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Int32":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Int16":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                case "Byte":
                    reval = NumberDisplayToStr ? string.Format("\t{0}", value) : value.ToString();
                    break;
                default:
                    reval = value.ToString();
                    break;
            }
            return reval;
        }


        public void Close()
        {
            this.Dispose();
        }

        #region -- Dispose --
        bool _disposed;
        public void Dispose()
        {
            Dispose(true);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) { return; }
            if (disposing)
            {
                if (fileStream != null)
                {
                    fileStream.Dispose();
                }
                if(fileWriteEndNum != fileNum)
                {
                    if (OnWriteFileEndHandler != null)
                    {
                        try
                        {
                            OnWriteFileEndHandler(new WriteLineEventArgs(SaveFilePrefix, fileNum, fileRowNum, sourceRowNum, lastRowStr));
                        }
                        catch { }
                    }
                }
                if (OnDisposedHandler != null)
                {
                    try
                    {
                        OnDisposedHandler();
                    }
                    catch { }
                }
            }
            _disposed = true;
        }
        #endregion

    }
}
