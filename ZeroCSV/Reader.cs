using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace ZeroCSV
{
    public class Reader
    {
        public class HeadEventArgs : EventArgs
        {
            public bool Next { get; set; }
            public string[] Names { get;}
            public HeadEventArgs(string[] names)
            {
                this.Next = true;
                this.Names = names;
            }
        }
        public class RowEventArgs : EventArgs
        {
            private Dictionary<string, int> NameDic { get; }
            public bool Next { get; set; }
            public long RowNum { get;}
            public string[] Values { get;}
            public string GetValue(string name)
            {
                string t = name.ToLower();
                if (NameDic.ContainsKey(t))
                {
                    return Values[NameDic[t]];
                }
                return "";
            }
            public string GetValue(int index)
            {
                if (index > -1 && index < Values.Length)
                {
                    return Values[index];
                }
                return "";
            }
            public RowEventArgs(long rowNum, string[] values, Dictionary<string, int> colNameDic)
            {
                this.Next = true;
                this.RowNum = rowNum;
                this.Values = values;
                this.NameDic = colNameDic;
            }
        }
        public delegate void HeadHandler(HeadEventArgs args);
        public delegate void RowHandler(RowEventArgs args);
        public delegate void EndHandler(Exception e);
        public delegate void StartHandler();

        #region -- private fields --
        private Encoding _UseEncoding = Encoding.Default;
        private int _ReadBlockSize = 1024 * 4;
        private string _ColSeparator = ",";
        private string _RowSeparator = "\r\n";
        private string _StrColBoundaryChars = "\"";
        private int _SkipRows = 0;
        private int _SkipRowsCount = 0;
        /// <summary>
        /// 读取到的步骤：0 初始 1 完成开始行的确认 2 完成头行的读取 
        /// </summary>
        private int _STEP = 0;
        private byte[] _ColSeparatorBytes = null;
        private byte[] _RowSeparatorBytes = null;
        private byte[] _StrColBoundaryCharsBytes = null;
        private byte[] _StrColEndCharsBytes1 = null;
        private byte[] _StrColEndCharsBytes2 = null;
        private byte[] _StrColSpecialCharsBytes = null;
        private long _RowNum = 0;
        private List<string> _ColNames = null;
        private Dictionary<string, int> _ColNameDic = null;
        private byte[] _CacheBytes = null;
        private int _bodyBytesIndex = 0;
        private int _colsCount = 0;
        private int _colsIndex = 0;
        private int _colsBeginIndex = 0;
        private bool _isStrTypeCol = false;
        private bool _isColReaded = true;
        private List<string> _myCols = null;
        private string _StrColBoundaryChars2 = "";
        #endregion

        #region -- public properties --
        public int SkipRows { get { return _SkipRows; }set { _SkipRows = value; } }
        public int ReadBlockSize { get { return _ReadBlockSize; }set { if (value > 0) { _ReadBlockSize = value; } } }
        public string ColSeparator { get { return _ColSeparator; } set { _ColSeparator = value; } }
        public string RowSeparator { get { return _RowSeparator; } set { _RowSeparator = value; } }
        public string StrColBoundaryChars { get { return _StrColBoundaryChars; } set { _StrColBoundaryChars = value; } }
        public Encoding UseEncoding { get { return _UseEncoding; } set { _UseEncoding = value; } }
        public HeadHandler OnHeadHandler { get; set; }
        public RowHandler OnRowHandler { get; set; }
        public StartHandler OnStartHandler { get; set; }
        public EndHandler OnEndHandler { get; set; }
        #endregion

        public void Read(string csvStr)
        {
            Read(this.UseEncoding.GetBytes(csvStr));
        }
        public void Read(byte[] bytes)
        {
            Read(new MemoryStream(bytes));
        }
        public void Read(MemoryStream stream)
        {
            _Read(stream);
        }
        public void Read(FileInfo file)
        {
            System.IO.FileStream fs = null;
            try
            {
                if (file == null)
                {
                    throw new Exception("file is null");
                }
                if (!file.Exists)
                {
                    throw new Exception("file does not exist");
                }
                fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            }
            catch (Exception ex)
            {
                if (fs != null)
                {
                    fs.Close();
                }
                FireStartHandler();
                FireEndHandler(ex);
                return;
            }
            _Read(fs);
        }
        public void Read(FileStream stream)
        {
            _Read(stream);
        }


        private void _Read(Stream stream)
        {
            Exception myEx = null;
            bool isStartCallback = false;
            try
            {
                if (OnRowHandler == null)
                {
                    throw new Exception("必须指定OnRowHandler");
                }
                if (!stream.CanRead)
                {
                    throw new Exception("stream不能是null");
                }
                if (!stream.CanRead)
                {
                    throw new Exception("stream不可读");
                }
                if (string.IsNullOrEmpty(ColSeparator))
                {
                    throw new Exception("必须指定ColSeparator");
                }
                if (string.IsNullOrEmpty(RowSeparator))
                {
                    throw new Exception("必须指定RowSeparator");
                }
                if (string.IsNullOrEmpty(StrColBoundaryChars))
                {
                    StrColBoundaryChars = "";
                }
                _ColSeparatorBytes = UseEncoding.GetBytes(ColSeparator);
                _RowSeparatorBytes = UseEncoding.GetBytes(RowSeparator);
                _StrColBoundaryCharsBytes = UseEncoding.GetBytes(StrColBoundaryChars);
                _CacheBytes = new byte[0];
                _ColNames = new List<string>();
                _ColNameDic = new Dictionary<string, int>();
                _RowNum = 0;
                _SkipRowsCount = 0;
                _StrColBoundaryChars2 = StrColBoundaryChars + StrColBoundaryChars;
                List<byte> listBytes = new List<byte>();
                listBytes.AddRange(_StrColBoundaryCharsBytes);
                listBytes.AddRange(_ColSeparatorBytes);
                _StrColEndCharsBytes1 = listBytes.ToArray();
                listBytes.Clear();
                listBytes.AddRange(_StrColBoundaryCharsBytes);
                listBytes.AddRange(_RowSeparatorBytes);
                _StrColEndCharsBytes2 = listBytes.ToArray();
                listBytes.Clear();
                listBytes.AddRange(_StrColBoundaryCharsBytes);
                listBytes.AddRange(_StrColBoundaryCharsBytes);
                _StrColSpecialCharsBytes = listBytes.ToArray();
                listBytes.Clear();
                long _FileLen = stream.Length;
                long _FilePos = 0;
                isStartCallback = true;
                FireStartHandler();
                //start reading
                bool isFristTime = true;
                long size = ReadBlockSize;
                while (_FilePos < _FileLen)
                {
                    long endPos = _FilePos + size;
                    if (endPos >= _FileLen)
                    {
                        size = _FileLen - _FilePos;
                    }
                    byte[] buffer = new byte[size];
                    stream.Position = _FilePos;
                    int len = stream.Read(buffer, 0, buffer.Length);
                    if (len < buffer.Length)
                    {
                        byte[] temp = new byte[len];
                        Array.Copy(buffer, temp, temp.Length);
                        buffer = temp;
                    }
                    _FilePos += buffer.Length;

                    if (isFristTime)
                    {
                        #region --Check BOM --
                        isFristTime = false;
                        if (buffer.Length > 2)
                        {
                            //BOM: utf-8
                            if (buffer[0] == 239 && buffer[1] == 187 && buffer[2] == 191)
                            {
                                listBytes.AddRange(buffer);
                                listBytes.RemoveRange(0, 3);
                                buffer = listBytes.ToArray();
                                listBytes.Clear();
                                this.UseEncoding = Encoding.UTF8;
                            }
                            //BOM: Big-Endian
                            if (buffer[0] == 254 && buffer[1] == 255)
                            {
                                listBytes.AddRange(buffer);
                                listBytes.RemoveRange(0, 2);
                                buffer = listBytes.ToArray();
                                listBytes.Clear();
                                this.UseEncoding = Encoding.BigEndianUnicode;
                            }
                            //BOM: Little-Endian
                            if (buffer[0] == 255 && buffer[1] == 254)
                            {
                                listBytes.AddRange(buffer);
                                listBytes.RemoveRange(0, 2);
                                buffer = listBytes.ToArray();
                                listBytes.Clear();
                                this.UseEncoding = Encoding.Unicode;
                            }
                        }
                        #endregion
                    }
                    if (_FileLen == _FilePos)
                    {
                        #region -- Check line ends --
                        int count = buffer.Length;
                        int rcbLen = _RowSeparatorBytes.Length;
                        if (rcbLen <= count)
                        {
                            int bIndex = count - 1;
                            int sameCount = 0;
                            for (int i = rcbLen - 1; i > -1; i--)
                            {
                                if (buffer[bIndex] != _RowSeparatorBytes[i])
                                {
                                    break;
                                }
                                sameCount++;
                            }
                            if (sameCount != rcbLen)
                            {
                                listBytes.AddRange(buffer);
                                //Supplemental line terminator
                                listBytes.AddRange(_RowSeparatorBytes);
                                buffer = listBytes.ToArray();
                                listBytes.Clear();
                            }
                        }
                        #endregion
                    }
                    if (_CacheBytes.Length < 1)
                    {
                        _CacheBytes = buffer;
                    }
                    else
                    {
                        byte[] temp = new byte[_CacheBytes.Length + buffer.Length];
                        Array.Copy(_CacheBytes, 0, temp, 0, _CacheBytes.Length);
                        Array.Copy(buffer, 0, temp, _CacheBytes.Length, buffer.Length);
                        _CacheBytes = temp;
                    }
                    if (!Analyzer())
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                myEx = ex;
            }
            if (stream != null)
            {
                stream.Close();
            }
            if (!isStartCallback)
            {
                FireStartHandler();
            }
            FireEndHandler(myEx);
        }
        private void FireStartHandler()
        {
            if (this.OnStartHandler != null)
            {
                try
                {
                    this.OnStartHandler();
                }
                catch { }
            }
        }
        private void FireEndHandler(Exception e)
        {
            if (this.OnEndHandler != null)
            {
                try
                {
                    this.OnEndHandler(e);
                }
                catch { }
            }
        }
        private bool Analyzer()
        {
            bool reval = true;

            while (true)
            {
                if (_CacheBytes.Length < 1) {
                    break;
                }
                bool isBreak = false;
                switch (_STEP)
                {
                    case 0://starting
                        if (!SkipLines())
                        {
                            isBreak = true;
                        }
                        else
                        {
                            _STEP = 1;
                            if (_CacheBytes.Length < 1)
                            {
                                isBreak = true;
                            }
                        }
                        break;
                    case 1://head reading
                        HeadEventArgs args1 = HeadLine();
                        if (args1 != null)
                        {
                            _STEP = 2;
                            if (!args1.Next)
                            {
                                reval = false;
                                isBreak = true;
                            }
                            _colsCount = _ColNames.Count;
                            _myCols = new List<string>(_colsCount);
                        }
                        else//Not enough data
                        {
                            isBreak = true;
                        }
                        break;
                    case 2://body reading
                        RowEventArgs args2 = BodyLine();
                        if (args2 != null)
                        {
                            if (!args2.Next)
                            {
                                reval = false;
                                isBreak = true;
                            }
                        }
                        else//not read complete line
                        {
                            int dif = _CacheBytes.Length - _colsBeginIndex;
                            if (dif > 0)//There are still bytes left
                            {
                                byte[] t = new byte[dif];
                                Array.Copy(_CacheBytes, _colsBeginIndex, t, 0, t.Length);
                                _CacheBytes = t;
                            }
                            else
                            {
                                _CacheBytes = new byte[0];
                            }
                            _bodyBytesIndex = 0;
                            _colsBeginIndex = 0;
                            isBreak = true;
                        }
                        break;
                }
                if (isBreak)
                {
                    break;
                }
            }
            return reval;
        }
        private bool SkipLines()
        {
            if (SkipRows < 1) { return true; }
            bool reval = false;
            int myIndex = ByteArrayIndexOf(_CacheBytes, _RowSeparatorBytes, 0);
            while (myIndex > -1)
            {
                _SkipRowsCount++;
                int n1 = myIndex + _RowSeparatorBytes.Length;
                int n2 = _CacheBytes.Length - n1;
                byte[] t = new byte[n2];
                Array.Copy(_CacheBytes, n1, t, 0, t.Length);
                _CacheBytes = t;

                if (_SkipRowsCount == SkipRows)
                {
                    reval = true;
                    break;
                }
                myIndex = ByteArrayIndexOf(_CacheBytes, _RowSeparatorBytes, 0);
            }
            return reval;
        }
        private HeadEventArgs HeadLine()
        {
            HeadEventArgs reval = null;
            int rowEndIndex = ByteArrayIndexOf(_CacheBytes, _RowSeparatorBytes, 0);
            if (rowEndIndex < 0)//line separator not found
            {
                return reval;
            }
            if (rowEndIndex < 1)
            {
                throw new Exception("head error");
            }
            byte[] lineBytes = new byte[rowEndIndex + _RowSeparatorBytes.Length];
            Array.Copy(_CacheBytes, 0, lineBytes, 0, lineBytes.Length);

            int n1 = lineBytes.Length;
            int n2 = _CacheBytes.Length - n1;
            byte[] t = new byte[n2];
            Array.Copy(_CacheBytes, n1, t, 0, t.Length);
            _CacheBytes = t;

            int colIndex = 0;
            while (colIndex > -1)
            {
                bool isEnd = false;
                bool isStrTypeCol = ByteArrayIndexOf(lineBytes, _StrColBoundaryCharsBytes, colIndex) == colIndex;//The first character is the string identifier
                if (isStrTypeCol)//is a string column
                {
                    int myColEndIndex = GetColEndIndex(lineBytes, colIndex, true);
                    if (myColEndIndex < 0)
                    {
                        throw new Exception("Missing column terminator");
                    }
                    colIndex += _StrColBoundaryCharsBytes.Length;
                    int count = myColEndIndex - colIndex;
                    string s = UseEncoding.GetString(lineBytes, colIndex, count);
                    string ts = UseEncoding.GetString(_StrColSpecialCharsBytes);
                    if (s.IndexOf(ts) > -1)
                    {
                        s = s.Replace(ts, UseEncoding.GetString(_StrColBoundaryCharsBytes));
                    }
                    ts = s.ToLower();
                    if (_ColNameDic.ContainsKey(ts))
                    {
                        throw new Exception("Duplicate column name \"" + s + "\"");
                    }
                    _ColNameDic.Add(ts, _ColNames.Count);
                    _ColNames.Add(s);

                    if (myColEndIndex + _StrColBoundaryCharsBytes.Length + _RowSeparatorBytes.Length == lineBytes.Length)
                    {
                        isEnd = true;
                    }
                    if (!isEnd)
                    {
                        colIndex = myColEndIndex + _StrColEndCharsBytes1.Length;
                    }
                }
                else//not a string column
                {
                    int myColEndIndex = ByteArrayIndexOf(lineBytes, _ColSeparatorBytes, colIndex);
                    if (myColEndIndex < 0)
                    {
                        myColEndIndex = lineBytes.Length - _RowSeparatorBytes.Length;
                        isEnd = true;
                    }
                    string s = UseEncoding.GetString(lineBytes, colIndex, myColEndIndex - colIndex);
                    string ts = s.ToLower();
                    if (_ColNameDic.ContainsKey(ts))
                    {
                        throw new Exception("Duplicate column name \"" + s + "\"");
                    }
                    _ColNameDic.Add(ts, _ColNames.Count);
                    _ColNames.Add(s);
                    if (!isEnd)
                    {
                        colIndex = myColEndIndex + _ColSeparatorBytes.Length;
                    }
                }

                if (isEnd)
                {
                    break;
                }
            }
            reval = new HeadEventArgs(_ColNames.ToArray());
            if (this.OnHeadHandler != null)
            {
                this.OnHeadHandler(reval);
            }
            return reval;

        }
        private RowEventArgs BodyLine()
        {
            RowEventArgs reval = null;
            int cLen = _CacheBytes.Length;
            if (cLen < 1) { return reval; }
            while (_colsIndex < _colsCount)
            {
                bool isRowEnd = 1 + _colsIndex >= _colsCount;
                if(_bodyBytesIndex + _StrColBoundaryCharsBytes.Length >= cLen)
                {
                    break;
                }
                if (_isColReaded)
                {
                    #region -- Step1：Check if it is a string type column --
                    _isColReaded = false;
                    int sameCount = 0;
                    for (int i = 0; i < _StrColBoundaryCharsBytes.Length; i++)
                    {
                        if (_CacheBytes[_bodyBytesIndex + i] != _StrColBoundaryCharsBytes[i])
                        {
                            break;
                        }
                        sameCount++;
                    }
                    _isStrTypeCol = sameCount == _StrColBoundaryCharsBytes.Length;
                    #endregion
                }

                int colEndIndex;
                #region -- Step2：read column terminator position --
                if (!isRowEnd)
                {
                    colEndIndex = GetColEndIndex(_CacheBytes, _bodyBytesIndex, _isStrTypeCol);
                }
                else
                {
                    byte[] tBytes = _isStrTypeCol ? _StrColEndCharsBytes2 : _RowSeparatorBytes;
                    //The last column should try to read newlines
                    colEndIndex = ByteArrayIndexOf(_CacheBytes, tBytes, _bodyBytesIndex);
                    if (colEndIndex < 0)
                    {
                        int dif = cLen - tBytes.Length;
                        if (dif > 0)
                        {
                            _bodyBytesIndex = dif;
                        }
                        break;
                    }
                }
                #endregion

                if (colEndIndex > -1)
                {
                    #region -- Step3：read complete column data --
                    string colStr = "";
                    if (_isStrTypeCol)
                    {
                        int beginIndex = _colsBeginIndex + _StrColBoundaryCharsBytes.Length;
                        int count = colEndIndex - beginIndex;
                        if (count > 0)
                        {
                            colStr = UseEncoding.GetString(_CacheBytes, beginIndex, count);
                        }
                    }
                    else
                    {
                        int count = colEndIndex - _colsBeginIndex;
                        if (count > 0)
                        {
                            if (_CacheBytes[_colsBeginIndex] != 9)
                            {
                                colStr = UseEncoding.GetString(_CacheBytes, _colsBeginIndex, count);
                            }
                            else//Begins with "\t"
                            {
                                colStr = UseEncoding.GetString(_CacheBytes, _colsBeginIndex + 1, count);
                            }
                        }
                    }
                    //Reset index after column read ends
                    _bodyBytesIndex = colEndIndex;
                    if (isRowEnd)
                    {
                        if (_isStrTypeCol)
                        {
                            _bodyBytesIndex += _StrColEndCharsBytes2.Length;
                        }
                        else
                        {
                            _bodyBytesIndex += _RowSeparatorBytes.Length;
                        }
                    }
                    else
                    {
                        if (_isStrTypeCol)
                        {
                            _bodyBytesIndex += _StrColEndCharsBytes1.Length;
                        }
                        else
                        {
                            _bodyBytesIndex += _ColSeparatorBytes.Length;
                        }
                    }
                    if (_StrColBoundaryChars2.Length > 0 && colStr.IndexOf(_StrColBoundaryChars2) > -1)
                    {
                        colStr = colStr.Replace(_StrColBoundaryChars2, StrColBoundaryChars);
                    }
                    //Record the start position of the next column
                    _colsBeginIndex = _bodyBytesIndex;
                    _myCols.Add(colStr);
                    _colsIndex++;
                    _isColReaded = true;
                    #endregion
                }
                else
                {
                    #region -- Step3：Column data is incomplete --
                    int len = (_isStrTypeCol ? _StrColEndCharsBytes2 : _ColSeparatorBytes).Length;
                    if (isRowEnd)
                    {
                        len = (_isStrTypeCol ? _StrColEndCharsBytes2 : _RowSeparatorBytes).Length;
                    }
                    int dif = cLen - len;
                    if (dif > 0)
                    {
                        _bodyBytesIndex = dif;
                    }
                    #endregion

                    break;//Exit the loop: wait for the next data fill
                }

                if (isRowEnd)
                {
                    #region -- Step4：Read to complete row data (callback) --
                    _bodyBytesIndex -= _RowSeparatorBytes.Length;
                    int myRowIndex = ByteArrayIndexOf(_CacheBytes, _RowSeparatorBytes, _bodyBytesIndex);
                    if (_bodyBytesIndex < 0)
                    {
                        break;
                    }
                    _RowNum++;
                    if (myRowIndex != _bodyBytesIndex)
                    {
                        //not a newline
                        throw new Exception("Wrong number of columns for row " + _RowNum);
                    }
                    _bodyBytesIndex += _RowSeparatorBytes.Length;

                    reval = new RowEventArgs(_RowNum, _myCols.ToArray(), _ColNameDic);
                    _myCols.Clear();
                    _colsIndex = 0;
                    this.OnRowHandler(reval);
                    #endregion

                    break;//Exit the loop: wait for the read of next line
                }
            }
            
            return reval;
        }
        private int GetColEndIndex(byte[] source, int sourceIndex, bool isStrTypeEnd)
        {
            int reval = -1;
            if (isStrTypeEnd)
            {
                int myColEndIndex = ByteArrayIndexOf(source, _StrColEndCharsBytes1, sourceIndex);
                bool useColSeparator1 = true;
                bool isEnd = false;
                if (myColEndIndex < 0)
                {
                    myColEndIndex = ByteArrayIndexOf(source, _StrColEndCharsBytes2, sourceIndex);
                    useColSeparator1 = false;
                    isEnd = myColEndIndex > -1;
                }
                else
                {
                    if (myColEndIndex == sourceIndex + 1)
                    {
                        return myColEndIndex;//is empty string
                    }
                    //Determine if the preceding is an escape character
                    int endN = myColEndIndex + _StrColSpecialCharsBytes.Length - 1;
                    int beginN = endN - _StrColSpecialCharsBytes.Length;
                    if (beginN > -1)
                    {
                        int specialN = ByteArrayIndexOf(source, _StrColSpecialCharsBytes, beginN);
                        if(beginN == specialN)
                        {
                            myColEndIndex = myColEndIndex + (useColSeparator1 ? _StrColEndCharsBytes1.Length: _StrColEndCharsBytes2.Length);
                            //recursion
                            myColEndIndex = GetColEndIndex(source, myColEndIndex, isStrTypeEnd);
                            isEnd = true;
                        }
                        else
                        {
                            isEnd = true;
                        }
                    }
                    else
                    {
                        isEnd = true;
                    }
                    //
                }
                if (isEnd&&myColEndIndex > -1)
                {
                    reval = myColEndIndex;
                }
            }
            else
            {
                reval = ByteArrayIndexOf(source, _ColSeparatorBytes, sourceIndex);
                if (reval < 0)//try to find newlines
                {
                    reval = ByteArrayIndexOf(source, _RowSeparatorBytes, sourceIndex);
                }
            }
            return reval;
        }
        private int ByteArrayIndexOf(byte[] source, byte[] frame, int sourceIndex)
        {
            if (sourceIndex > -1 && frame.Length > 0 && frame.Length + sourceIndex <= source.Length)
            {
                int myLen = source.Length - frame.Length + 1;
                for (int i = sourceIndex; i < myLen; i++)
                {
                    if (source[i] == frame[0])
                    {
                        if (frame.Length < 2) { return i; }
                        bool flag = true;
                        for (int j = 1; j < frame.Length; j++)
                        {
                            if (source[i + j] != frame[j])
                            {
                                flag = false;
                                break;
                            }
                        }
                        if (flag) { return i; }
                    }
                }
            }
            return -1;
        }

    }
}
