using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroCSV
{
    public class DataReaderToCSV: Writer
    {
        public class WriteBatchEndArgs : EventArgs
        {
            public bool Close { get; set; }
            public int BatchNum { get; }
            public WriteBatchEndArgs(int batchNum)
            {
                this.Close = false;
                this.BatchNum = batchNum;
            }
        }
        public delegate void WriteBatchEndHandler(WriteBatchEndArgs e);
        public delegate System.Data.IDataReader GetDataReaderHandler();

        private static object myLock = new object();
        private int fieldCount = 0;
        private bool isFirst = true;
        private int batchNum = 0;
        private int[] useColIndexArray = null;

        public DataReaderToCSV() : base() { }
        public DataReaderToCSV(System.IO.Stream writeStream) : base(writeStream) { }
        public DataReaderToCSV(System.IO.Stream writeStream, bool ownsStream) : base(writeStream, ownsStream) { }

        public WriteBatchEndHandler OnWriteBatchEndHandler { get; set; }

        private int GetIndex(IEnumerable<string> source, string item, StringComparison comparison = StringComparison.OrdinalIgnoreCase)
        {
            int i = 0;
            foreach (string s in source)
            {
                if(string.Equals(s, item, comparison))
                {
                    return i;
                }
                i++;
            }
            return -1;
        }
        private void CheckSource(System.Data.IDataReader source)
        {
            if (!isFirst) { return; }
            isFirst = false;
            if (source == null || source.FieldCount < 1) { throw new Exception("Wrong number of columns for parameter \"source\""); }
            fieldCount = source.FieldCount;
            List<string> fieldNames = new List<string>(fieldCount);
            List<int> fieldIndexList = new List<int>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                fieldNames.Add(source.GetName(i));
                fieldIndexList.Add(i);
            }
            if (this.CustomWriteColNames != null && this.CustomWriteColNames.Length > 0)
            {
                List<string> nameList = new List<string>(this.CustomWriteColNames.Length);
                List<int> indexList = new List<int>(this.CustomWriteColNames.Length);
                for(int i = 0; i < this.CustomWriteColNames.Length; i++)
                {
                    int index = GetIndex(fieldNames, this.CustomWriteColNames[i]);
                    if (index < 0)
                    {
                        throw new Exception("Field name \"" + this.CustomWriteColNames[i] + "\" of CustomWriteColNames[" + i + "] does not exist");
                    }
                    if (nameList.Contains(fieldNames[index]))
                    {
                        throw new Exception("Field name \"" + this.CustomWriteColNames[i] + "\" of CustomWriteColNames[" + i + "] is duplicated");
                    }
                    nameList.Add(fieldNames[index]);
                    indexList.Add(index);
                }
                useColIndexArray = indexList.ToArray();
                base.ColNames = nameList.ToArray();
            }
            else
            {
                useColIndexArray = fieldIndexList.ToArray();
                base.ColNames = fieldNames.ToArray();
            }
        }
        public void Write(System.Data.IDataReader source)
        {
            CheckSource(source);

            if (fieldCount != source.FieldCount)
            {
                throw new Exception("Wrong number of columns for parameter \"source\"");
            }
            while (source.Read())
            {
                object[] values = new object[useColIndexArray.Length];
                for (int i = 0; i < useColIndexArray.Length; i++)
                {
                    values[i] = source[useColIndexArray[i]];
                }
                base.Write(values);
            }
            source.Close();
            lock (myLock)
            {
                batchNum++;
            }
            if (OnWriteBatchEndHandler != null)
            {
                try
                {
                    WriteBatchEndArgs result = new WriteBatchEndArgs(batchNum);
                    OnWriteBatchEndHandler(result);
                    if (result.Close)
                    {
                        base.Close();
                    }
                }
                catch { }
            }
            else {
                base.Close();
            }
        }
        public void Write(System.Data.Common.DbConnection conn, string querySql, bool endCloseConn = true)
        {
            if (conn.State != System.Data.ConnectionState.Open)
            {
                conn.Open();
            }
            System.Data.Common.DbCommand cmd = conn.CreateCommand();
            cmd.CommandText = querySql;
            Write(cmd.ExecuteReader());
            cmd.Dispose();
            if (endCloseConn)
            {
                conn.Close();
            }
        }


    }
}
