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
            public WriteBatchEndArgs()
            {
                this.Close = false;
            }
        }
        public delegate void WriteBatchEndHandler(WriteBatchEndArgs e);
        public delegate System.Data.IDataReader GetDataReaderHandler();

        private int fieldCount = 0;
        private bool isFirst = true;

        public WriteBatchEndHandler OnWriteBatchEndHandler { get; set; }

        public void Write(System.Data.IDataReader source)
        {
            if (source == null || source.FieldCount < 1) { return; }
            if (isFirst) { fieldCount = source.FieldCount; }
            else
            {
                if (fieldCount != source.FieldCount)
                {
                    return;
                }
            }
            while (source.Read())
            {
                if (isFirst)
                {
                    isFirst = false;
                    string[] fieldNames = new string[fieldCount];
                    for(int i = 0; i < fieldCount; i++)
                    {
                        fieldNames[i] = source.GetName(i);
                    }
                    base.ColNames = fieldNames;
                }
                object[] values = new object[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                {
                    values[i] = source[i];
                }
                base.Write(values);
            }
            source.Close();
            if (OnWriteBatchEndHandler != null)
            {
                try
                {
                    WriteBatchEndArgs result = new WriteBatchEndArgs();
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
