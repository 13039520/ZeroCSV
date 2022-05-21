using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace ZeroCSV
{
    public class DataEntityToCSV<T> : Writer where T: class
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

        private static object myLock = new object();
        private bool isFirst = true;
        private PropertyInfo[] properties = null;
        private int propertyCount = 0;
        private int batchNum = 0;
        private int[] useColIndexArray = null;

        public DataEntityToCSV() : base() { }
        public DataEntityToCSV(System.IO.Stream writeStream) : base(writeStream) { }
        public DataEntityToCSV(System.IO.Stream writeStream, bool ownsStream) : base(writeStream, ownsStream) { }

        public WriteBatchEndHandler OnWriteBatchEndHandler { get; set; }

        private int GetIndex(IEnumerable<string> source, string item, StringComparison comparison = StringComparison.OrdinalIgnoreCase)
        {
            int i = 0;
            foreach (string s in source)
            {
                if (string.Equals(s, item, comparison))
                {
                    return i;
                }
                i++;
            }
            return -1;
        }
        private void CheckSource(List<T> data)
        {
            if(data== null) { throw new Exception("data is null"); }
            if (!isFirst) { return; }
            isFirst = false;
            Type type = typeof(T);
            properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            propertyCount = properties.Length;
            string[] fieldNames = new string[propertyCount];
            List<int> fieldIndexList = new List<int>(propertyCount);
            for (int i = 0; i < propertyCount; i++)
            {
                fieldNames[i] = properties[i].Name;
                fieldIndexList.Add(i);
            }
            if (this.CustomWriteColNames != null && this.CustomWriteColNames.Length > 0)
            {
                List<string> nameList = new List<string>(this.CustomWriteColNames.Length);
                List<int> indexList = new List<int>(this.CustomWriteColNames.Length);
                for (int i = 0; i < this.CustomWriteColNames.Length; i++)
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
                base.ColNames = fieldNames;
            }
        }

        private object[] GetPropertyValues(T data)
        {
            object[] values = new object[useColIndexArray.Length];
            int i = 0;
            while (i < useColIndexArray.Length)
            {
                values[i] = properties[useColIndexArray[i]].GetValue(data, null);
                i++;
            }
            return values;
        }
        public void Write(params T[] data)
        {
            if (data == null || data.Length < 1) { return; }
            List<T> temp = new List<T>(data.Length);
            temp.AddRange(data);
            Write(temp);
        }
        public void Write(List<T> data)
        {
            CheckSource(data);

            int i = 0;
            int count = data.Count;
            while (i < count)
            {
                if (data[i] != null)
                {
                    object[] values = GetPropertyValues(data[i]);
                    base.Write(values);
                }
                i++;
            }
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
            else
            {
                base.Close();
            }
        }
    }
}
