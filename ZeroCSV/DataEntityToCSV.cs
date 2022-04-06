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

        public WriteBatchEndHandler OnWriteBatchEndHandler { get; set; }

        private void PropertyInfoInit()
        {
            if (!isFirst) { return; }
            lock (myLock)
            {
                if (!isFirst) { return; }
                Type type = typeof(T);
                properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                propertyCount = properties.Length;
                string[] fieldNames = new string[propertyCount];
                for (int i = 0; i < propertyCount; i++)
                {
                    fieldNames[i] = properties[i].Name;
                }
                base.ColNames = fieldNames;
                isFirst = false;
            }
        }
        private object[] GetPropertyValues(T data)
        {
            object[] values = new object[propertyCount];
            int i = 0;
            while (i < propertyCount)
            {
                values[i] = properties[i].GetValue(data, null);
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
            if (data == null || data.Count < 1) { return; }
            if (isFirst) { PropertyInfoInit(); }
            if (propertyCount < 1) { return; }

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
