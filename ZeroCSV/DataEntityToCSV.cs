using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace ZeroCSV
{
    public class DataEntityToCSV<T> : Writer where T: class
    {
        public class WriteDataEndArgs : EventArgs
        {
            public bool CloseWriteStream { get; set; }
            public readonly List<T> Source = null;
            public WriteDataEndArgs(List<T> source)
            {
                this.CloseWriteStream = false;
                this.Source = source;
            }
        }
        public delegate void WriteDataEndHandler(WriteDataEndArgs e);

        private static object initLock = new object();
        private bool isFirst = true;
        private PropertyInfo[] properties = null;
        private int propertyCount = 0;

        public WriteDataEndHandler OnWriteDataEndHandler { get; set; }

        private void PropertyInfoInit()
        {
            if (!isFirst) { return; }
            lock (initLock)
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
            if (OnWriteDataEndHandler != null)
            {
                try
                {
                    WriteDataEndArgs result = new WriteDataEndArgs(data);
                    OnWriteDataEndHandler(result);
                    if (result.CloseWriteStream)
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
