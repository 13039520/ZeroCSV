using System;
using System.Collections.Generic;
using System.Text;

namespace ZeroCSV.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            string dir = AppDomain.CurrentDomain.BaseDirectory;

            Console.WriteLine("Writing : ");
            //DataReaderToCSV(System.IO.Path.Combine(dir, "table_bak"), "table");
            DataEntityToCSV(System.IO.Path.Combine(dir, "table_bak"), "table");
            
            Console.WriteLine("Reading : ");
            CsvRead(System.IO.Path.Combine(dir, "table_bak/table-1.csv"), 1, false, 0);
            
            while (true)
            {
                System.Threading.Thread.Sleep(1000);
            }
        }
        static void CsvRead(string filePath, int skipRows, bool withHeader, int readLimit)
        {
            bool useLimit = readLimit > 0;
            int cols = 0;
            long rows = 0;
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            ZeroCSV.Reader reader = new ZeroCSV.Reader
            {
                SkipRows = skipRows,
                WithHeader = withHeader,
                UseEncoding = Encoding.Default,
                OnStartHandler = () => {
                    stopwatch.Start();
                },
                OnHeadHandler = (e) =>
                {
                    cols = e.Names.Length;
                    ShowMsg(string.Format("Columns({0}) : {1}", e.Names.Length, string.Join("|", e.Names)));
                },
                OnRowHandler = (e) =>
                {
                    if (useLimit && e.RowNum == readLimit)
                    {
                        e.Next = false;//stop
                    }
                    rows = e.RowNum;
                    if (rows % 5 == 0)
                    {
                        //string s1 = e.GetValue("FS_WEIGHTNO");
                        //string s2 = e.GetValue(0);
                        ShowMsg(string.Format("Rows {0} : {1}", e.RowNum, string.Join("|", e.Values)));
                    }
                },
                OnEndHandler = (ex) =>
                {
                    stopwatch.Stop();
                    if (ex != null)
                    {
                        ShowMsg("Error : " + ex.Message);
                    }
                    ShowMsg(string.Format("The end : TotalSeconds={0}&Columns={1}&Rows={2}", stopwatch.Elapsed.TotalSeconds, cols, rows), ConsoleColor.Red);
                }
            };
            //reading
            reader.Read(new System.IO.FileInfo(filePath));
            //reader.Read(string csvStr);
            //reader.Read(byte[] bytes);
            //reader.Read(System.IO.FileInfo file);
            //reader.Read(System.IO.MemoryStream stream);
            //reader.Read(System.IO.FileStream stream);
        }

        static void DataReaderToCSV(string dir, string saveFilePrefix)
        {
            System.IO.DirectoryInfo directory = new System.IO.DirectoryInfo(dir);
            if (!directory.Exists)
            {
                directory.Create();
            }
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            ZeroCSV.DataReaderToCSV toCSV = new DataReaderToCSV
            {
                SaveDir = directory,
                SaveFilePrefix = saveFilePrefix,
                SingleFileRecordLimit = 0,
                UseEncoding = Encoding.Default,
                DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff",
                NumberDisplayToStr = false,
                OnWriteLineHandler = (e) => {
                    if (e.FileRowNum % 50000 == 0)
                    {
                        ShowMsg(string.Format("FileNum={0}&FileRowNum={1} : \r\n【{2}】", e.FileNum, e.FileRowNum, e.FileRowStr));
                    }
                },
                OnWriteFileEndHandler = (e) => {
                    ShowMsg(string.Format("FileNum={0}&FileRowNum={1}&SourceRowNum={2} : \r\n【{3}】", e.FileNum, e.FileRowNum, e.SourceRowNum, e.FileRowStr), ConsoleColor.Yellow);
                },
                OnWriteBatchEndHandler = (e) => {
                    e.Close = e.BatchNum == 1;
                    if (e.Close)
                    {
                        ShowMsg("Closing.");
                    }
                },
                OnDisposedHandler = () => {
                    stopwatch.Stop();
                    ShowMsg(string.Format("Disposed : TotalSeconds={0}", stopwatch.Elapsed.TotalSeconds), ConsoleColor.Red);
                }
            };

            string connStr = "Data Source=.;Initial Catalog=BankStatement;User ID=sa;Password=123456;";
            Microsoft.Data.SqlClient.SqlConnection conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
            
            stopwatch.Start();
            //WriteBatch 1:
            toCSV.Write(conn, "SELECT * FROM MyBankStatement");

        }

        public class MyEntity
        {
            public int ID { get; set; }
            public string Name { get; set; }
        }
        static void DataEntityToCSV(string dir, string saveFilePrefix)
        {
            System.IO.DirectoryInfo directory = new System.IO.DirectoryInfo(dir);
            if (!directory.Exists)
            {
                directory.Create();
            }
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            System.IO.MemoryStream ms = new System.IO.MemoryStream(1024);
            ZeroCSV.DataEntityToCSV<MyEntity> toCSV = new DataEntityToCSV<MyEntity>(ms)
            {
                SaveDir = directory,
                SaveFilePrefix = saveFilePrefix,
                SingleFileRecordLimit = 5,
                UseEncoding = Encoding.Default,
                DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff",
                NumberDisplayToStr = false,
                OnWriteLineHandler = (e) => {
                    if (e.FileRowNum % 50000 == 0)
                    {
                        ShowMsg(string.Format("FileNum={0}&FileRowNum={1} : \r\n【{2}】", e.FileNum, e.FileRowNum, e.FileRowStr));
                    }
                },
                OnWriteFileEndHandler = (e) => {
                    ShowMsg(string.Format("FileNum={0}&FileRowNum={1}&SourceRowNum={2} : \r\n【{3}】", e.FileNum, e.FileRowNum, e.SourceRowNum, e.FileRowStr), ConsoleColor.Yellow);
                    ShowMsg("Length=" + ms.Length);
                },
                OnWriteBatchEndHandler = (e) => {
                    //e.Close = e.BatchNum == 2;
                    ShowMsg("Write Batch " + e.BatchNum + " End.");
                },
                OnDisposedHandler = () => {
                    stopwatch.Stop();
                    ShowMsg(string.Format("Disposed : TotalSeconds={0}", stopwatch.Elapsed.TotalSeconds), ConsoleColor.Red);
                }
            };
            stopwatch.Start();
            //Write Batch 1:
            toCSV.Write(
                new MyEntity { ID = 1, Name = "Name1" },
                new MyEntity { ID = 2, Name = "Name2" },
                new MyEntity { ID = 3, Name = "Name3" },
                new MyEntity { ID = 4, Name = "Name4" },
                new MyEntity { ID = 5, Name = "Name5" });

            //Write Batch 2:
            List<MyEntity> list = new List<MyEntity>();
            list.Add(new MyEntity { ID = 6, Name = "Name6" });
            list.Add(new MyEntity { ID = 7, Name = "Name7" });
            list.Add(new MyEntity { ID = 8, Name = "Name8" });
            list.Add(new MyEntity { ID = 9, Name = "Name9" });
            list.Add(new MyEntity { ID = 10, Name = "Name10" });
            toCSV.Write(list);

            toCSV.Close();

            string s = Encoding.Default.GetString(ms.ToArray());
            ms.Dispose();
            Console.WriteLine(s);

        }

        static void ShowMsg(string info)
        {
            ShowMsg(info, null);
        }
        static void ShowMsg(string info, ConsoleColor? useColor)
        {
            bool myColor = useColor != null;
            if (myColor)
            {
                Console.ForegroundColor = useColor.Value;
            }
            Console.WriteLine("{0}\t{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), info);
            if (myColor)
            {
                Console.ResetColor();
            }
        }

    }
}
