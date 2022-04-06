﻿using System;
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
            CsvWrite(System.IO.Path.Combine(dir, "table_bak"), "table");

            Console.WriteLine("Reading : ");
            CsvRead(System.IO.Path.Combine(dir, "table_bak/table-1.csv"), 0, 0);
            
            while (true)
            {
                System.Threading.Thread.Sleep(1000);
            }
        }
        static void CsvRead(string filePath, int skipRows, int readLimit)
        {
            bool useLimit = readLimit > 0;
            int cols = 0;
            long rows = 0;
            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            ZeroCSV.Reader reader = new ZeroCSV.Reader
            {
                SkipRows = skipRows,
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
                    if (rows % 50000 == 0)
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
                    double totalSeconds = stopwatch.Elapsed.TotalSeconds;
                    ShowMsg(string.Format("The end : TotalSeconds={0}&Columns={1}&Rows={2}&ReadInOneSecond={3}", totalSeconds, cols, rows, Convert.ToInt32(rows / totalSeconds)), ConsoleColor.Red);
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

        static void CsvWrite(string dir, string saveFilePrefix)
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
                UseEncoding=Encoding.Default,
                DateTimeFormat="yyyy-MM-dd HH:mm:ss.fff",
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
                    e.Close = true;
                    ShowMsg("Closing.");
                },
                OnDisposedHandler = () => {
                    stopwatch.Stop();
                    ShowMsg(string.Format("Disposed : TotalSeconds={0}", stopwatch.Elapsed.TotalSeconds), ConsoleColor.Red);
                }
            };

            string connStr = "Data Source=.;Initial Catalog=BankStatementAnalysis;User ID=sa;Password=yangshaogao;";
            Microsoft.Data.SqlClient.SqlConnection conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
            
            stopwatch.Start();
            toCSV.Write(conn, "SELECT * FROM CaseBankStatement");

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
