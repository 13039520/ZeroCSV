# ZeroCSV
 Read and write large csv files in byte stream mode
## csv file reading example
````C#
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
            Console.WriteLine(string.Format("Columns({0}) : {1}", e.Names.Length, string.Join("|", e.Names)));
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
                //string s1 = e.GetValue("ID");
                //string s2 = e.GetValue(0);
                Console.WriteLine(string.Format("Rows {0} : {1}", e.RowNum, string.Join("|", e.Values)));
            }
        },
        OnEndHandler = (ex) =>
        {
            stopwatch.Stop();
            if (ex != null)
            {
                Console.WriteLine("Error : " + ex.Message);
            }
            Console.WriteLine(string.Format("The end : TotalSeconds={0}&Columns={1}&Rows={2}", stopwatch.Elapsed.TotalSeconds, cols, rows));
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
````
## csv file writing example
```` C#
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
        UseEncoding = Encoding.Default,
        DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff",
        NumberDisplayToStr = false,
        OnWriteLineHandler = (e) => {
            if (e.FileRowNum % 50000 == 0)
            {
                Console.WriteLine(string.Format("FileNum={0}&FileRowNum={1} : \r\n【{2}】", e.FileNum, e.FileRowNum, e.FileRowStr));
            }
        },
        OnWriteFileEndHandler = (e) => {
            Console.WriteLine(string.Format("FileNum={0}&FileRowNum={1}&SourceRowNum={2} : \r\n【{3}】", e.FileNum, e.FileRowNum, e.SourceRowNum, e.FileRowStr), ConsoleColor.Yellow);
        },
        OnWriteBatchEndHandler = (e) => {
            e.Close = true;
            Console.WriteLine("Closing.");
        },
        OnDisposedHandler = () => {
            stopwatch.Stop();
            Console.WriteLine(string.Format("Disposed : TotalSeconds={0}", stopwatch.Elapsed.TotalSeconds));
        }
    };

    string connStr = "Data Source=.;Initial Catalog=BankStatement;User ID=sa;Password=123456;";
    Microsoft.Data.SqlClient.SqlConnection conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
            
    stopwatch.Start();
    toCSV.Write(conn, "SELECT * FROM MyBankStatement");

}
````