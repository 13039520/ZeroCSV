# ZeroCSV
 Read and write large csv files in byte stream mode
## Reading
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
                //string s1 = e.GetValue("FS_WEIGHTNO");
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
