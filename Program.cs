

namespace MyParasol.FileWatcher
{
    public class Watcher
    {
        public static string ProcessedFilesLocation
        {
            get
            {
                return ConfigurationManager.AppSettings["ProcessedFilePath"];
            }
        }

        public static string ErrorFileLocation
        {
            get
            {
                return ConfigurationManager.AppSettings["ErrorFilePath"];
            }
        }

        private static string WatchFolderLocation
        {
            get
            {
                return ConfigurationManager.AppSettings["WatchDirectory"];
            }
        }

        private static string FileFilter
        {
            get
            {
                return ConfigurationManager.AppSettings["FileFilter"];
            }
        }

        private static string AzureStorageAccountName
        {
            get
            {
                return ConfigurationManager.AppSettings["StorageAccountName"];
            }
        }

        private static string AzureStorageAccountKey
        {
            get
            {
                return ConfigurationManager.AppSettings["StorageAccountKey"];
            }
        }
		
        private static string AzureDefaultContainerName
        {
            get
            {
                return ConfigurationManager.AppSettings["DefaultContainerName"];
            }
        }

        private static string FileNameRegularExpression
        {
            get
            {
                return ConfigurationManager.AppSettings["FileNameRegularExpression"];
            }
        }

        private static string FileNameExample
        {
            get
            {
                return ConfigurationManager.AppSettings["FileNameExample"];
            }
        }

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger();

        public static void Main()
        {
            Run();
        }

        private static void Run()
        {
            try
            {
                //Process all files that are in the watch folder
                ProcessFiles();
            }
            catch (Exception ex)
            {
                log.Error($"An Error Occurred: Message: {ex.Message}, {Environment.NewLine} stacktrace: {ex.StackTrace}");

            }
        }

        private static void ProcessFiles()
        {
            var filesToProcess = Directory.GetFiles(WatchFolderLocation, "*.pdf");

            for (int i = 0; i <= filesToProcess.Length - 1; i++)
            {
                ProcessFile(filesToProcess[i]);
            }
        }

        private static void ProcessFile(string filePath)
        {
            var webMembersDataLayer = new WebMembersDataLayer();

            var azureRepository = new AzureDocumentRepository(AzureStorageAccountName, AzureStorageAccountKey);
 
            var fileName = Path.GetFileName(filePath);

            string employeeCode = string.Empty;

            try
            {
                employeeCode = GetEmployeeCodeFromFileName(fileName);
            }
            catch (Exception ex)
            {
                log.Error($"invalid FileName: fileName, Exception: {ex.Message}");
                System.IO.File.Move(filePath, Path.Combine(ErrorFileLocation, fileName));
                return;
            }

            var webMemberId = webMembersDataLayer.GetWebMemberId(employeeCode);

            var azureUrl = string.Empty;

            //add date and time to the end of file name to avoid duplicate issues.
            var uniqueFileName = fileName.Replace(".pdf", string.Format("{0}.pdf", DateTime.Now.ToString().Replace("/", "-").Replace(":", "-")));

            var destinationFilePath = string.Format("{0}\\{1}", ProcessedFilesLocation, uniqueFileName);

            var errorPath = string.Format("{0}\\{1}", ErrorFileLocation, uniqueFileName);

            if (webMemberId == 0)
            {
                log.Error($"invalid employee code {employeeCode} {uniqueFileName}");
                System.IO.File.Move(filePath, errorPath);
                return;
            }

            try
            {
                var fileByteArray = StreamFile(filePath);
                log.Info($"Attempting to add file to azure storage: {filePath} ");
                var azureStorageUri = azureRepository.SaveBlob(string.Format("{0}{1}", AzureDefaultContainerName, webMemberId), uniqueFileName, fileByteArray);
                azureUrl = azureStorageUri.ToString();
            }
            catch (Exception ex)
            {
                log.Error($"Adding file to azure storage failed {ex.Message} ");
                System.IO.File.Move(filePath, errorPath);
            }

            log.Info($"file saving to azure storage finished: {filePath} ");

            try
            {
                //Add file to database table
                log.Info($"Adding file reference to database: {filePath} ");
                webMembersDataLayer.InsertFileData(webMemberId, azureUrl);
            }
            catch (Exception ex)
            {
                log.Error($"Adding file to database failed {ex.Message} ");
                System.IO.File.Move(filePath, errorPath);
            }

            try
            {    // Specify what is done when a file is changed, created, or deleted.
                log.Info($"Move file to processed location: {filePath}");
                
                //set destination file locatio

                System.IO.File.Move(filePath, destinationFilePath);

                log.Info(string.Format("File {0} processed and moved to {1}", filePath, destinationFilePath));
            }
            catch (Exception ex)
            {
                log.Error($"Move file to processed failed {ex.Message} ");
                System.IO.File.Move(filePath, errorPath);
            }
        }

        // Define the event handlers.
        private static void OnChanged(object source, FileSystemEventArgs e)
        {
            ProcessFile(e.FullPath);
        }

        private static byte[] StreamFile(string filename)
        {
            while (!IsFileReady(filename))
            {
                log.Info("waiting for file");
                System.Threading.Thread.Sleep(5000);
            }

            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                // Create a byte array of file stream length
                byte[] bytes = System.IO.File.ReadAllBytes(filename);
                //Read block of bytes from stream into the byte array
                fs.Read(bytes, 0, System.Convert.ToInt32(fs.Length));
                //Close the File Stream
                fs.Close();
                return bytes; //return the byte data
            }
        }

        private static bool IsFileReady(string filename)
        {
            // If the file can be opened for exclusive access it means that the file
            // is no longer locked by another process.
            try
            {
                using (FileStream inputStream = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.None))
                    return inputStream.Length > 0;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static string GetEmployeeCodeFromFileName(string fileName)
        {
            return fileName.Substring(21, 11);
        }
    }
}