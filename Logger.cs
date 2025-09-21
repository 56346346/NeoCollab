using System;
using System.IO;
using Serilog;

namespace NeoCollab
{
    internal static class Logger
    {
        private static readonly string _logDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "NeoCollab",
            "log"
        );
        private static readonly string _logPath =
           Path.Combine(_logDir, nameof(Logger) + ".log");
        private static readonly object _logLock = new object();
        // Legt das Log-Verzeichnis an und initialisiert den Serilog-Logger
        static Logger()
        {
            if (!Directory.Exists(_logDir))
                Directory.CreateDirectory(_logDir);

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.File(Path.Combine(_logDir, "main.log"), shared: true)
                .CreateLogger();
        }
        // Hilfsmethode zum Schreiben mit gemeinsamem Dateizugriff
        private static void AppendLine(string path, string message)
        {
            lock (_logLock)
            {
                using var stream = new FileStream(
                    path,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.ReadWrite);
                using var writer = new StreamWriter(stream) { AutoFlush = true };
                writer.WriteLine(message);
            }
        }


        // Protokolliert eine Crash-Exception
        // Schreibt einen Absturz oder eine Ausnahme in die crash.log.

        public static void LogCrash(string label, Exception ex)
        {

            var path = Path.Combine(_logDir, "crash.log");
            AppendLine(path, $"{DateTime.Now:O} [{label}] {ex}");
        }

        // Protokolliert eine beliebige Meldung in eine Datei
        // Fügt eine beliebige Textzeile an die angegebene Logdatei an.

        public static void LogToFile(string message, string fileName = "main.log")
        {
            // Stelle sicher, dass fileName mit .log endet
            if (!fileName.EndsWith(".log", StringComparison.OrdinalIgnoreCase))
                fileName += ".log";

            var path = Path.Combine(_logDir, fileName);
            AppendLine(path, $"{DateTime.Now:O} {message}");
        }
    }
}