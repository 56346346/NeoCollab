using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using static System.Environment;


namespace NeoCollab
{
    internal static class MethodLogger
    {
        private static readonly object _logLock = new object();
        private static readonly string _logDir = Path.Combine(
            GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "NeoCollab",
            "log");
        private static readonly string _logPath =
       Path.Combine(_logDir, nameof(MethodLogger) + ".log");
        public static void InitializeLog(string className)
        {
            EnsureDir();
            var path = Path.Combine(_logDir, $"{className}.log");
            File.WriteAllText(path, string.Empty);
        }

        public static void Log(string className, string methodName, Dictionary<string, object?> parameters)
        {
            EnsureDir();
            var path = Path.Combine(_logDir, $"{className}.log");
            var paramStr = string.Join(", ", parameters.Select(kv => $"{kv.Key}={Serialize(kv.Value)}"));
            var line = $"{className}.{methodName}({paramStr})";
            lock (_logLock)
            {
                using var stream = new FileStream(
                                    path,
                                    FileMode.Append,
                                    FileAccess.Write,
                                    FileShare.ReadWrite);
                using var writer = new StreamWriter(stream) { AutoFlush = true };
                writer.WriteLine(line);
            }
        }

        private static string Serialize(object? value)
        {
            if (value == null) return "null";
            try
            {
                return JsonConvert.SerializeObject(value);
            }
            catch
            {
                return value.ToString() ?? "";
            }
        }

        private static void EnsureDir()
        {
            if (!Directory.Exists(_logDir))
                Directory.CreateDirectory(_logDir);
        }
    }
}