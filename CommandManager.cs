using System;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Threading.Tasks;
using Autodesk.Revit.DB.Events;
using Autodesk.Revit.DB;
using Neo4j.Driver;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using NeoCollab;
using System.Text.RegularExpressions;
using System.Linq;


namespace NeoCollab
{
    public class CommandManager
    {
        public ConcurrentQueue<string> cypherCommands = new ConcurrentQueue<string>();

        private Neo4jConnector _neo4jConnector;
        public Neo4jConnector Neo4jConnector => _neo4jConnector;

        // Event to signal when Neo4j operations are completed - for Solibri timing
        public static event Action<Document, List<long>, List<long>> OnNeo4jOperationsCompleted;

        // NEU: Pull-Modus Flag
        private bool _isPullInProgress = false;
        
        /// <summary>
        /// Indicates if a pull operation is currently in progress.
        /// During pull, no automatic push operations should be triggered.
        /// </summary>
        public bool IsPullInProgress 
        { 
            get => _isPullInProgress;
            set 
            {
                _isPullInProgress = value;
                Logger.LogToFile($"PULL MODE: {(value ? "ENABLED" : "DISABLED")} - Automatic push operations {(value ? "SUPPRESSED" : "ALLOWED")}", "sync.log");
            }
        }




        private readonly SemaphoreSlim _cypherLock = new(1, 1);


        private static CommandManager _instance;

        private static readonly object _lock = new object();

        public string SessionId { get; private set; }

        public DateTime LastSyncTime { get; set; } = DateTime.MinValue;
        public DateTime LastPulledAt { get; set; } = DateTime.MinValue;

        /// <summary>
        /// Indicates if this is the initial session (no previous sync time recorded).
        /// Initial sessions may require different handling to prevent UI blocking.
        /// </summary>
        public bool IsInitialSession => LastSyncTime == DateTime.MinValue;

        // Privater Konstruktor; erzeugt eine neue Instanz und initialisiert
        // Session-ID sowie den Zeitstempel der letzten Synchronisation.
        private CommandManager(Neo4jConnector neo4jConnector)
        {
            _neo4jConnector = neo4jConnector;

            SessionId = GenerateSessionId();
            LastSyncTime = LoadLastSyncTime();
            LastPulledAt = LastSyncTime;
            cypherCommands = new ConcurrentQueue<string>();
        }

        // Muss einmalig aufgerufen werden um die Singleton-Instanz anzulegen.
        public static void Initialize(Neo4jConnector neo4jConnector)
        {
            lock (_lock)
            {
                if (_instance != null)
                    throw new InvalidOperationException("CommandManager bereits initialisiert");

                _instance = new CommandManager(neo4jConnector);

            }
        }

        // Öffentliche Instanz-Eigenschaft
        // Liefert die zuvor initialisierte Singleton-Instanz.
        public static CommandManager Instance
        {
            get
            {
                lock (_lock)
                {
                    if (_instance == null)
                        throw new InvalidOperationException("CommandManager nicht initialisiert. Zuerst Initialize() aufrufen!");
                    return _instance;
                }
            }
        }

        // Öffentliche Instanz-Eigenschaft
        // Gibt die Ressourcen des Neo4jConnectors frei.

        // Removed Dispose method to prevent race conditions with active Neo4j operations
        // The .NET runtime will handle cleanup automatically

        // Überträgt alle gesammelten Cypher-Befehle an Neo4j und aktualisiert
        // anschließend den Sync-Zeitstempel. Optional wird der aktuelle
        // Revit-Status geprüft.
        public async Task ProcessCypherQueueAsync(Document currentDoc = null)
        {
            await _cypherLock.WaitAsync();
            try
            {
                if (cypherCommands.IsEmpty)
                    return;
                var commands = new List<string>();
                var changedIds = new HashSet<long>();
                var deletedIds = new HashSet<long>();
                var idRegex = new Regex(@"[eE]lementId\s*:\s*(\d+)", RegexOptions.IgnoreCase);
                
                while (cypherCommands.TryDequeue(out string cyCommand))
                {
                    commands.Add(cyCommand);
                    var matches = idRegex.Matches(cyCommand);
                    
                    // Check if this is a DELETE operation
                    bool isDeleteOperation = cyCommand.ToUpper().Contains("DELETE") || cyCommand.ToUpper().Contains("DETACH DELETE");
                    
                    foreach (Match match in matches)
                    {
                        if (long.TryParse(match.Groups[1].Value, out long parsed))
                        {
                            if (isDeleteOperation)
                                deletedIds.Add(parsed);
                            else
                                changedIds.Add(parsed);
                        }
                    }
                }

                // Critical Neo4j operations only - must complete fast
                await _neo4jConnector.PushChangesAsync(commands, SessionId, currentDoc).ConfigureAwait(false);
                LastSyncTime = DateTime.UtcNow;
                PersistSyncTime();
                await _neo4jConnector.UpdateSessionLastSyncAsync(SessionId, LastSyncTime).ConfigureAwait(false);
                
                // CRITICAL: Notify that Neo4j operations are complete - this triggers Solibri validation
                // This ensures Solibri gets the most recent data including all ChangeLog entries
                Logger.LogToFile($"NEO4J COMPLETION: Invoking OnNeo4jOperationsCompleted with {changedIds.Count} changed and {deletedIds.Count} deleted element IDs", "sync.log");
                Logger.LogToFile($"NEO4J COMPLETION: Changed IDs: [{string.Join(", ", changedIds)}]", "sync.log");
                Logger.LogToFile($"NEO4J COMPLETION: Deleted IDs: [{string.Join(", ", deletedIds)}]", "sync.log");
                OnNeo4jOperationsCompleted?.Invoke(currentDoc, changedIds.ToList(), deletedIds.ToList());
                
                // Non-critical operations - run in background to prevent shutdown race conditions
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await _neo4jConnector.CleanupObsoleteChangeLogsAsync().ConfigureAwait(false);
                        if (currentDoc != null)
                        {
                            // NOTE: Solibri checking is now handled by SolibriValidationService in GraphPuller
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogCrash("ProcessCypherQueueAsync background", ex);
                    }
                });

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Neo4j-Error] {ex.Message}");
                Logger.LogCrash("ProcessCypherQueueAsync", ex);
            }
            finally
            {
                _cypherLock.Release();
            }
        }
        // Schreibt den aktuellen LastSyncTime-Wert in eine Datei im
        // Benutzerprofil.
        public void PersistSyncTime()
        {
            try
            {
                var dir = System.IO.Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    "NeoCollab");
                System.IO.Directory.CreateDirectory(dir);

                var file = System.IO.Path.Combine(dir, $"last_sync_{SessionId}.txt");
                System.IO.File.WriteAllText(file, LastSyncTime.ToString("o"));
            }
            catch (Exception ex)
            {
                Logger.LogCrash("PersistSyncTime", ex);
            }
        }
        // Generiert eine eindeutige Session-ID basierend auf dem Benutzernamen
        private static string GenerateSessionId()
        {
            string processId = Environment.ProcessId.ToString();
            return $"{processId}_{Guid.NewGuid().ToString().Substring(0, 8)}";
        }
        // Liest einen zuvor gespeicherten Sync-Zeitstempel aus der Datei oder
        // liefert DateTime.Now wenn keiner vorhanden ist. 
        private DateTime LoadLastSyncTime()
        {
            try
            {
                var dir = System.IO.Path.Combine(
                   Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    "NeoCollab");

                var sessionFile = System.IO.Path.Combine(dir, $"last_sync_{SessionId}.txt");
                var globalFile = System.IO.Path.Combine(dir, "last_sync.txt");

                string path = System.IO.File.Exists(sessionFile) ? sessionFile : globalFile;

                if (System.IO.File.Exists(path))
                {
                    var content = System.IO.File.ReadAllText(path);
                    if (DateTime.TryParse(content, out var ts))
                        return ts;
                }
            }
            catch { }

            return DateTime.MinValue;
        }

        /// <summary>
        /// Triggers the OnNeo4jOperationsCompleted event manually.
        /// Used by GraphPuller to notify Solibri after pull operations.
        /// </summary>
        public static void TriggerNeo4jOperationsCompleted(Document doc, List<long> changedElementIds, List<long> deletedElementIds)
        {
            try
            {
                Logger.LogToFile($"MANUAL NEO4J COMPLETION: Triggering OnNeo4jOperationsCompleted manually with {changedElementIds.Count} changed and {deletedElementIds.Count} deleted element IDs", "sync.log");
                OnNeo4jOperationsCompleted?.Invoke(doc, changedElementIds, deletedElementIds);
                Logger.LogToFile($"MANUAL NEO4J COMPLETION: Successfully triggered OnNeo4jOperationsCompleted event", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"MANUAL NEO4J COMPLETION ERROR: Failed to trigger OnNeo4jOperationsCompleted: {ex.Message}", "sync.log");
                Logger.LogCrash("Manual Neo4j completion trigger failed", ex);
            }
        }

    }
}