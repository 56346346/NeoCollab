using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Neo4j.Driver;
using System.Diagnostics;
using System.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Plumbing;
using Autodesk.Revit.DB.Events;
using System.IO;
using NeoCollab;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using static System.Environment;


namespace NeoCollab
{
    public interface INeo4jConnector
    {
        Task RunWriteQueryAsync(string query, object parameters = null);
    }


    public class Neo4jConnector : IDisposable, INeo4jConnector
    {
        private static readonly string _logDir =
            Path.Combine(GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "NeoCollab", "log");
        private static readonly string _logPath =
            Path.Combine(_logDir, nameof(Neo4jConnector) + ".log");
        private static void LogMethodCall(string methodName, Dictionary<string, object?> parameters)
        {
            MethodLogger.Log(nameof(Neo4jConnector), methodName, parameters);
        }
        private readonly IDriver _driver;
        private readonly Microsoft.Extensions.Logging.ILogger<Neo4jConnector> _logger;
        private const string CommandLogFile = "neo4j_commands.log";
        
        // Public access to the Neo4j driver for external components
        public IDriver Driver => _driver;
        private readonly string _cypherFilePath = Path.Combine(
    Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
    "NeoCollab",
    "neo4j_cypher.txt"
);
        // Stellt die Verbindung zu Neo4j her. Die Zugangsdaten können optional
        // angepasst werden.
        public Neo4jConnector(Microsoft.Extensions.Logging.ILogger<Neo4jConnector> logger, string uri = "bolt://localhost:7687",
                                string user = "neo4j",
                               string password = "password")
        {
            _logger = logger;
            _driver = GraphDatabase.Driver(
                            uri,
                            AuthTokens.Basic(user, password),
                            o => o.WithConnectionTimeout(TimeSpan.FromSeconds(15))
                                  .WithMaxConnectionPoolSize(50)
                        );
            // Ensure uniqueness constraints exist so elements are not duplicated
            // when multiple users push the same wall.
            try
            {
                EnsureConstraintsAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to ensure constraints");
            }
        }


        private async Task EnsureConstraintsAsync()
        {
            const string c1 = "CREATE INDEX wall_uid IF NOT EXISTS FOR (w:Wall) ON (w.uid)";
            const string c2 = "CREATE INDEX door_uid IF NOT EXISTS FOR (d:Door) ON (d.uid)";
            const string c3 = "CREATE INDEX pipe_uid IF NOT EXISTS FOR (p:Pipe) ON (p.uid)";
            const string c4 = "CREATE CONSTRAINT ps_guid IF NOT EXISTS FOR (ps:ProvisionalSpace) REQUIRE ps.guid IS UNIQUE";
            await using var session = _driver.AsyncSession();
            await session.ExecuteWriteAsync(async tx =>
            {
                await tx.RunAsync(c1).ConfigureAwait(false);
                await tx.RunAsync(c2).ConfigureAwait(false);
                await tx.RunAsync(c3).ConfigureAwait(false);
                await tx.RunAsync(c4).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }
        // Führt eine Leseabfrage aus und gibt die Ergebnismenge zurück.

        public async Task<List<IRecord>> RunReadQueryAsync(string query, object parameters = null)
        {
            LogMethodCall(nameof(RunReadQueryAsync), new()
            {
                ["query"] = query,
                ["parameters"] = parameters
            });
            await using var session = _driver.AsyncSession();
            try
            {
                var resultCursor = parameters == null
                     ? await session.RunAsync(query).ConfigureAwait(false)
                    : await session.RunAsync(query, parameters).ConfigureAwait(false);
                return await resultCursor.ToListAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Neo4j-Fehler] Lese-Query fehlgeschlagen: {ex.Message}");
                Logger.LogCrash("RunReadQueryAsync", ex);
                throw;
            }
        }

        // Schreibt alle Änderungen in einer Transaktion nach Neo4j und legt für
        // jedes Element einen Log-Eintrag an.
        public async Task PushChangesAsync(IEnumerable<string> cypherCommands, string sessionId, Autodesk.Revit.DB.Document currentDocument = null)
        {
            var startTime = DateTime.Now;
            var commandList = cypherCommands.ToList();
            
            Logger.LogToFile($"PUSH STARTED: Session {sessionId} with {commandList.Count} commands at {startTime:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");
            
            LogMethodCall(nameof(PushChangesAsync), new()
            {
                ["commandCount"] = commandList.Count,
                ["sessionId"] = sessionId
            });
            
            // 1) Asynchrone Neo4j-Session öffnen
            await using var session = _driver.AsyncSession();
            Logger.LogToFile($"PUSH NEO4J SESSION: Opened database session for {commandList.Count} commands", "sync.log");
            Logger.LogToFile($"BEGIN push {commandList.Count} commands", CommandLogFile);
            try
            {
                // 2) Transaction starten
                Logger.LogToFile("PUSH TRANSACTION: Starting Neo4j transaction", "sync.log");
                var tx = await session.BeginTransactionAsync().ConfigureAwait(false);

                // ─────────────────────────────────────────────────────────
                // ► SESSION-KNOTEN ERSTELLEN / UPDATEN (MERGE ganz am Anfang)
                // ─────────────────────────────────────────────────────────
                var initTime = DateTime.Now.ToString("o");
                Logger.LogToFile($"PUSH SESSION UPDATE: Creating/updating session node {sessionId}", "sync.log");
                await tx.RunAsync(
                  @"MERGE (s:Session { id: $session })
               SET s.lastSync = datetime($time),
                  s.lastUpdate = datetime($time)",
                  new
                  {
                      session = sessionId,
                      time = initTime
                  }
              ).ConfigureAwait(false);

                // 3) Improved Regex zum Extrahieren der ElementId aus dem Cypher-String  
                // Handles patterns like: "elementId = 123", "p.elementId = 456", "elementId: 789", "{elementId: 123}"
                var idRegex = new Regex(@"(?:\.)?elementId\s*[=:]\s*(\d+)", RegexOptions.IgnoreCase);
                // 4) Alle Commands durchlaufen
                int commandIndex = 0;
                foreach (var cmd in commandList)
                {
                    commandIndex++;
                    Logger.LogToFile($"PUSH COMMAND {commandIndex}/{commandList.Count}: Executing Cypher command", "sync.log");
                    Logger.LogToFile($"RUN {cmd}", CommandLogFile);
                    try
                    {
                        // 4.1) Änderungsbefehl ausführen – jetzt mit $session-Parameter
                        await tx.RunAsync(cmd, new { session = sessionId }).ConfigureAwait(false);
                        Logger.LogToFile($"PUSH COMMAND {commandIndex} SUCCESS: Command executed successfully", "sync.log");
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"PUSH COMMAND {commandIndex} ERROR: {ex.Message} for {cmd}", "sync.log");
                        Logger.LogToFile($"ERROR {ex.Message} for {cmd}", CommandLogFile);
                        Logger.LogCrash("PushChangesAsync", ex);
                        throw;
                    }
                    // 4.2) CRITICAL FIX: DISABLE automatic ChangeLog creation in RunWriteQueryAsync
                    // This was causing self-pull problems where sessions pulled their own ChangeLog entries
                    // ChangeLog creation is now ONLY handled by SpaceExtractor.CreateChangeLogForElement for cross-session sync
                    Logger.LogToFile($"PUSH COMMAND {commandIndex} CHANGLOG SKIP: Automatic ChangeLog creation disabled to prevent self-pull conflicts", "sync.log");
                    
                    /* DISABLED AUTOMATIC CHANGELOG CREATION - CAUSES SELF-PULL BUGS
                    string changeType;
                    if (cmd.Contains("DELETE", StringComparison.OrdinalIgnoreCase))
                        changeType = "Delete";
                    else if (cmd.Contains("MERGE", StringComparison.OrdinalIgnoreCase))
                        changeType = "Insert";
                    else
                        changeType = "Modify";

                    // 4.3) Alle ElementIds extrahieren (es können mehrere pro Command sein)
                    var matches = idRegex.Matches(cmd);
                    var elementIds = new List<long>();
                    /* DISABLED AUTOMATIC CHANGELOG CREATION - CAUSES SELF-PULL BUGS
                    
                    foreach (Match match in matches)
                    {
                        if (match.Success && long.TryParse(match.Groups[1].Value, out var parsedId))
                        {
                            elementIds.Add(parsedId);
                            Logger.LogToFile($"ELEMENT ID EXTRACTION SUCCESS: Found elementId {parsedId} in command: {cmd.Substring(0, Math.Min(100, cmd.Length))}...", "sync.log");
                        }
                    }
                    
                    // Remove duplicates to prevent multiple ChangeLog entries for same elementId
                    elementIds = elementIds.Distinct().ToList();
                    
                    Logger.LogToFile($"ELEMENT ID EXTRACTION SUMMARY: Found {elementIds.Count} unique elementIds: [{string.Join(", ", elementIds)}] in command", "sync.log");
                    
                    if (elementIds.Count == 0)
                    {
                        Logger.LogToFile($"ELEMENT ID EXTRACTION FAILED: No elementIds found in command, SKIPPING ChangeLog creation: {cmd.Substring(0, Math.Min(150, cmd.Length))}...", "sync.log");
                        Logger.LogToFile($"REGEX DEBUG: Pattern='{idRegex}', Matches={matches.Count}", "sync.log");
                        continue;
                    }

                    // 4.4) Create ChangeLog for each extracted elementId
                    foreach (var elementId in elementIds)
                    {
                        Logger.LogToFile($"CHANGELOG PROCESSING: Starting ChangeLog creation for elementId {elementId}, operation {changeType}", "sync.log");
                        var logTime = DateTime.UtcNow.ToString("o");
                        
                        try
                        {
                            await CreateChangeLogEntryInTransactionAsync(tx, elementId, changeType, sessionId);
                            Logger.LogToFile($"CHANGELOG CREATION SUCCESS for elementId {elementId}, operation {changeType}", "sync.log");
                        }
                        catch (Exception logEx)
                        {
                            Logger.LogToFile($"CHANGELOG CREATION FAILED for elementId {elementId}: {logEx.Message}", "sync.log");
                            // Continue with next elementId even if this one fails
                        }

                        // lastModifiedUtc setzen
                        if (elementId >= 0)
                        {
                            await tx.RunAsync(
                                "MATCH (e { elementId: $id }) SET e.lastModifiedUtc = datetime($time)",
                                new { id = elementId, time = logTime }).ConfigureAwait(false);

                            // Level-Beziehungen und ChangeLogs für verschiedene Element-Typen aktualisieren
                            // DISABLED: Level ChangeLogs moved to batch processing to prevent duplicates
                            // if (currentDocument != null)
                            // {
                            //     await HandleElementLevelRelationshipsAsync(tx, cmd, changeType, elementId, logTime, sessionId, currentDocument).ConfigureAwait(false);
                            // }
                        }
                    }
                    
                    END OF DISABLED AUTOMATIC CHANGELOG CREATION */
                }

                // 5) Create missing GOT_CHANGED relationships for newly created elements
                Logger.LogToFile("PUSH RELATIONSHIPS: Creating GOT_CHANGED relationships for new elements", "sync.log");
                await CreateMissingGotChangedRelationshipsAsync(tx, sessionId).ConfigureAwait(false);

                // 6) Transaction committen
                Logger.LogToFile("PUSH TRANSACTION COMMIT: Committing Neo4j transaction", "sync.log");
                await tx.CommitAsync().ConfigureAwait(false);
                var duration = DateTime.Now - startTime;
                Logger.LogToFile($"PUSH COMPLETED: Successfully committed {commandList.Count} commands in {duration.TotalMilliseconds:F0}ms", "sync.log");
                Logger.LogToFile($"COMMIT {commandList.Count} commands", CommandLogFile);
                Debug.WriteLine($"[Neo4j] PushChanges: {commandList.Count} Änderungen übertragen und protokolliert.");

            }
            catch (Exception ex)
            {
                var duration = DateTime.Now - startTime;
                Debug.WriteLine($"[Neo4j Push Fehler] {ex.Message}");
                Logger.LogToFile($"PUSH FAILED: Transaction failed after {duration.TotalMilliseconds:F0}ms - {ex.Message}", "sync.log");
                Logger.LogToFile($"FAIL {ex.Message}", CommandLogFile);
                Logger.LogCrash("PushChangesAsync", ex);

                // 6) Bei Fehler: Rollback
                try
                {
                    Logger.LogToFile("PUSH ROLLBACK: Attempting to close session after error", "sync.log");
                    await session.CloseAsync().ConfigureAwait(false); // Session sauber schließen
                }
                catch { /* ignore */ }
                throw;
            }
            finally
            {
                // 7) Session schließen
                Logger.LogToFile("PUSH CLEANUP: Closing Neo4j session", "sync.log");
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        // Markiert alle fremden ChangeLogs als gelesen.
        public async Task AcknowledgeAllAsync(string currentSession)
        {
            await using var session = _driver.AsyncSession();
            try
            {
                await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(@"MATCH (c:ChangeLog)
WHERE c.sessionId <> $session AND c.acknowledged = false
SET c.acknowledged = true", new { session = currentSession }).ConfigureAwait(false);
                });
            }
            finally
            {
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        // Bestätigt nur Logs der angegebenen Elemente.
        public async Task RunWriteQueryAsync(string query, object parameters = null)
        {
            LogMethodCall(nameof(RunWriteQueryAsync), new()
            {
                ["query"] = query,
                ["parameters"] = parameters
            });
            await using var session = _driver.AsyncSession();
            try
            {
                await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(query, parameters).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            finally
            {
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        // Löscht alte ChangeLogs anhand des kleinsten Session-Zeitstempels.

        public async Task CleanupObsoleteChangeLogsAsync()
        {
            await using var session = _driver.AsyncSession();
            try
            {
                // Alte Sessions bereinigen, damit die Mindestberechnung korrekt bleibt
                await RemoveStaleSessionsAsync(TimeSpan.FromDays(1)).ConfigureAwait(false);

                await session.ExecuteWriteAsync(async tx =>
                {
                    // 1) ältesten Zeitstempel aller Sessions ermitteln
                    var res = await tx.RunAsync(
                        "MATCH (s:Session) RETURN min(s.lastSync) AS cutoff").ConfigureAwait(false);
                    var record = await res.SingleAsync().ConfigureAwait(false);
                    var cutoff = record["cutoff"].As<ZonedDateTime>().ToString();

                    // 2) ChangeLogs löschen, die älter sind
                    await tx.RunAsync(
                        @"MATCH (cl:ChangeLog)
                     WHERE cl.timestamp < datetime($cutoff)
                    DELETE cl",
                        new { cutoff = cutoff.ToString() }).ConfigureAwait(false);
                });
            }
            finally
            {
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        // Entfernt Session-Knoten die länger nicht synchronisiert wurden.

        public async Task RemoveStaleSessionsAsync(TimeSpan maxAge)
        {
            await using var session = _driver.AsyncSession();
            try
            {
                await session.ExecuteWriteAsync(async tx =>
                {
                    var cutoff = DateTime.UtcNow.Subtract(maxAge).ToString("o");
                    await tx.RunAsync(
                        @"MATCH (s:Session)
                          WHERE s.lastSync < datetime($cutoff)
                          DETACH DELETE s",
                        new { cutoff }).ConfigureAwait(false);
                });
            }
            finally
            {
                await session.CloseAsync().ConfigureAwait(false);
            }
        }
        // Aktualisiert das lastSync-Datum einer Session in Neo4j.
        public async Task UpdateSessionLastSyncAsync(string sessionId, DateTime syncTime)
        {
            LogMethodCall(nameof(UpdateSessionLastSyncAsync), new()
            {
                ["sessionId"] = sessionId,
                ["syncTime"] = syncTime
            });
            await using var session = _driver.AsyncSession();
            try
            {
                await session.ExecuteWriteAsync(async tx =>
                {
                    await tx.RunAsync(
                        @"MERGE (s:Session { id: $session })
                          SET s.lastSync = datetime($time)",
                        new { session = sessionId, time = syncTime.ToString("o") }
                    ).ConfigureAwait(false);
                });
            }
            finally
            {
                await session.CloseAsync().ConfigureAwait(false);
            }
        }

        public async Task<DateTime> GetLastUpdateTimestampAsync(string currentSession)
        {
            LogMethodCall(nameof(GetLastUpdateTimestampAsync), new() { ["currentSession"] = currentSession });

            const string query = @"MATCH (s:Session)
WHERE s.id <> $session
RETURN max(s.lastUpdate) AS lastUpdate";
            var records = await RunReadQueryAsync(query, new { session = currentSession }).ConfigureAwait(false);
            var rec = records.FirstOrDefault();
            if (rec != null && rec["lastUpdate"] != null)
                return rec["lastUpdate"].As<ZonedDateTime>().ToDateTimeOffset().UtcDateTime;
            return DateTime.MinValue;
        }
        /// <summary>
        /// CENTRAL AND ONLY METHOD: Creates a ChangeLog entry with proper HAS_LOG and GOT_CHANGED relationships
        /// This is the ONLY method that should be used for creating ChangeLog entries
        /// </summary>
        // Transaction-aware version that reuses existing transaction
        private async Task CreateChangeLogEntryInTransactionAsync(IAsyncTransaction tx, long elementId, string operation, string sessionId)
        {
            // Skip invalid elementIds
            if (elementId <= 0)
            {
                Logger.LogToFile($"SKIPPING ChangeLog creation for invalid elementId: {elementId}", "sync.log");
                return;
            }

            const string cypher = @"
// First check if the element actually exists in the database
OPTIONAL MATCH (wall:Wall) WHERE wall.elementId = $eid OR wall.elementId = toString($eid)
OPTIONAL MATCH (door:Door) WHERE door.elementId = $eid OR door.elementId = toString($eid)
OPTIONAL MATCH (pipe:Pipe) WHERE pipe.elementId = $eid OR pipe.elementId = toString($eid)
OPTIONAL MATCH (space:ProvisionalSpace) WHERE space.elementId = $eid OR space.elementId = toString($eid)
OPTIONAL MATCH (level:Level) WHERE level.elementId = $eid OR level.elementId = toString($eid)
OPTIONAL MATCH (building:Building) WHERE building.elementId = $eid OR building.elementId = toString($eid)

// Always create ChangeLog (even if element doesn't exist yet - it will be created in the same transaction)
MERGE (s:Session { id: $session })
MERGE (cl:ChangeLog {
    sessionId: $session,
    elementId: $eid,
    type: $type
})
ON CREATE SET 
    cl.user = $session,
    cl.timestamp = datetime(),
    cl.acknowledged = false
ON MATCH SET 
    cl.timestamp = datetime(),
    cl.acknowledged = false,
    cl.user = $session
MERGE (s)-[:HAS_LOG]->(cl)

// Create GOT_CHANGED relationships only for existing elements
WITH cl, wall, door, pipe, space, level, building
FOREACH (elem IN CASE WHEN wall IS NOT NULL THEN [wall] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN door IS NOT NULL THEN [door] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN pipe IS NOT NULL THEN [pipe] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN space IS NOT NULL THEN [space] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN level IS NOT NULL THEN [level] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN building IS NOT NULL THEN [building] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))

RETURN id(cl) as changeId, 
       CASE WHEN wall IS NOT NULL THEN 'Wall' ELSE null END as wall_found,
       CASE WHEN door IS NOT NULL THEN 'Door' ELSE null END as door_found,
       CASE WHEN pipe IS NOT NULL THEN 'Pipe' ELSE null END as pipe_found,
       CASE WHEN space IS NOT NULL THEN 'ProvisionalSpace' ELSE null END as space_found,
       CASE WHEN level IS NOT NULL THEN 'Level' ELSE null END as level_found,
       CASE WHEN building IS NOT NULL THEN 'Building' ELSE null END as building_found";

            try
            {
                var result = await tx.RunAsync(cypher, new { session = sessionId, type = operation, eid = elementId });
                
                if (await result.FetchAsync())
                {
                    var record = result.Current;
                    var changeId = record["changeId"].As<int>();
                    var wallFound = record["wall_found"]?.ToString();
                    var doorFound = record["door_found"]?.ToString();
                    var pipeFound = record["pipe_found"]?.ToString();
                    var spaceFound = record["space_found"]?.ToString();
                    var levelFound = record["level_found"]?.ToString();
                    var buildingFound = record["building_found"]?.ToString();
                    
                    var elementTypes = new[] { wallFound, doorFound, pipeFound, spaceFound, levelFound, buildingFound }.Where(t => t != null).ToArray();
                    var foundElements = elementTypes.Any() ? string.Join(", ", elementTypes) : "NONE";
                    
                    Logger.LogToFile($"CHANGELOG CREATION SUCCESS for elementId {elementId}, operation {operation} in existing transaction. Found: {foundElements}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"CHANGELOG CREATION SUCCESS for elementId {elementId}, operation {operation} in existing transaction (element will be created later)", "sync.log");
                }
            }
            catch (Exception ex)
            {
                Logger.LogCrash($"Failed to create ChangeLog for elementId {elementId} in transaction", ex);
                throw;
            }
        }

        // Creates missing GOT_CHANGED relationships for ChangeLogs that don't have them yet
        private async Task CreateMissingGotChangedRelationshipsAsync(IAsyncTransaction tx, string sessionId)
        {
            const string cypher = @"
// Find recent ChangeLogs without GOT_CHANGED relationships (within last 5 minutes)
// Skip those already marked as deleteProcessed
MATCH (cl:ChangeLog)
WHERE NOT EXISTS((cl)-[:GOT_CHANGED]->()) 
  AND cl.timestamp > datetime() - duration('PT5M')
  AND cl.deleteProcessed IS NULL

// For each ChangeLog, find the corresponding element and create relationship
WITH cl
OPTIONAL MATCH (wall:Wall) WHERE wall.elementId = cl.elementId OR wall.elementId = toString(cl.elementId)
OPTIONAL MATCH (door:Door) WHERE door.elementId = cl.elementId OR door.elementId = toString(cl.elementId)
OPTIONAL MATCH (pipe:Pipe) WHERE pipe.elementId = cl.elementId OR pipe.elementId = toString(cl.elementId)
OPTIONAL MATCH (space:ProvisionalSpace) WHERE space.elementId = cl.elementId OR space.elementId = toString(cl.elementId)
OPTIONAL MATCH (level:Level) WHERE level.elementId = cl.elementId OR level.elementId = toString(cl.elementId)
OPTIONAL MATCH (building:Building) WHERE building.elementId = cl.elementId OR building.elementId = toString(cl.elementId)

// Create relationships for found elements
WITH cl, wall, door, pipe, space, level, building
FOREACH (elem IN CASE WHEN wall IS NOT NULL THEN [wall] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN door IS NOT NULL THEN [door] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN pipe IS NOT NULL THEN [pipe] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN space IS NOT NULL THEN [space] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN level IS NOT NULL THEN [level] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN building IS NOT NULL THEN [building] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))

// For Delete operations where element no longer exists, mark as processed to prevent retry
WITH cl, wall, door, pipe, space, level, building
FOREACH (ignored IN CASE WHEN cl.type = 'Delete' AND wall IS NULL AND door IS NULL AND pipe IS NULL AND space IS NULL AND level IS NULL AND building IS NULL THEN [1] ELSE [] END |
  SET cl.deleteProcessed = true
)

RETURN count(cl) as processedChangeLogs, 
       count(wall) as wallsLinked,
       count(door) as doorsLinked,
       count(pipe) as pipesLinked,
       count(space) as spacesLinked,
       count(level) as levelsLinked,
       count(building) as buildingsLinked";

            try
            {
                var result = await tx.RunAsync(cypher, new { session = sessionId });
                
                if (await result.FetchAsync())
                {
                    var record = result.Current;
                    var processedChangeLogs = record["processedChangeLogs"].As<int>();
                    var wallsLinked = record["wallsLinked"].As<int>();
                    var doorsLinked = record["doorsLinked"].As<int>();
                    var pipesLinked = record["pipesLinked"].As<int>();
                    var spacesLinked = record["spacesLinked"].As<int>();
                    var levelsLinked = record["levelsLinked"].As<int>();
                    var buildingsLinked = record["buildingsLinked"].As<int>();
                    
                    Logger.LogToFile($"GOT_CHANGED RELATIONSHIPS: Processed {processedChangeLogs} ChangeLogs. Linked: {wallsLinked} walls, {doorsLinked} doors, {pipesLinked} pipes, {spacesLinked} spaces, {levelsLinked} levels, {buildingsLinked} buildings", "sync.log");
                }
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to create missing GOT_CHANGED relationships", ex);
                throw;
            }
        }

        /// <summary>
        /// CENTRAL AND ONLY METHOD: Creates a ChangeLog entry with proper HAS_LOG and GOT_CHANGED relationships
        /// This is the ONLY method that should be used for creating ChangeLog entries
        /// </summary>
        public async Task CreateChangeLogEntryWithRelationshipsAsync(long elementId, string operation, string targetSessionId)
        {
            // Skip invalid elementIds
            if (elementId <= 0)
            {
                Logger.LogToFile($"SKIPPING ChangeLog creation for invalid elementId: {elementId}", "sync.log");
                return;
            }

            // CRITICAL FIX: originalSession is the CURRENT session that is making the change (not from Neo4j)
            // When Session A builds/modifies an element, Session A IS the original session for that change
            string originalSessionId = CommandManager.Instance.SessionId;
            
            Logger.LogToFile($"CHANGELOG CREATION: Starting for elementId {elementId}, operation {operation}, targetSession {targetSessionId}, originalSession {originalSessionId}", "sync.log");

            const string cypher = @"
// First check if the element actually exists in the database
OPTIONAL MATCH (wall:Wall) WHERE wall.elementId = $eid OR wall.elementId = toString($eid)
OPTIONAL MATCH (door:Door) WHERE door.elementId = $eid OR door.elementId = toString($eid)
OPTIONAL MATCH (pipe:Pipe) WHERE pipe.elementId = $eid OR pipe.elementId = toString($eid)
OPTIONAL MATCH (space:ProvisionalSpace) WHERE space.elementId = $eid OR space.elementId = toString($eid)
OPTIONAL MATCH (level:Level) WHERE level.elementId = $eid OR level.elementId = toString($eid)
OPTIONAL MATCH (building:Building) WHERE building.elementId = $eid OR building.elementId = toString($eid)

// CRITICAL FIX: ALWAYS create ChangeLog, regardless of element existence
MERGE (targetSession:Session { id: $targetSession })
CREATE (cl:ChangeLog {
    sessionId: $targetSession,
    targetSessionId: $targetSession,
    originalSessionId: $originalSession,
    user: $originalSession,
    timestamp: datetime(),
    type: $type,
    elementId: $eid,
    acknowledged: false,
    op: $type
})
MERGE (targetSession)-[:HAS_LOG]->(cl)

// Create GOT_CHANGED relationships for existing elements
WITH cl, wall, door, pipe, space, level, building
FOREACH (elem IN CASE WHEN wall IS NOT NULL THEN [wall] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN door IS NOT NULL THEN [door] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN pipe IS NOT NULL THEN [pipe] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN space IS NOT NULL THEN [space] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN level IS NOT NULL THEN [level] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN building IS NOT NULL THEN [building] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))

// For Delete operations where element no longer exists, mark as processed
WITH cl, wall, door, pipe, space, level, building
FOREACH (ignored IN CASE WHEN $type = 'Delete' AND wall IS NULL AND door IS NULL AND pipe IS NULL AND space IS NULL AND level IS NULL AND building IS NULL THEN [1] ELSE [] END |
  SET cl.deleteProcessed = true
)

RETURN id(cl) as changeId, 
       $targetSession as targetSession,
       $originalSession as originalSession,
       CASE WHEN wall IS NOT NULL THEN 'Wall' ELSE null END as wall_found,
       CASE WHEN door IS NOT NULL THEN 'Door' ELSE null END as door_found,
       CASE WHEN pipe IS NOT NULL THEN 'Pipe' ELSE null END as pipe_found,
       CASE WHEN space IS NOT NULL THEN 'ProvisionalSpace' ELSE null END as space_found,
       CASE WHEN level IS NOT NULL THEN 'Level' ELSE null END as level_found,
       CASE WHEN building IS NOT NULL THEN 'Building' ELSE null END as building_found";

            try
            {
                await using var session = _driver.AsyncSession();
                var result = await session.RunAsync(cypher, new { 
                    targetSession = targetSessionId, 
                    originalSession = originalSessionId, 
                    type = operation, 
                    eid = elementId 
                });
                
                // CRITICAL: Cypher should ALWAYS return a result since we CREATE the ChangeLog
                var record = await result.SingleAsync();
                var changeId = record["changeId"].As<int>();
                var targetSession = record["targetSession"].As<string>();
                var originalSession = record["originalSession"].As<string>();
                var wallFound = record["wall_found"]?.ToString();
                var doorFound = record["door_found"]?.ToString();
                var pipeFound = record["pipe_found"]?.ToString();
                var spaceFound = record["space_found"]?.ToString();
                var levelFound = record["level_found"]?.ToString();
                var buildingFound = record["building_found"]?.ToString();
                
                var elementTypes = new[] { wallFound, doorFound, pipeFound, spaceFound, levelFound, buildingFound }.Where(t => t != null).ToArray();
                var foundElements = elementTypes.Any() ? string.Join(", ", elementTypes) : "NEW_ELEMENT";
                
                Logger.LogToFile($"CHANGELOG CREATION SUCCESS: ChangeLog {changeId} created for elementId {elementId} ({operation}) - targetSession: {targetSession}, originalSession: {originalSession}, elements: {foundElements}", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash($"Failed to create ChangeLog for elementId {elementId}", ex);
                throw;
            }
        }

        public async Task CreateLogChangeAsync(long elementId, ChangeType type, string sessionId)
        {
            // Redirect to central method
            await CreateChangeLogEntryWithRelationshipsAsync(elementId, type.ToString(), sessionId);
        }
        // Schreibt eine Tür in Neo4j (INSERT/UPDATE).


        // ------------------------------------------------------------------
        // Neue API für Node-basierten Datenaustausch
        // Helfer für generische Abfragen mit Mapping-Funktion.

        public async Task<List<T>> RunQueryAsync<T>(string cypher, object parameters, Func<IRecord, T> map)
        {
            await using var session = _driver.AsyncSession();
            _logger.LogDebug("RunQueryAsync: {Cypher}", cypher);
            var cursor = await session.RunAsync(cypher, parameters).ConfigureAwait(false);
            var list = new List<T>();
            await cursor.ForEachAsync(r => list.Add(map(r))).ConfigureAwait(false);
            return list;
        }
        // Erstellt oder aktualisiert eine Wand in Neo4j.


        // Holt alle seit einem Zeitpunkt geänderten Wände.

        public async Task<List<WallNode>> GetUpdatedWallsAsync(DateTime sinceUtc)
        {
            const string cypher = @"MATCH (w:Wall)
WHERE  w.lastModifiedUtc > datetime($since)
RETURN w";
            _logger.LogDebug("GetUpdatedWalls since {Time}", sinceUtc);
            var list = await RunQueryAsync(cypher, new { since = sinceUtc.ToString("o") }, r =>
            {
                var node = r["w"].As<INode>();
                return new WallNode
                (
                    node.Properties.ContainsKey("uid") ? node.Properties["uid"].As<string>() : string.Empty,
                    node.Properties.ContainsKey("elementId") ? node.Properties["elementId"].As<long>() : 
                        (node.Properties.ContainsKey("ElementId") ? node.Properties["ElementId"].As<long>() : -1),
                    node.Properties["typeId"].As<long>(),
                    node.Properties.TryGetValue("typeName", out var typeName) ? typeName.As<string>() : string.Empty,
                    node.Properties.TryGetValue("familyName", out var familyName) ? familyName.As<string>() : string.Empty,
                    node.Properties["levelId"].As<long>(),
                    node.Properties["x1"].As<double>(),
                    node.Properties["y1"].As<double>(),
                    node.Properties["z1"].As<double>(),
                    node.Properties["x2"].As<double>(),
                    node.Properties["y2"].As<double>(),
                    node.Properties["z2"].As<double>(),
                    node.Properties["height_mm"].As<double>(),
                    node.Properties["thickness_mm"].As<double>(),
                    node.Properties["structural"].As<bool>(),
                    node.Properties.TryGetValue("flipped", out var flipped) && flipped.As<bool>(),
                    node.Properties.TryGetValue("base_offset_mm", out var baseOffset) ? baseOffset.As<double>() : 0.0,
 node.Properties.TryGetValue("location_line", out var locLine) ? locLine.As<int>() : (int)WallLocationLine.WallCenterline,
                    node.Properties.TryGetValue("rvtClass", out var cls) ? cls.As<string>() : "Wall");
            }).ConfigureAwait(false);
            _logger.LogInformation("Pulled {Count} walls", list.Count);
            return list;
        }

        // Holt alle seit einem Zeitpunkt geänderten Türen.
        public async Task<List<DoorNode>> GetUpdatedDoorsAsync(DateTime sinceUtc)
        {
            const string cypher = @"MATCH (d:Door)
WHERE  d.lastModifiedUtc > datetime($since)
RETURN d";
            _logger.LogDebug("GetUpdatedDoors since {Time}", sinceUtc);
            var list = await RunQueryAsync(cypher, new { since = sinceUtc.ToString("o") }, r =>
            {
                var node = r["d"].As<INode>();
                return new DoorNode
                (
                                        node.Properties.TryGetValue("name", out var name) ? name.As<string>() : string.Empty,
                    node.Properties.ContainsKey("uid") ? node.Properties["uid"].As<string>() : string.Empty,
                    node.Properties.ContainsKey("elementId") ? node.Properties["elementId"].As<long>() : 
                        (node.Properties.ContainsKey("ElementId") ? node.Properties["ElementId"].As<long>() : -1),
                    node.Properties["typeId"].As<long>(),
                    node.Properties.TryGetValue("familyName", out var famName) ? famName.As<string>() : string.Empty,
                    node.Properties.TryGetValue("symbolName", out var symName) ? symName.As<string>() : string.Empty,
                    node.Properties["levelId"].As<long>(),
                    node.Properties.TryGetValue("hostId", out var hostId) ? hostId.As<long>() : -1,
                    node.Properties.TryGetValue("hostUid", out var hostUid) ? hostUid.As<string>() : string.Empty,
                    node.Properties.TryGetValue("x", out var x) ? x.As<double>() : 0.0,
                    node.Properties.TryGetValue("y", out var y) ? y.As<double>() : 0.0,
                    node.Properties.TryGetValue("z", out var z) ? z.As<double>() : 0.0,
                    node.Properties.TryGetValue("rotation", out var rot) ? rot.As<double>() : 0.0,
                    node.Properties.TryGetValue("width", out var width) ? width.As<double>() : 0.0,
                    node.Properties.TryGetValue("height", out var height) ? height.As<double>() : 0.0,
                    node.Properties.TryGetValue("thickness", out var thickness) ? thickness.As<double>() : 0.0,
                    node.Properties.TryGetValue("rvtClass", out var cls) ? cls.As<string>() : "Door");
            }).ConfigureAwait(false);
            _logger.LogInformation("Pulled {Count} doors", list.Count);
            return list;
        }

        // Holt alle seit einem Zeitpunkt geänderten Rohre.
        public async Task<List<PipeNode>> GetUpdatedPipesAsync(DateTime sinceUtc)
        {
            const string cypher = @"MATCH (p:Pipe)
WHERE  p.lastModifiedUtc > datetime($since)
RETURN p";
            _logger.LogDebug("GetUpdatedPipes since {Time}", sinceUtc);
            var list = await RunQueryAsync(cypher, new { since = sinceUtc.ToString("o") }, r =>
            {
                var node = r["p"].As<INode>();
                return new PipeNode(
                    node.Properties.ContainsKey("uid") ? node.Properties["uid"].As<string>() : string.Empty,
                    node.Properties.ContainsKey("elementId") ? node.Properties["elementId"].As<long>() : 
                        (node.Properties.ContainsKey("ElementId") ? node.Properties["ElementId"].As<long>() : -1),
                    node.Properties["typeId"].As<long>(),
                    node.Properties.TryGetValue("systemTypeId", out var sysId) ? sysId.As<long>() : -1,
                    node.Properties["levelId"].As<long>(),
                    node.Properties["x1"].As<double>(),
                    node.Properties["y1"].As<double>(),
                    node.Properties["z1"].As<double>(),
                    node.Properties["x2"].As<double>(),
                    node.Properties["y2"].As<double>(),
                    node.Properties["z2"].As<double>(),
                    node.Properties.TryGetValue("diameter", out var dia) ? dia.As<double>() : 0.0,
                       node.Properties.TryGetValue("rvtClass", out var cls) ? cls.As<string>() : "Pipe"
                );
            }).ConfigureAwait(false);
            _logger.LogInformation("Pulled {Count} pipes", list.Count);
            return list;
        }

        // Holt alle seit einem Zeitpunkt geänderten ProvisionalSpaces.
        public async Task<List<ProvisionalSpaceNode>> GetUpdatedProvisionalSpacesAsync(DateTime sinceUtc)
        {
            const string cypher = @"MATCH (ps:ProvisionalSpace)
WHERE  ps.lastModifiedUtc > datetime($since)
RETURN ps";
            _logger.LogDebug("GetUpdatedProvisionalSpaces since {Time}", sinceUtc);
            var list = await RunQueryAsync(cypher, new { since = sinceUtc.ToString("o") }, r =>
            {
                var node = r["ps"].As<INode>();
                return new ProvisionalSpaceNode(
                    node.Properties.TryGetValue("guid", out var guid) ? guid.As<string>() : string.Empty,
                    node.Properties.TryGetValue("name", out var name) ? name.As<string>() : string.Empty,
                    node.Properties.TryGetValue("familyName", out var famName) ? famName.As<string>() : string.Empty,
                    node.Properties.TryGetValue("symbolName", out var symName) ? symName.As<string>() : string.Empty,
                    node.Properties.TryGetValue("width", out var width) ? width.As<double>() : 0.0,
                    node.Properties.TryGetValue("height", out var height) ? height.As<double>() : 0.0,
                    node.Properties.TryGetValue("thickness", out var thick) ? thick.As<double>() : 0.0,
                    node.Properties.TryGetValue("level", out var lvlName) ? lvlName.As<string>() : string.Empty,
                    node.Properties.TryGetValue("levelId", out var lvlId) ? lvlId.As<long>() : -1,
                    node.Properties.TryGetValue("x", out var x) ? x.As<double>() : 0.0,
                    node.Properties.TryGetValue("y", out var y) ? y.As<double>() : 0.0,
                    node.Properties.TryGetValue("z", out var z) ? z.As<double>() : 0.0,
                    node.Properties.TryGetValue("rotation", out var rot) ? rot.As<double>() : 0.0,
                    node.Properties.TryGetValue("hostId", out var hostId) ? hostId.As<long>() : -1,
                    node.Properties.TryGetValue("elementId", out var elemId) ? elemId.As<int>() : -1,  // FIXED: Changed from revitId to elementId for consistency
                    node.Properties.TryGetValue("ifcType", out var ifc) ? ifc.As<string>() : string.Empty,
                    node.Properties.TryGetValue("category", out var cat) ? cat.As<string>() : null,
                    node.Properties.TryGetValue("phaseCreated", out var pc) ? pc.As<int>() : -1,
                    node.Properties.TryGetValue("phaseDemolished", out var pd) ? pd.As<int>() : -1,
                    node.Properties.TryGetValue("bbMinX", out var bbMinX) ? bbMinX.As<double>() : 0.0,
                    node.Properties.TryGetValue("bbMinY", out var bbMinY) ? bbMinY.As<double>() : 0.0,
                    node.Properties.TryGetValue("bbMinZ", out var bbMinZ) ? bbMinZ.As<double>() : 0.0,
                    node.Properties.TryGetValue("bbMaxX", out var bbMaxX) ? bbMaxX.As<double>() : 0.0,
                    node.Properties.TryGetValue("bbMaxY", out var bbMaxY) ? bbMaxY.As<double>() : 0.0,
          node.Properties.TryGetValue("bbMaxZ", out var bbMaxZ) ? bbMaxZ.As<double>() : 0.0,
                    node.Properties.TryGetValue("rvtClass", out var cls) ? cls.As<string>() : "ProvisionalSpace"                );
            }).ConfigureAwait(false);
            _logger.LogInformation("Pulled {Count} provisional spaces", list.Count);
            return list;
        }
        // Schließt den Neo4j-Treiber und gibt Ressourcen frei.
        public void Dispose()
        {
            LogMethodCall(nameof(Dispose), new());
            _driver?.Dispose();
            GC.SuppressFinalize(this);
        }

        // NEW CHANGELOG-BASED SYNCHRONIZATION METHODS
        
        /// <summary>
        /// Creates a ChangeLog entry for an element change (using existing schema compatibility)
        /// </summary>
        public async Task CreateChangeLogEntryAsync(int elementId, string operation, string targetSessionId)
        {
            // Redirect to central method
            await CreateChangeLogEntryWithRelationshipsAsync(elementId, operation, targetSessionId);
        }

        /// <summary>
        /// Gets pending ChangeLog entries for a specific session (using existing schema)
        /// Returns Wall, Door, Pipe, and ProvisionalSpace elements. Architectural elements are auto-acknowledged.
        /// </summary>
        public async Task<List<(int changeId, string op, Dictionary<string, object> element)>> GetPendingChangeLogsAsync(string sessionId)
        {
            try
            {
                Logger.LogToFile($"Starting GetPendingChangeLogsAsync for session {sessionId}", "sync.log");
                
                // Step 1: Auto-acknowledge architectural element ChangeLog entries from other sessions
                await AcknowledgeNonBuildingElementChangeLogsAsync(sessionId).ConfigureAwait(false);

                // Step 2: Get all unacknowledged ChangeLog entries FOR current session (by targetSessionId)
                const string debugQuery = @"
                    MATCH (c:ChangeLog)
                    WHERE c.acknowledged = false AND c.targetSessionId = $sessionId
                    RETURN c.acknowledged as acknowledged, c.sessionId as sessionId, c.targetSessionId as targetSessionId, c.originalSessionId as originalSessionId, c.type as type, c.elementId as elementId
                    LIMIT 20";
                
                var debugResult = await RunQueryAsync(debugQuery, new { sessionId }, record =>
                {
                    var acknowledged = record["acknowledged"]?.As<bool>() ?? true;
                    var changeSessionId = record["sessionId"]?.As<string>() ?? "null";
                    var targetSessionId = record["targetSessionId"]?.As<string>() ?? "null";
                    var originalSessionId = record["originalSessionId"]?.As<string>() ?? "null";
                    var type = record["type"]?.As<string>() ?? "null";
                    var elementId = record["elementId"]?.As<long>() ?? -1;
                    return new { acknowledged, changeSessionId, targetSessionId, originalSessionId, type, elementId };
                }).ConfigureAwait(false);
                
                Logger.LogToFile($"Debug: Found {debugResult.Count} unacknowledged ChangeLog entries FOR current targetSession:", "sync.log");
                foreach (var entry in debugResult)
                {
                    Logger.LogToFile($"  - sessionId:{entry.changeSessionId}, targetSession:{entry.targetSessionId}, originalSession:{entry.originalSessionId}, type:{entry.type}, elementId:{entry.elementId}", "sync.log");
                }

                // CRITICAL DEBUG: Check if Pipe/ProvisionalSpace nodes exist with the ChangeLog elementIds
                foreach (var entry in debugResult.Where(e => e.type == "Modify" || e.type == "Insert"))
                {
                    var elementId = entry.elementId;
                    Logger.LogToFile($"CHANGELOG DEBUG: Checking existence of elementId {elementId} in Neo4j nodes", "sync.log");
                    
                    // Check Pipe existence
                    const string pipeCheckQuery = @"MATCH (p:Pipe) WHERE p.elementId = $elementId RETURN p.elementId as pipeElementId, p.uid as pipeUid LIMIT 1";
                    var pipeResult = await RunQueryAsync(pipeCheckQuery, new { elementId }, record =>
                    {
                        return new { pipeElementId = record["pipeElementId"]?.As<long>() ?? -1, pipeUid = record["pipeUid"]?.As<string>() ?? "" };
                    }).ConfigureAwait(false);
                    
                    if (pipeResult.Count > 0)
                    {
                        Logger.LogToFile($"CHANGELOG DEBUG: FOUND Pipe with elementId {elementId}, uid: {pipeResult[0].pipeUid}", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"CHANGELOG DEBUG: NO Pipe found with elementId {elementId}", "sync.log");
                    }
                    
                    // Check ProvisionalSpace existence  
                    const string psCheckQuery = @"MATCH (ps:ProvisionalSpace) WHERE ps.elementId = $elementId RETURN ps.elementId as psElementId, ps.guid as psGuid LIMIT 1";
                    var psResult = await RunQueryAsync(psCheckQuery, new { elementId }, record =>
                    {
                        return new { psElementId = record["psElementId"]?.As<long>() ?? -1, psGuid = record["psGuid"]?.As<string>() ?? "" };
                    }).ConfigureAwait(false);
                    
                    if (psResult.Count > 0)
                    {
                        Logger.LogToFile($"CHANGELOG DEBUG: FOUND ProvisionalSpace with elementId {elementId}, guid: {psResult[0].psGuid}", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"CHANGELOG DEBUG: NO ProvisionalSpace found with elementId {elementId}", "sync.log");
                    }
                }

                // Additional debug: Check what nodes actually exist in Neo4j
                const string nodeCheckCypher = @"
                    OPTIONAL MATCH (w:Wall) 
                    OPTIONAL MATCH (d:Door)
                    OPTIONAL MATCH (p:Pipe) 
                    OPTIONAL MATCH (ps:ProvisionalSpace)
                    RETURN count(w) as wallCount, count(d) as doorCount, count(p) as pipeCount, count(ps) as psCount,
                           collect(DISTINCT w.elementId)[0..5] as sampleWallIds,
                           collect(DISTINCT d.elementId)[0..5] as sampleDoorIds, 
                           collect(DISTINCT p.elementId)[0..5] as samplePipeIds,
                           collect(DISTINCT ps.elementId)[0..5] as samplePsIds";
                
                var nodeCheckResult = await RunQueryAsync(nodeCheckCypher, new {}, record =>
                {
                    return new
                    {
                        wallCount = record["wallCount"].As<int>(),
                        doorCount = record["doorCount"].As<int>(),
                        pipeCount = record["pipeCount"].As<int>(),
                        psCount = record["psCount"].As<int>(),
                        sampleWallIds = record["sampleWallIds"].As<List<object>>(),
                        sampleDoorIds = record["sampleDoorIds"].As<List<object>>(),
                        samplePipeIds = record["samplePipeIds"].As<List<object>>(),
                        samplePsIds = record["samplePsIds"].As<List<object>>()
                    };
                }).ConfigureAwait(false);
                
                if (nodeCheckResult.Count > 0)
                {
                    var nodeStats = nodeCheckResult[0];
                    Logger.LogToFile($"NEO4J NODE STATS: Walls={nodeStats.wallCount}, Doors={nodeStats.doorCount}, Pipes={nodeStats.pipeCount}, ProvisionalSpaces={nodeStats.psCount}", "sync.log");
                    Logger.LogToFile($"SAMPLE WALL IDs: {string.Join(", ", nodeStats.sampleWallIds)}", "sync.log");
                    Logger.LogToFile($"SAMPLE DOOR IDs: {string.Join(", ", nodeStats.sampleDoorIds)}", "sync.log");
                    Logger.LogToFile($"SAMPLE PIPE IDs: {string.Join(", ", nodeStats.samplePipeIds)}", "sync.log");
                    Logger.LogToFile($"SAMPLE PS IDs: {string.Join(", ", nodeStats.samplePsIds)}", "sync.log");
                }

                // Step 3: Get Wall, Door, Pipe, and ProvisionalSpace ChangeLog entries 
                // ENHANCED SCHEMA: Get ChangeLog entries BY targetSessionId for current session
                const string cypher = @"
                    MATCH (c:ChangeLog)
                    WHERE c.acknowledged = false AND c.targetSessionId = $sessionId
                    OPTIONAL MATCH (w:Wall) WHERE w.elementId = c.elementId
                    OPTIONAL MATCH (d:Door) WHERE d.elementId = c.elementId  
                    OPTIONAL MATCH (p:Pipe) WHERE p.elementId = c.elementId
                    OPTIONAL MATCH (ps:ProvisionalSpace) WHERE ps.elementId = c.elementId
                    WITH c, w, d, p, ps
                    WHERE w IS NOT NULL OR d IS NOT NULL OR p IS NOT NULL OR ps IS NOT NULL OR c.type = 'Delete'
                    RETURN id(c) AS changeId, c.type AS op, c.elementId AS elementId, 
                           c.targetSessionId AS targetSessionId, c.originalSessionId AS originalSessionId,
                           w AS wall, d AS door, p AS pipe, ps AS provisionalSpace
                    ORDER BY c.timestamp ASC";

                var result = await RunQueryAsync(cypher, new { sessionId }, record =>
                {
                    var changeId = record["changeId"].As<int>();
                    var op = record["op"].As<string>();
                    var elementId = record["elementId"].As<long>();
                    var targetSessionId = record["targetSessionId"].As<string>();
                    var originalSessionId = record["originalSessionId"].As<string>();
                    
                    Logger.LogToFile($"PENDING CHANGELOG: ID {changeId}, elementId {elementId}, operation {op}, targetSession {targetSessionId}, originalSession {originalSessionId}", "sync.log");
                    
                    Dictionary<string, object> elementProperties = new Dictionary<string, object>();
                    
                    try
                    {
                        // Check which type of node we have and extract properties
                        var wallNode = record["wall"]?.As<INode>();
                        var doorNode = record["door"]?.As<INode>(); 
                        var pipeNode = record["pipe"]?.As<INode>();
                        var provisionalSpaceNode = record["provisionalSpace"]?.As<INode>();
                        
                        if (wallNode?.Properties != null)
                        {
                            elementProperties = wallNode.Properties.ToDictionary(kv => kv.Key, kv => kv.Value);
                            elementProperties["__element_type__"] = "Wall";
                            elementProperties["elementId"] = elementId; // Ensure elementId is always present
                            Logger.LogToFile($"Successfully loaded wall properties for ElementId {elementId}: {elementProperties.Keys.Count} properties", "sync.log");
                        }
                        else if (doorNode?.Properties != null)
                        {
                            elementProperties = doorNode.Properties.ToDictionary(kv => kv.Key, kv => kv.Value);
                            elementProperties["__element_type__"] = "Door";
                            elementProperties["elementId"] = elementId; // Ensure elementId is always present
                            Logger.LogToFile($"Successfully loaded door properties for ElementId {elementId}: {elementProperties.Keys.Count} properties", "sync.log");
                        }
                        else if (pipeNode?.Properties != null)
                        {
                            elementProperties = pipeNode.Properties.ToDictionary(kv => kv.Key, kv => kv.Value);
                            elementProperties["__element_type__"] = "Pipe";
                            elementProperties["elementId"] = elementId; // Ensure elementId is always present
                            Logger.LogToFile($"Successfully loaded pipe properties for ElementId {elementId}: {elementProperties.Keys.Count} properties", "sync.log");
                        }
                        else if (provisionalSpaceNode?.Properties != null)
                        {
                            elementProperties = provisionalSpaceNode.Properties.ToDictionary(kv => kv.Key, kv => kv.Value);
                            elementProperties["__element_type__"] = "ProvisionalSpace";
                            elementProperties["elementId"] = elementId; // Ensure elementId is always present
                            Logger.LogToFile($"Successfully loaded provisional space properties for ElementId {elementId}: {elementProperties.Keys.Count} properties", "sync.log");
                        }
                        else
                        {
                            // For Delete operations, nodes may no longer exist - handle this case
                            if (op == "Delete")
                            {
                                Logger.LogToFile($"DELETE OPERATION: No element node found for ElementId {elementId} (expected for Delete)", "sync.log");
                                elementProperties["elementId"] = elementId;
                                elementProperties["__element_type__"] = "Wall"; // Default to Wall for Delete operations - will be determined geographically
                                elementProperties["__deleted__"] = true; // Mark as deleted operation
                            }
                            else
                            {
                                Logger.LogToFile($"WARNING: No element node found for ElementId {elementId}", "sync.log");
                                elementProperties["elementId"] = elementId;
                                elementProperties["__element_type__"] = "Unknown";
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"ERROR: Failed to process element node for ElementId {elementId}: {ex.Message}", "sync.log");
                        // Create minimal fallback properties
                        elementProperties["elementId"] = elementId;
                        elementProperties["__element_type__"] = "Unknown";
                    }
                    
                    // Ensure elementId is always present and correct
                    elementProperties["elementId"] = elementId;
                    
                    // ENHANCED SCHEMA: Add session attribution to element properties
                    elementProperties["__targetSessionId__"] = targetSessionId;
                    elementProperties["__originalSessionId__"] = originalSessionId;
                    
                    return (changeId, op, elementProperties);
                }).ConfigureAwait(false);

                Logger.LogToFile($"Retrieved {result.Count} pending ChangeLog entries (Wall/Door/Pipe/ProvisionalSpace) for targetSession {sessionId}", "sync.log");
                return result;
            }
            catch (Exception ex)
            {
                Logger.LogCrash($"Failed to get pending ChangeLogs for session {sessionId}", ex);
                throw;
            }
        }

        /// <summary>
        /// DISABLED: Auto-acknowledge functionality temporarily disabled to fix synchronization issues.
        /// The pull algorithm should handle all ChangeLog entries for building elements.
        /// </summary>
        private Task AcknowledgeNonBuildingElementChangeLogsAsync(string currentSessionId)
        {
            try
            {
                Logger.LogToFile($"AcknowledgeNonBuildingElementChangeLogsAsync called but DISABLED to fix sync issues", "sync.log");
                
                // TEMPORARILY DISABLED - This was acknowledging ChangeLog entries before their corresponding 
                // element nodes were pushed to Neo4j, causing doors/pipes/provisional spaces to never sync
                
                /*
                const string cypher = @"
                    MATCH (c:ChangeLog)
                    WHERE c.acknowledged = false AND c.sessionId = $sessionId
                    OPTIONAL MATCH (w:Wall) WHERE w.elementId = c.elementId
                    OPTIONAL MATCH (d:Door) WHERE d.elementId = c.elementId  
                    OPTIONAL MATCH (p:Pipe) WHERE p.elementId = c.elementId
                    OPTIONAL MATCH (ps:ProvisionalSpace) WHERE ps.elementId = c.elementId
                    WITH c, w, d, p, ps
                    WHERE w IS NULL AND d IS NULL AND p IS NULL AND ps IS NULL
                    SET c.acknowledged = true, 
                        c.ackBy = 'AutoAck_ArchitecturalElements',
                        c.ackTs = datetime()
                    RETURN count(c) as acknowledgedCount";
                */

                // DISABLED: Return early without executing any database operations
                Logger.LogToFile("Auto-acknowledge temporarily disabled - allowing pull algorithm to handle all ChangeLog entries", "sync.log");
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to acknowledge architectural element ChangeLogs", ex);
                // Don't throw - this is not critical for synchronization
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Acknowledges a specific ChangeLog entry (using existing schema)
        /// </summary>
        public async Task AcknowledgeChangeLogAsync(int changeId)
        {
            const string cypher = @"
                MATCH (c:ChangeLog)
                WHERE id(c) = $changeId
                SET c.acknowledged = true, c.ackTs = datetime()
                RETURN c.elementId as elementId";

            try
            {
                await RunWriteQueryAsync(cypher, new { changeId }).ConfigureAwait(false);
                Logger.LogToFile($"Acknowledged ChangeLog entry {changeId}", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash($"Failed to acknowledge ChangeLog {changeId}", ex);
                throw;
            }
        }

        /// <summary>
        /// Acknowledges ALL ChangeLog entries regardless of session (using existing schema)
        /// </summary>
        public async Task AcknowledgeAllChangeLogsAsync()
        {
            try
            {
                Logger.LogToFile("Starting AcknowledgeAllChangeLogsAsync for ALL sessions", "sync.log");
                
                // First check what exists (using correct field names)
                var checkQuery = @"
                    MATCH (c:ChangeLog)
                    RETURN count(c) as totalCount, 
                           sum(CASE WHEN c.acknowledged = false THEN 1 ELSE 0 END) as unacknowledgedCount";
                
                await using var session = _driver.AsyncSession();
                var checkResult = await session.RunAsync(checkQuery, new { }).ConfigureAwait(false);
                var checkRecord = await checkResult.SingleAsync().ConfigureAwait(false);
                var totalCount = checkRecord["totalCount"].As<int>();
                var unacknowledgedCount = checkRecord["unacknowledgedCount"].As<int>();
                
                Logger.LogToFile($"Found {totalCount} total ChangeLog entries, {unacknowledgedCount} unacknowledged", "sync.log");

                // Update using correct field names
                const string cypher = @"
                    MATCH (c:ChangeLog)
                    WHERE c.acknowledged = false
                    SET c.acknowledged = true, 
                        c.ackBy = 'AcknowledgeAll', 
                        c.ackTs = datetime()
                    RETURN count(c) as acknowledgedCount";

                var result = await session.RunAsync(cypher, new { }).ConfigureAwait(false);
                var record = await result.SingleAsync().ConfigureAwait(false);
                var count = record["acknowledgedCount"].As<int>();
                
                Logger.LogToFile($"Successfully acknowledged {count} ChangeLog entries from ALL sessions", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to acknowledge all ChangeLogs", ex);
                throw;
            }
        }

        /// <summary>
        /// Creates test ChangeLog entries for debugging (uses existing ChangeLog format)
        /// </summary>
        public async Task CreateTestChangeLogEntriesAsync(string targetSessionId)
        {
            try
            {
                Logger.LogToFile($"Creating test ChangeLog entries for target session {targetSessionId}", "sync.log");
                
                // Use central method for test ChangeLog creation
                await CreateChangeLogEntryWithRelationshipsAsync(999, "Insert", targetSessionId);
                
                Logger.LogToFile($"Created test ChangeLog entry for ElementId 999, target session {targetSessionId}", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to create test ChangeLog entries", ex);
                throw;
            }
        }

        /// <summary>
        /// Handles level relationships and ChangeLog creation for all element types
        /// </summary>
        private async Task HandleElementLevelRelationshipsAsync(
            IAsyncTransaction tx, 
            string cmd, 
            string changeType, 
            long elementId, 
            string logTime, 
            string sessionId, 
            Document currentDocument)
        {
            Level level = null;
            string elementType = null;

            // Determine element type and relationship
            if (cmd.Contains(":Wall", StringComparison.OrdinalIgnoreCase))
            {
                elementType = "Wall";
                level = await HandleWallLevelRelationshipAsync(tx, changeType, elementId, currentDocument).ConfigureAwait(false);
            }
            else if (cmd.Contains(":Door", StringComparison.OrdinalIgnoreCase))
            {
                elementType = "Door";
                level = await HandleDoorLevelRelationshipAsync(tx, changeType, elementId, currentDocument).ConfigureAwait(false);
            }
            else if (cmd.Contains(":Pipe", StringComparison.OrdinalIgnoreCase))
            {
                elementType = "Pipe";
                level = await HandlePipeLevelRelationshipAsync(tx, changeType, elementId, currentDocument).ConfigureAwait(false);
            }
            else if (cmd.Contains(":ProvisionalSpace", StringComparison.OrdinalIgnoreCase))
            {
                elementType = "ProvisionalSpace";
                level = await HandleProvisionalSpaceLevelRelationshipAsync(tx, changeType, elementId, currentDocument).ConfigureAwait(false);
            }

            // Create ChangeLog entry for affected Level (for all element types)
            if (level != null && !string.IsNullOrEmpty(elementType))
            {
                await CreateLevelChangeLogAsync(tx, level, elementType, changeType, elementId, logTime, sessionId).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Handles Wall-Level relationships
        /// </summary>
        private async Task<Level> HandleWallLevelRelationshipAsync(
            IAsyncTransaction tx, 
            string changeType, 
            long elementId, 
            Document currentDocument)
        {
            Level level = null;

            if (changeType == "Insert" || changeType == "Modify")
            {
                // For Insert and Modify: get level from current wall
                var wall = currentDocument.GetElement(new ElementId((int)elementId)) as Wall;
                if (wall != null)
                {
                    level = currentDocument.GetElement(wall.LevelId) as Level;
                    
                    if (level != null && changeType == "Insert")
                    {
                        // Only for Insert: create level relationship
                        const string relCypher = @"
                            MATCH (l:Level {elementId: $levelId}), (w:Wall {elementId: $wallId})
                            MERGE (l)-[:CONTAINS]->(w)";
                        await tx.RunAsync(relCypher,
                            new { levelId = level.Id.Value, wallId = elementId }).ConfigureAwait(false);
                    }
                }
            }
            else if (changeType == "Delete")
            {
                // For Delete: find level via existing Neo4j relationship
                const string findLevelQuery = @"
                    MATCH (l:Level)-[:CONTAINS]->(w:Wall {elementId: $wallId})
                    RETURN l.elementId as levelId";
                    
                var levelResult = await tx.RunAsync(findLevelQuery, new { wallId = elementId }).ConfigureAwait(false);
                var levelRecords = await levelResult.ToListAsync().ConfigureAwait(false);
                var levelRecord = levelRecords.FirstOrDefault();
                
                if (levelRecord != null)
                {
                    var levelId = levelRecord["levelId"].As<long>();
                    level = currentDocument.GetElement(new ElementId((int)levelId)) as Level;
                    
                    // Delete level relationship
                    const string deleteRelCypher = @"
                        MATCH (l:Level {elementId: $levelId})-[r:CONTAINS]->(w:Wall {elementId: $wallId})
                        DELETE r";
                    await tx.RunAsync(deleteRelCypher,
                        new { levelId = levelId, wallId = elementId }).ConfigureAwait(false);
                }
            }

            return level;
        }

        /// <summary>
        /// Handles Door-Wall-Level relationships
        /// </summary>
        private async Task<Level> HandleDoorLevelRelationshipAsync(
            IAsyncTransaction tx, 
            string changeType, 
            long elementId, 
            Document currentDocument)
        {
            Level level = null;

            if (changeType == "Insert" || changeType == "Modify")
            {
                // For Insert and Modify: get level from door's host wall
                var door = currentDocument.GetElement(new ElementId((int)elementId)) as FamilyInstance;
                if (door != null && door.Host is Wall hostWall)
                {
                    level = currentDocument.GetElement(hostWall.LevelId) as Level;
                    
                    if (level != null && changeType == "Insert")
                    {
                        // Create Door-Wall and Level-Door relationships
                        const string relCypher = @"
                            MATCH (l:Level {elementId: $levelId}), (w:Wall {elementId: $wallId}), (d:Door {elementId: $doorId})
                            MERGE (w)-[:HOSTS]->(d)
                            MERGE (l)-[:CONTAINS]->(d)";
                        await tx.RunAsync(relCypher,
                            new { levelId = level.Id.Value, wallId = hostWall.Id.Value, doorId = elementId }).ConfigureAwait(false);
                    }
                }
            }
            else if (changeType == "Delete")
            {
                // For Delete: find level via existing Neo4j relationships
                const string findLevelQuery = @"
                    MATCH (l:Level)-[:CONTAINS]->(d:Door {elementId: $doorId})
                    RETURN l.elementId as levelId";
                    
                var levelResult = await tx.RunAsync(findLevelQuery, new { doorId = elementId }).ConfigureAwait(false);
                var levelRecords = await levelResult.ToListAsync().ConfigureAwait(false);
                var levelRecord = levelRecords.FirstOrDefault();
                
                if (levelRecord != null)
                {
                    var levelId = levelRecord["levelId"].As<long>();
                    level = currentDocument.GetElement(new ElementId((int)levelId)) as Level;
                    
                    // Delete door relationships
                    const string deleteRelCypher = @"
                        MATCH (l:Level)-[r1:CONTAINS]->(d:Door {elementId: $doorId})
                        MATCH (w:Wall)-[r2:HOSTS]->(d)
                        DELETE r1, r2";
                    await tx.RunAsync(deleteRelCypher, new { doorId = elementId }).ConfigureAwait(false);
                }
            }

            return level;
        }

        /// <summary>
        /// Handles Pipe-Level relationships
        /// </summary>
        private async Task<Level> HandlePipeLevelRelationshipAsync(
            IAsyncTransaction tx, 
            string changeType, 
            long elementId, 
            Document currentDocument)
        {
            Level level = null;

            if (changeType == "Insert" || changeType == "Modify")
            {
                // For Insert and Modify: get level from pipe
                var pipe = currentDocument.GetElement(new ElementId((int)elementId)) as Pipe;
                if (pipe != null)
                {
                    level = currentDocument.GetElement(pipe.LevelId) as Level;
                    
                    if (level != null && changeType == "Insert")
                    {
                        // Create Level-Pipe relationship
                        const string relCypher = @"
                            MATCH (l:Level {elementId: $levelId}), (p:Pipe {elementId: $pipeId})
                            MERGE (l)-[:CONTAINS]->(p)";
                        await tx.RunAsync(relCypher,
                            new { levelId = level.Id.Value, pipeId = elementId }).ConfigureAwait(false);
                    }
                }
            }
            else if (changeType == "Delete")
            {
                // For Delete: find level via existing Neo4j relationship
                const string findLevelQuery = @"
                    MATCH (l:Level)-[:CONTAINS]->(p:Pipe {elementId: $pipeId})
                    RETURN l.elementId as levelId";
                    
                var levelResult = await tx.RunAsync(findLevelQuery, new { pipeId = elementId }).ConfigureAwait(false);
                var levelRecords = await levelResult.ToListAsync().ConfigureAwait(false);
                var levelRecord = levelRecords.FirstOrDefault();
                
                if (levelRecord != null)
                {
                    var levelId = levelRecord["levelId"].As<long>();
                    level = currentDocument.GetElement(new ElementId((int)levelId)) as Level;
                    
                    // Delete level relationship
                    const string deleteRelCypher = @"
                        MATCH (l:Level {elementId: $levelId})-[r:CONTAINS]->(p:Pipe {elementId: $pipeId})
                        DELETE r";
                    await tx.RunAsync(deleteRelCypher,
                        new { levelId = levelId, pipeId = elementId }).ConfigureAwait(false);
                }
            }

            return level;
        }

        /// <summary>
        /// Handles ProvisionalSpace-Wall relationships
        /// </summary>
        private async Task<Level> HandleProvisionalSpaceLevelRelationshipAsync(
            IAsyncTransaction tx, 
            string changeType, 
            long elementId, 
            Document currentDocument)
        {
            Level level = null;

            if (changeType == "Insert" || changeType == "Modify")
            {
                // For Insert and Modify: find associated wall and get its level
                var space = currentDocument.GetElement(new ElementId((int)elementId)) as FamilyInstance;
                if (space != null)
                {
                    // Try to find a nearby wall or use a default level
                    // For now, use the first available level as ProvisionalSpaces are often not bound to specific levels
                    level = new FilteredElementCollector(currentDocument)
                        .OfClass(typeof(Level))
                        .Cast<Level>()
                        .FirstOrDefault();
                    
                    if (level != null && changeType == "Insert")
                    {
                        // Create Level-ProvisionalSpace relationship
                        const string relCypher = @"
                            MATCH (l:Level {elementId: $levelId}), (ps:ProvisionalSpace {elementId: $spaceId})
                            MERGE (l)-[:CONTAINS]->(ps)";
                        await tx.RunAsync(relCypher,
                            new { levelId = level.Id.Value, spaceId = elementId }).ConfigureAwait(false);
                            
                        // Also try to find nearby walls for ProvisionalSpace-Wall relationships
                        var nearbyWalls = new FilteredElementCollector(currentDocument)
                            .OfClass(typeof(Wall))
                            .Cast<Wall>()
                            .Take(5); // Limit to avoid performance issues
                            
                        foreach (var wall in nearbyWalls)
                        {
                            const string wallRelCypher = @"
                                MATCH (w:Wall {elementId: $wallId}), (ps:ProvisionalSpace {elementId: $spaceId})
                                MERGE (ps)-[:ADJACENT_TO]->(w)";
                            await tx.RunAsync(wallRelCypher,
                                new { wallId = wall.Id.Value, spaceId = elementId }).ConfigureAwait(false);
                        }
                    }
                }
            }
            else if (changeType == "Delete")
            {
                // For Delete: find level via existing Neo4j relationship
                const string findLevelQuery = @"
                    MATCH (l:Level)-[:CONTAINS]->(ps:ProvisionalSpace {elementId: $spaceId})
                    RETURN l.elementId as levelId";
                    
                var levelResult = await tx.RunAsync(findLevelQuery, new { spaceId = elementId }).ConfigureAwait(false);
                var levelRecords = await levelResult.ToListAsync().ConfigureAwait(false);
                var levelRecord = levelRecords.FirstOrDefault();
                
                if (levelRecord != null)
                {
                    var levelId = levelRecord["levelId"].As<long>();
                    level = currentDocument.GetElement(new ElementId((int)levelId)) as Level;
                    
                    // Delete all ProvisionalSpace relationships
                    const string deleteRelCypher = @"
                        MATCH (l:Level)-[r1:CONTAINS]->(ps:ProvisionalSpace {elementId: $spaceId})
                        MATCH (ps)-[r2:ADJACENT_TO]->(w:Wall)
                        DELETE r1, r2";
                    await tx.RunAsync(deleteRelCypher, new { spaceId = elementId }).ConfigureAwait(false);
                }
            }

            return level;
        }

        /// <summary>
        /// Creates ChangeLog entry for affected Level
        /// </summary>
        private async Task CreateLevelChangeLogAsync(
            IAsyncTransaction tx, 
            Level level, 
            string elementType, 
            string changeType, 
            long elementId, 
            string logTime, 
            string sessionId)
        {
            Logger.LogToFile($"PUSH LEVEL CHANGE: Level {level.Id.Value} modified by {elementType} {changeType} operation on element {elementId}, merging ChangeLog for other sessions", "sync.log");
            
            // Update Level lastModifiedUtc
            await tx.RunAsync(
                "MATCH (l:Level { elementId: $levelId }) SET l.lastModifiedUtc = datetime($time)",
                new { levelId = level.Id.Value, time = logTime }).ConfigureAwait(false);

            // Create/Update ONE ChangeLog entry per session for Level (merge to prevent duplicates)
            const string levelLogQuery = @"
                MATCH (s:Session) 
                WHERE s.id <> $currentSession
                WITH s
                MERGE (cl:ChangeLog {
                    sessionId: s.id,
                    elementId: $levelId,
                    type: 'Modify'
                })
                ON CREATE SET 
                    cl.user = $user,
                    cl.timestamp = datetime($time),
                    cl.acknowledged = false
                ON MATCH SET 
                    cl.timestamp = datetime($time),
                    cl.user = $user
                MERGE (s)-[:HAS_LOG]->(cl)
                WITH cl
                OPTIONAL MATCH (level:Level) WHERE level.elementId = $levelId
                WITH cl, level
                WHERE level IS NOT NULL
                MERGE (cl)-[:GOT_CHANGED]->(level)
                RETURN count(DISTINCT cl) as changeLogCount";

            var result = await tx.RunAsync(levelLogQuery,
                new
                {
                    currentSession = sessionId,
                    user = sessionId,
                    time = logTime,
                    levelId = level.Id.Value
                }).ConfigureAwait(false);

            var records = await result.ToListAsync().ConfigureAwait(false);
            int changeLogCount = records.FirstOrDefault()?["changeLogCount"]?.As<int>() ?? 0;
            
            Logger.LogToFile($"PUSH LEVEL CHANGELOG: Merged {changeLogCount} ChangeLog entries for Level {level.Id.Value} across other sessions (prevented duplicates)", "sync.log");
        }

        /// <summary>
        /// Cleanup utility to remove invalid ChangeLog entries and reset acknowledged flags
        /// </summary>
        public async Task CleanupInvalidChangeLogEntriesAsync()
        {
            try
            {
                Logger.LogToFile("Starting cleanup of invalid ChangeLog entries...", "sync.log");
                
                await using var session = _driver.AsyncSession();
                
                // Step 1: Delete ChangeLog entries with invalid ElementIds (-1, 999)
                const string cleanupInvalidCypher = @"
                    MATCH (c:ChangeLog) 
                    WHERE c.elementId IN [-1, 999] 
                    WITH c, id(c) as changeId
                    DELETE c 
                    RETURN count(*) as deletedCount";
                
                var result = await session.RunAsync(cleanupInvalidCypher).ConfigureAwait(false);
                var record = await result.SingleAsync().ConfigureAwait(false);
                var deletedInvalidCount = record["deletedCount"].As<int>();
                
                Logger.LogToFile($"CLEANUP: Deleted {deletedInvalidCount} invalid ChangeLog entries with ElementIds -1 or 999", "sync.log");
                
                // Step 2: Remove duplicate ChangeLog entries (keep only the latest one per elementId)
                const string cleanupDuplicatesCypher = @"
                    MATCH (c:ChangeLog)
                    WITH c.elementId as elementId, collect(c) as changeLogs
                    WHERE size(changeLogs) > 1
                    UNWIND changeLogs[..-1] as duplicateChange
                    DELETE duplicateChange
                    RETURN count(*) as deletedDuplicates";
                
                var duplicateResult = await session.RunAsync(cleanupDuplicatesCypher).ConfigureAwait(false);
                var duplicateRecord = await duplicateResult.SingleAsync().ConfigureAwait(false);
                var deletedDuplicatesCount = duplicateRecord["deletedDuplicates"].As<int>();
                
                Logger.LogToFile($"CLEANUP: Deleted {deletedDuplicatesCount} duplicate ChangeLog entries", "sync.log");
                
                // Step 3: Reset all acknowledged flags to ensure fresh start
                const string resetCypher = @"
                    MATCH (c:ChangeLog {acknowledged: true}) 
                    SET c.acknowledged = false 
                    REMOVE c.ackBy, c.ackTs
                    RETURN count(c) as resetCount";
                
                var resetResult = await session.RunAsync(resetCypher).ConfigureAwait(false);
                var resetRecord = await resetResult.SingleAsync().ConfigureAwait(false);
                var resetCount = resetRecord["resetCount"].As<int>();
                
                Logger.LogToFile($"CLEANUP: Reset {resetCount} acknowledged ChangeLog entries to unacknowledged", "sync.log");
                
                // Step 4: Get final statistics
                const string statsCypher = @"
                    MATCH (c:ChangeLog)
                    RETURN count(c) as totalChangeLogs,
                           count(DISTINCT c.elementId) as uniqueElementIds,
                           collect(DISTINCT c.elementId)[0..10] as sampleElementIds";
                           
                var statsResult = await session.RunAsync(statsCypher).ConfigureAwait(false);
                var statsRecord = await statsResult.SingleAsync().ConfigureAwait(false);
                var totalChangeLogs = statsRecord["totalChangeLogs"].As<int>();
                var uniqueElementIds = statsRecord["uniqueElementIds"].As<int>();
                var sampleIds = statsRecord["sampleElementIds"].As<List<object>>();
                
                Logger.LogToFile($"CLEANUP STATS: {totalChangeLogs} total ChangeLog entries, {uniqueElementIds} unique ElementIds", "sync.log");
                Logger.LogToFile($"CLEANUP SAMPLE IDs: {string.Join(", ", sampleIds)}", "sync.log");
                Logger.LogToFile($"CLEANUP COMPLETE: Deleted {deletedInvalidCount} invalid + {deletedDuplicatesCount} duplicates, reset {resetCount} acknowledged entries", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to cleanup invalid ChangeLog entries", ex);
                throw;
            }
        }
        /// <summary>
        /// Diagnostics: Analyze GOT_CHANGED relationships status
        /// </summary>
        public async Task AnalyzeGotChangedRelationshipsAsync()
        {
            try
            {
                Logger.LogToFile("=== GOT_CHANGED Relationships Analysis ===", "sync.log");
                
                await using var session = _driver.AsyncSession();
                
                // Count total ChangeLog entries
                var totalResult = await session.RunAsync("MATCH (cl:ChangeLog) RETURN count(cl) as total").ConfigureAwait(false);
                var totalRecord = await totalResult.SingleAsync().ConfigureAwait(false);
                var totalChangeLogs = totalRecord["total"].As<int>();
                
                // Count ChangeLog entries with GOT_CHANGED relationships
                var withRelResult = await session.RunAsync("MATCH (cl:ChangeLog)-[:GOT_CHANGED]->() RETURN count(DISTINCT cl) as with_relationships").ConfigureAwait(false);
                var withRelRecord = await withRelResult.SingleAsync().ConfigureAwait(false);
                var withRelationships = withRelRecord["with_relationships"].As<int>();
                
                // Count ChangeLog entries without GOT_CHANGED relationships
                var withoutRelResult = await session.RunAsync("MATCH (cl:ChangeLog) WHERE NOT EXISTS { (cl)-[:GOT_CHANGED]->() } RETURN count(cl) as without_relationships").ConfigureAwait(false);
                var withoutRelRecord = await withoutRelResult.SingleAsync().ConfigureAwait(false);
                var withoutRelationships = withoutRelRecord["without_relationships"].As<int>();
                
                // Analyze element types and ID formats
                var elementsResult = await session.RunAsync(@"
MATCH (cl:ChangeLog)
WHERE NOT EXISTS { (cl)-[:GOT_CHANGED]->() }
WITH cl.elementId as eid
OPTIONAL MATCH (wall:Wall) WHERE wall.elementId = eid OR wall.elementId = toString(eid)
OPTIONAL MATCH (door:Door) WHERE door.elementId = eid OR door.elementId = toString(eid)  
OPTIONAL MATCH (pipe:Pipe) WHERE pipe.elementId = eid OR pipe.elementId = toString(eid)
OPTIONAL MATCH (space:ProvisionalSpace) WHERE space.elementId = eid OR space.elementId = toString(eid)
RETURN eid, 
       CASE WHEN wall IS NOT NULL THEN 'Wall' ELSE null END as wall_match,
       CASE WHEN door IS NOT NULL THEN 'Door' ELSE null END as door_match,
       CASE WHEN pipe IS NOT NULL THEN 'Pipe' ELSE null END as pipe_match,
       CASE WHEN space IS NOT NULL THEN 'ProvisionalSpace' ELSE null END as space_match
LIMIT 10").ConfigureAwait(false);
                
                Logger.LogToFile($"Total ChangeLog entries: {totalChangeLogs}", "sync.log");
                Logger.LogToFile($"ChangeLog entries WITH GOT_CHANGED relationships: {withRelationships}", "sync.log");
                Logger.LogToFile($"ChangeLog entries WITHOUT GOT_CHANGED relationships: {withoutRelationships}", "sync.log");
                
                Logger.LogToFile("Sample ChangeLog entries without relationships:", "sync.log");
                await foreach (var record in elementsResult)
                {
                    var elementId = record["eid"]?.ToString() ?? "null";
                    var wallMatch = record["wall_match"]?.ToString();
                    var doorMatch = record["door_match"]?.ToString();
                    var pipeMatch = record["pipe_match"]?.ToString();
                    var spaceMatch = record["space_match"]?.ToString();
                    
                    var matches = new[] { wallMatch, doorMatch, pipeMatch, spaceMatch }.Where(m => m != null).ToArray();
                    var matchStr = matches.Any() ? string.Join(", ", matches) : "NO MATCHES";
                    
                    Logger.LogToFile($"  elementId: {elementId} -> {matchStr}", "sync.log");
                }
                
                Logger.LogToFile("=== End Analysis ===", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to analyze GOT_CHANGED relationships", ex);
                throw;
            }
        }

        /// <summary>
        /// Repairs missing GOT_CHANGED relationships for existing ChangeLog entries
        /// </summary>
        public async Task RepairMissingGotChangedRelationshipsAsync()
        {
            const string repairQuery = @"
MATCH (cl:ChangeLog)
WHERE NOT EXISTS { (cl)-[:GOT_CHANGED]->() }
WITH cl
OPTIONAL MATCH (wall:Wall) WHERE wall.elementId = cl.elementId OR wall.elementId = toString(cl.elementId)
OPTIONAL MATCH (door:Door) WHERE door.elementId = cl.elementId OR door.elementId = toString(cl.elementId)
OPTIONAL MATCH (pipe:Pipe) WHERE pipe.elementId = cl.elementId OR pipe.elementId = toString(cl.elementId)
OPTIONAL MATCH (space:ProvisionalSpace) WHERE space.elementId = cl.elementId OR space.elementId = toString(cl.elementId)
OPTIONAL MATCH (level:Level) WHERE level.elementId = cl.elementId OR level.elementId = toString(cl.elementId)
OPTIONAL MATCH (building:Building) WHERE building.elementId = cl.elementId OR building.elementId = toString(cl.elementId)
WITH cl, wall, door, pipe, space, level, building
WHERE wall IS NOT NULL OR door IS NOT NULL OR pipe IS NOT NULL OR space IS NOT NULL OR level IS NOT NULL OR building IS NOT NULL
FOREACH (elem IN CASE WHEN wall IS NOT NULL THEN [wall] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN door IS NOT NULL THEN [door] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN pipe IS NOT NULL THEN [pipe] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN space IS NOT NULL THEN [space] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN level IS NOT NULL THEN [level] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
FOREACH (elem IN CASE WHEN building IS NOT NULL THEN [building] ELSE [] END | MERGE (cl)-[:GOT_CHANGED]->(elem))
RETURN count(cl) as repaired_count";

            try
            {
                Logger.LogToFile("Repairing missing GOT_CHANGED relationships for existing ChangeLog entries...", "sync.log");
                
                await using var session = _driver.AsyncSession();
                var result = await session.RunAsync(repairQuery).ConfigureAwait(false);
                var record = await result.SingleAsync().ConfigureAwait(false);
                var repairedCount = record["repaired_count"].As<int>();
                
                Logger.LogToFile($"Successfully repaired {repairedCount} ChangeLog entries with missing GOT_CHANGED relationships", "sync.log");

                // Verify results
                const string verifyQuery = @"
MATCH (cl:ChangeLog)
WHERE NOT EXISTS { (cl)-[:GOT_CHANGED]->() }
RETURN count(cl) as orphaned_count";
                
                var verifyResult = await session.RunAsync(verifyQuery).ConfigureAwait(false);
                var verifyRecord = await verifyResult.SingleAsync().ConfigureAwait(false);
                var orphanedCount = verifyRecord["orphaned_count"].As<int>();
                
                Logger.LogToFile($"After repair: {orphanedCount} ChangeLog entries still without GOT_CHANGED relationships", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to repair missing GOT_CHANGED relationships", ex);
                throw;
            }
        }

        /// <summary>
        /// Gets all active session IDs from Neo4j database.
        /// This is essential for multi-instance synchronization to find sessions across different Revit instances.
        /// </summary>
        public async Task<List<string>> GetAllActiveSessionsAsync()
        {
            try
            {
                Logger.LogToFile("SESSIONS QUERY: Getting all active sessions from Neo4j", "sync.log");
                
                const string query = @"MATCH (s:Session) 
                                     WHERE s.status = 'active' OR s.status IS NULL
                                     RETURN s.id AS sessionId 
                                     ORDER BY s.id";
                
                var records = await RunReadQueryAsync(query).ConfigureAwait(false);
                var sessions = records.Select(r => r["sessionId"].As<string>()).ToList();
                
                Logger.LogToFile($"SESSIONS QUERY: Found {sessions.Count} active sessions: [{string.Join(", ", sessions)}]", "sync.log");
                return sessions;
            }
            catch (Exception ex)
            {
                Logger.LogCrash("GetAllActiveSessionsAsync failed", ex);
                return new List<string>(); // Return empty list on error
            }
        }

        /// <summary>
        /// Creates a Session node in Neo4j immediately when Revit starts, without requiring any changes.
        /// This ensures sessions are available for ChangeLog targeting from the beginning.
        /// </summary>
        public async Task CreateSessionNodeAsync(string sessionId)
        {
            try
            {
                Logger.LogToFile($"SESSION CREATION: Creating Session node for {sessionId} in Neo4j at startup", "sync.log");
                
                var initTime = DateTime.UtcNow.ToString("o");
                
                const string query = @"
                    MERGE (s:Session { id: $sessionId })
                    SET s.created = CASE WHEN s.created IS NULL THEN datetime($time) ELSE s.created END,
                        s.lastSync = datetime($time),
                        s.lastUpdate = datetime($time),
                        s.status = 'active'
                    RETURN s.id AS sessionId";
                
                var session = _driver.AsyncSession();
                try
                {
                    var result = await session.RunAsync(query, new { 
                        sessionId = sessionId, 
                        time = initTime 
                    }).ConfigureAwait(false);
                    
                    var record = await result.SingleAsync().ConfigureAwait(false);
                    var createdSessionId = record["sessionId"].As<string>();
                    
                    Logger.LogToFile($"SESSION CREATION SUCCESS: Session node created for {createdSessionId}", "sync.log");
                }
                finally
                {
                    await session.CloseAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SESSION CREATION ERROR: Failed to create Session node for {sessionId}: {ex.Message}", "sync.log");
                Logger.LogCrash($"CreateSessionNodeAsync failed for {sessionId}", ex);
            }
        }

        /// <summary>
        /// Marks a Session node as inactive when Revit closes, to help with cleanup.
        /// </summary>
        public async Task DeactivateSessionNodeAsync(string sessionId)
        {
            try
            {
                Logger.LogToFile($"SESSION DEACTIVATION: Marking session {sessionId} as inactive", "sync.log");
                
                const string query = @"
                    MATCH (s:Session { id: $sessionId })
                    SET s.status = 'inactive',
                        s.lastUpdate = datetime($time)
                    RETURN s.id AS sessionId";
                
                var session = _driver.AsyncSession();
                try
                {
                    var result = await session.RunAsync(query, new { 
                        sessionId = sessionId, 
                        time = DateTime.UtcNow.ToString("o") 
                    }).ConfigureAwait(false);
                    
                    Logger.LogToFile($"SESSION DEACTIVATION SUCCESS: Session {sessionId} marked as inactive", "sync.log");
                }
                finally
                {
                    await session.CloseAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SESSION DEACTIVATION ERROR: Failed to deactivate session {sessionId}: {ex.Message}", "sync.log");
                Logger.LogCrash($"DeactivateSessionNodeAsync failed for {sessionId}", ex);
            }
        }

    }
}




