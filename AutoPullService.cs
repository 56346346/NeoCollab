using System;
using System.Threading;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using System.Linq;
using Neo4j.Driver;
using Autodesk.Revit.UI;

namespace NeoCollab
{
    /// <summary>
    /// Event-based service that automatically triggers pull operations when ChangeLog entries 
    /// are created for the current session from other sessions.
    /// Replaces polling with immediate event-based notifications.
    /// </summary>
    public class AutoPullService : IDisposable
    {
        private readonly Neo4jConnector _neo4jConnector;
        private readonly GraphPuller _graphPuller;
        private readonly Neo4jChangeNotifier _changeNotifier;
        private readonly object _lock = new object();
        private DateTime _lastPullTime = DateTime.MinValue;
        private bool _disposed = false;

        // Store UIApplication reference to get current document
        private static UIApplication _uiApplication;
        
        // Delay before executing pull after detecting changes (allows batching)
        // CRITICAL FIX: Increased from 0.5s to 1.5s to allow push transactions to complete before pull reads from Neo4j
        // This prevents race condition where pull reads old data before push transaction commits
        private static readonly TimeSpan PullDelay = TimeSpan.FromSeconds(1.5);

        public AutoPullService(Neo4jConnector neo4jConnector, GraphPuller graphPuller)
        {
            _neo4jConnector = neo4jConnector;
            _graphPuller = graphPuller;
            
            string currentSessionId = CommandManager.Instance.SessionId;
            Logger.LogToFile($"AUTO-PULL SERVICE INIT: Pull delay = {PullDelay.TotalSeconds} seconds", "sync.log");
            
            // Initialize event-based change notifier instead of polling timer
            _changeNotifier = new Neo4jChangeNotifier(_neo4jConnector.Driver);
            _changeNotifier.ChangeLogCreated += OnChangeLogCreated;
            
            Logger.LogToFile($"AUTO-PULL SERVICE INIT: Event-based change notification system started", "sync.log");
        }

        // Event handler for ChangeLog creation events
        private void OnChangeLogCreated(string targetSessionId)
        {
            try
            {
                string currentSessionId = CommandManager.Instance.SessionId;
                
                // Only respond to changes targeting this session
                if (targetSessionId == currentSessionId)
                {
                    Logger.LogToFile($"AUTO-PULL EVENT: ChangeLog event received for session {currentSessionId}, scheduling pull in {PullDelay.TotalSeconds} seconds", "sync.log");
                    
                    // Schedule delayed pull to allow for batching of multiple changes
                    _ = Task.Delay(PullDelay).ContinueWith(_ => 
                    {
                        if (!_disposed)
                        {
                            Logger.LogToFile($"AUTO-PULL DELAY COMPLETE: {PullDelay.TotalSeconds} second delay completed, executing pull", "sync.log");
                            ExecuteAutoPull();
                        }
                        else
                        {
                            Logger.LogToFile("AUTO-PULL CANCELLED: Service was disposed during delay", "sync.log");
                        }
                    });
                }
                else
                {
                    Logger.LogToFile($"AUTO-PULL EVENT IGNORED: ChangeLog event for session {targetSessionId} does not match current session {currentSessionId}", "sync.log");
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"AUTO-PULL EVENT ERROR: Exception handling ChangeLog event: {ex.Message}", "sync.log");
                Logger.LogCrash("AutoPullService ChangeLog event error", ex);
            }
        }

        // Static method to set UIApplication reference
        public static void SetUIApplication(UIApplication uiApp)
        {
            _uiApplication = uiApp;
            Logger.LogToFile("AUTO-PULL SERVICE: UIApplication reference set for document access", "sync.log");
        }

        private void ExecuteAutoPull()
        {
            try
            {
                string currentSessionId = CommandManager.Instance.SessionId;
                
                Logger.LogToFile($"AUTO-PULL EXECUTE START: Beginning automatic pull execution for session {currentSessionId}", "sync.log");
                
                // Debouncing: Don't execute pull if one was executed recently
                var timeSinceLastPull = DateTime.UtcNow - _lastPullTime;
                if (timeSinceLastPull < TimeSpan.FromSeconds(1))
                {
                    Logger.LogToFile($"AUTO-PULL DEBOUNCED: Skipping pull, last pull was {timeSinceLastPull.TotalSeconds:F1} seconds ago (< 1s threshold)", "sync.log");
                    return;
                }
                
                _lastPullTime = DateTime.UtcNow;
                Logger.LogToFile($"AUTO-PULL EXECUTE: Starting automatic pull for session {currentSessionId} at {_lastPullTime:HH:mm:ss.fff}", "sync.log");

                // Use the GraphPullHandler ExternalEvent to ensure thread safety
                var pullHandler = NeoCollabClass.GraphPullHandlerInstance;
                if (pullHandler != null)
                {
                    // Get current active document from UIApplication
                    Document currentDoc = null;
                    try
                    {
                        if (_uiApplication?.ActiveUIDocument?.Document != null)
                        {
                            currentDoc = _uiApplication.ActiveUIDocument.Document;
                            Logger.LogToFile($"AUTO-PULL DOCUMENT: Found active document '{currentDoc.Title}'", "sync.log");
                        }
                        else
                        {
                            Logger.LogToFile("AUTO-PULL DOCUMENT: No active UIDocument found, attempting SessionManager fallback", "sync.log");
                            
                            // Fallback: Try SessionManager
                            var firstSession = SessionManager.OpenSessions.Values.FirstOrDefault();
                            if (firstSession != null)
                            {
                                currentDoc = firstSession.Document;
                                Logger.LogToFile($"AUTO-PULL FALLBACK: Using SessionManager document '{currentDoc.Title}'", "sync.log");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"AUTO-PULL DOCUMENT ERROR: Exception getting current document: {ex.Message}", "sync.log");
                    }

                    if (currentDoc != null)
                    {
                        Logger.LogToFile($"AUTO-PULL REQUEST: Using ExternalEvent to request pull for document '{currentDoc.Title}'", "sync.log");
                        
                        try 
                        {
                            pullHandler.RequestPull(currentDoc);
                            Logger.LogToFile($"AUTO-PULL REQUEST SENT: ExternalEvent.Raise() called for automatic pull", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"AUTO-PULL REQUEST ERROR: Exception calling RequestPull: {ex.Message}", "sync.log");
                            Logger.LogCrash("AutoPullService RequestPull error", ex);
                        }
                    }
                    else
                    {
                        Logger.LogToFile("AUTO-PULL SKIPPED: No active document found and no SessionManager sessions available", "sync.log");
                    }
                }
                else
                {
                    Logger.LogToFile("AUTO-PULL ERROR: GraphPullHandler instance not available from NeoCollabClass", "sync.log");
                }
                
                Logger.LogToFile($"AUTO-PULL EXECUTE END: Finished automatic pull request for session {currentSessionId}", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"AUTO-PULL EXECUTE ERROR: Exception during automatic pull execution: {ex.Message}", "sync.log");
                Logger.LogCrash("AutoPullService execute error", ex);
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed) return;
                _disposed = true;
            }

            Logger.LogToFile("AUTO-PULL SERVICE DISPOSE: Stopping event-based AutoPullService", "sync.log");
            
            // Dispose change notifier instead of timer
            _changeNotifier?.Dispose();
            
            Logger.LogToFile("AUTO-PULL SERVICE DISPOSE: AutoPullService stopped and disposed", "sync.log");
        }
    }
}
