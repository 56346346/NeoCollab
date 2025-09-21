using Autodesk.Revit.UI;
using System;
using Autodesk.Revit.DB;
using System.Collections.Concurrent;


namespace NeoCollab
{
    public class GraphPullHandler : IExternalEventHandler
    {
        private readonly ConcurrentQueue<Document> _queue = new();
        internal ExternalEvent ExternalEvent { get; set; }
        private readonly GraphPuller _puller = new();

        public void RequestPull(Document doc)
        {
            var sessionId = CommandManager.Instance.SessionId;
            Logger.LogToFile($"PULL REQUESTED: Document '{doc?.Title}' by session {sessionId} at {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");
            
            _queue.Enqueue(doc);
            if (ExternalEvent != null && !ExternalEvent.IsPending)
            {
                Logger.LogToFile("PULL EVENT RAISED: ExternalEvent.Raise() called", "sync.log");
                ExternalEvent.Raise();
            }
            else if (ExternalEvent == null)
            {
                Logger.LogToFile("WARNING: ExternalEvent is null, cannot raise pull event", "sync.log");
            }
            else
            {
                Logger.LogToFile("PULL EVENT PENDING: ExternalEvent already pending, not raising again", "sync.log");
            }
        }

        public void Execute(UIApplication app)
        {
            Logger.LogToFile($"PULL EXECUTE STARTED: Processing {_queue.Count} documents in queue at {DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");
            
            int processedCount = 0;
            while (_queue.TryDequeue(out var doc))
            {
                if (doc != null)
                {
                    processedCount++;
                    Logger.LogToFile($"PULL PROCESSING: Document {processedCount} - '{doc.Title}'", "sync.log");
                    Handle(doc, CommandManager.Instance.SessionId);
                }
                else
                {
                    Logger.LogToFile("WARNING: Null document found in pull queue", "sync.log");
                }
            }

            Logger.LogToFile($"PULL EXECUTE COMPLETED: Processed {processedCount} documents", "sync.log");
            
            if (!_queue.IsEmpty && ExternalEvent != null)
            {
                Logger.LogToFile($"PULL QUEUE NOT EMPTY: {_queue.Count} documents remaining, raising event again", "sync.log");
                ExternalEvent.Raise();
            }
        }

        public void Handle(Document doc, string sessionId)
        {
            var startTime = DateTime.Now;
            
            // KRITISCH: Pull-Modus aktivieren VOR der Transaction
            CommandManager.Instance.IsPullInProgress = true;
            
            try
            {
                // Check document state before starting
                if (doc == null || doc.IsReadOnly)
                {
                    Logger.LogToFile("PULL ABORTED: Document is null or read-only, skipping pull", "sync.log");
                    return;
                }

                Logger.LogToFile($"PULL HANDLER STARTED: Session {sessionId} on document '{doc.Title}' at {startTime:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");

                using (var tx = new Transaction(doc, "NeoCollab Pull"))
                {
                    Logger.LogToFile("PULL TRANSACTION STARTED: Beginning Revit transaction", "sync.log");
                    tx.Start();
                    try
                    {
                        // Delegation an bestehenden Puller (keine neue Klasse!)
                        Logger.LogToFile("PULL APPLYING CHANGES: Calling ApplyPendingWallChanges", "sync.log");
                        _puller.ApplyPendingWallChanges(doc, sessionId);

                        tx.Commit();
                        var duration = DateTime.Now - startTime;
                        Logger.LogToFile($"PULL TRANSACTION COMMITTED: Successfully completed in {duration.TotalMilliseconds:F0}ms", "sync.log");
                    }
                    catch (Exception ex)
                    {
                        tx.RollBack();
                        var duration = DateTime.Now - startTime;
                        Logger.LogCrash($"PULL TRANSACTION FAILED: Rolled back after {duration.TotalMilliseconds:F0}ms", ex);
                        
                        // Show user-friendly error message
                        Autodesk.Revit.UI.TaskDialog.Show("NeoCollab Pull", 
                            $"Pull failed: {ex.Message}\n\nCheck log files for details. Try using 'Acknowledge All' to reset the sync state.");
                        
                        // Don't re-throw to prevent Revit from hanging
                    }
                }
            }
            catch (Exception ex)
            {
                var duration = DateTime.Now - startTime;
                Logger.LogCrash($"PULL HANDLER CRITICAL ERROR: Failed after {duration.TotalMilliseconds:F0}ms", ex);
                
                // Show critical error dialog but don't crash Revit
                Autodesk.Revit.UI.TaskDialog.Show("NeoCollab Critical Error", 
                    $"Critical error in pull handler: {ex.Message}\n\nRevit should remain stable. Check logs for details.");
            }
            finally
            {
                // KRITISCH: Pull-Modus deaktivieren NACH der Transaction
                CommandManager.Instance.IsPullInProgress = false;
                Logger.LogToFile("PULL TRANSACTION: Pull transaction completed, automatic push re-enabled", "sync.log");
            }
        }

        public string GetName() => "GraphPullHandler";
    }
}
