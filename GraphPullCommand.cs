using Autodesk.Revit.Attributes;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System;
using System.Runtime.Versioning;

namespace NeoCollab
{
    [Transaction(TransactionMode.Manual)]
    [Regeneration(RegenerationOption.Manual)]
    [SupportedOSPlatform("windows")]
    public class GraphPullCommand : IExternalCommand
    {
        public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
        {
            var startTime = DateTime.Now;
            var doc = commandData.Application.ActiveUIDocument?.Document;
            if (doc == null)
            {
                Logger.LogToFile("PULL COMMAND FAILED: No active document found", "sync.log");
                Autodesk.Revit.UI.TaskDialog.Show("NeoCollab", "Kein aktives Dokument gefunden.");
                return Result.Failed;
            }

            try
            {
                var sessionId = CommandManager.Instance.SessionId;
                Logger.LogToFile($"PULL COMMAND STARTED: User initiated pull for document '{doc.Title}' with session {sessionId} at {startTime:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");
                
                // Use the global GraphPullHandler instance from NeoCollabClass
                var pullHandler = NeoCollabClass.GraphPullHandlerInstance;
                if (pullHandler == null)
                {
                    Logger.LogToFile("PULL COMMAND FALLBACK: GraphPullHandler instance not available, using direct call", "sync.log");
                    // Fallback: Call directly
                    var directHandler = new GraphPullHandler();
                    directHandler.Handle(doc, sessionId);
                }
                else
                {
                    Logger.LogToFile("PULL COMMAND DISPATCH: GraphPullHandler instance found, calling RequestPull", "sync.log");
                    pullHandler.RequestPull(doc);
                }
                
                var duration = DateTime.Now - startTime;
                Logger.LogToFile($"PULL COMMAND COMPLETED: Pull request finished in {duration.TotalMilliseconds:F0}ms", "sync.log");
                Autodesk.Revit.UI.TaskDialog.Show("NeoCollab", "Pull-Request wurde gestartet. Überprüfen Sie die Logs für Details.");
                return Result.Succeeded;
            }
            catch (Exception ex)
            {
                var duration = DateTime.Now - startTime;
                Logger.LogToFile($"PULL COMMAND ERROR: Pull command failed after {duration.TotalMilliseconds:F0}ms - {ex.Message}", "sync.log");
                Logger.LogCrash("GraphPullCommand failed", ex);
                message = ex.Message;
                Autodesk.Revit.UI.TaskDialog.Show("NeoCollab Fehler", $"Pull-Command fehlgeschlagen: {ex.Message}");
                return Result.Failed;
            }
        }
    }
}