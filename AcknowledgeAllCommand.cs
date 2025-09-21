using Autodesk.Revit.Attributes;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System;
using System.Threading.Tasks;
using Neo4j.Driver;

namespace NeoCollab
{
    [Transaction(TransactionMode.Manual)]
    [Regeneration(RegenerationOption.Manual)]
    public class AcknowledgeAllCommand : IExternalCommand
    {
        public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
        {
            try
            {
                Logger.LogToFile("AcknowledgeAllCommand started - acknowledging ALL sessions", "sync.log");

                // Acknowledge all ChangeLog entries from ALL sessions synchronously for better error handling
                try
                {
                    var connector = CommandManager.Instance.Neo4jConnector;
                    
                    // First check what exists
                    Logger.LogToFile("Checking existing ChangeLog entries before acknowledge...", "sync.log");
                    
                    // Execute acknowledge all
                    connector.AcknowledgeAllChangeLogsAsync().GetAwaiter().GetResult();
                    
                    Logger.LogToFile("AcknowledgeAll completed successfully", "sync.log");
                    
                    Autodesk.Revit.UI.TaskDialog.Show("NeoCollab", 
                        "Alle ChangeLog-Einträge von ALLEN Sessions wurden erfolgreich als acknowledged markiert.\nÜberprüfen Sie sync.log für Details.");
                }
                catch (Exception ex)
                {
                    Logger.LogCrash("AcknowledgeAllCommand execution failed", ex);
                    Autodesk.Revit.UI.TaskDialog.Show("NeoCollab Fehler", 
                        $"Fehler beim Acknowledge All: {ex.Message}\nÜberprüfen Sie die Log-Dateien für Details.");
                    return Result.Failed;
                }

                return Result.Succeeded;
            }
            catch (Exception ex)
            {
                Logger.LogCrash("AcknowledgeAllCommand", ex);
                message = ex.Message;
                return Result.Failed;
            }
        }
    }
}
