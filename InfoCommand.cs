using Autodesk.Revit.Attributes;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;

namespace NeoCollab
{
    [Transaction(TransactionMode.Manual)]
    [Regeneration(RegenerationOption.Manual)]
    public class InfoCommand : IExternalCommand
    {
                // Zeigt eine kurze Erläuterung der verfügbaren Befehle an.

        public Result Execute(ExternalCommandData commandData, ref string message, ElementSet elements)
        {
            string infoText =
                "Export to Neo4j: Exportiert das gesamte Modell in die Datenbank.\n" +
                "Pull Changes: Holt externe Änderungen in das Modell.\n" +
                "Consistency Check: Prüft die Konsistenz zwischen Modell und Datenbank.\n\n" +
                "DEBUG: Cleanup invalid ChangeLog entries? (Click Yes to run cleanup)";

            var result = Autodesk.Revit.UI.TaskDialog.Show("NeoCollab Info", infoText, 
                Autodesk.Revit.UI.TaskDialogCommonButtons.Yes | Autodesk.Revit.UI.TaskDialogCommonButtons.No);
            
            if (result == Autodesk.Revit.UI.TaskDialogResult.Yes)
            {
                try
                {
                    CleanupScript.CleanupInvalidChangeLogEntries().GetAwaiter().GetResult();
                    Autodesk.Revit.UI.TaskDialog.Show("Cleanup Complete", 
                        "Invalid ChangeLog entries cleaned up. Check sync.log for details.");
                }
                catch (System.Exception ex)
                {
                    Autodesk.Revit.UI.TaskDialog.Show("Cleanup Failed", 
                        $"Failed to cleanup: {ex.Message}");
                }
            }
            
            return Result.Succeeded;
        }
    }
}