using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System.Collections.Generic;

namespace NeoCollab
{
    public class MarkSeverityHandler : IExternalEventHandler
    {
        public Dictionary<ElementId, string> SeverityMap { get; set; } = new();

        public void Execute(UIApplication app)
        {
            if (SeverityMap != null && SeverityMap.Count > 0)
                NeoCollabClass.MarkElementsBySeverity(SeverityMap);
            SeverityMap = new();
        }

        public string GetName() => "MarkSeverityHandler";
    }
}