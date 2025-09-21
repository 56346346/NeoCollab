using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System.IO.Compression;
using System.Xml.Linq;
using System.IO;                     // für Path
using Autodesk.Revit.DB.Architecture;// für Room
using NeoCollab;
using System.Threading;

namespace NeoCollab
{
    public class DatabaseUpdateHandler : IExternalEventHandler
    {
        private readonly ConcurrentQueue<ChangeData> _changeQueue = new ConcurrentQueue<ChangeData>();

        private readonly SpaceExtractor _extractor;


        private static MarkSeverityHandler _markHandler = new MarkSeverityHandler();
        private ExternalEvent _markEvent;

        private ExternalEvent _externalEvent;
        private readonly CypherPushHandler _pushHandler = new CypherPushHandler();
        private ExternalEvent _pushEvent;

        // Wird aufgerufen, wenn Änderungen verarbeitet werden sollen.
        // Erstellt IFC-Teilexporte und triggert anschließend die Solibri-Prüfung.    
        public void Execute(UIApplication app)
        {
            var doc = app.ActiveUIDocument?.Document;
            if (doc == null) return;
            List<ChangeData> processedChanges = new List<ChangeData>();
            try
            {
                // 1. Prozessiere alle wartenden Changes
                while (_changeQueue.TryDequeue(out var change))
                {
                    processedChanges.Add(change);
                    _extractor.UpdateGraph(doc,
                                           change.AddedElements,
                                           change.DeletedElementIds,
                                           change.ModifiedElements);
                }

                // CRITICAL FIX: Immediately push cypher commands to Neo4j after UpdateGraph
                if (_pushEvent != null && !_pushEvent.IsPending)
                {
                    _pushEvent.Raise();
                    Logger.LogToFile("DATABASE UPDATE: Push event raised after UpdateGraph", "sync.log");
                    
                    // SOLIBRI FIX: Trigger Solibri validation after Neo4j push completes
                    // This ensures Solibri is always up-to-date and not one iteration behind
                    Task.Run(async () =>
                    {
                        try
                        {
                            // Small delay to ensure Neo4j push completes
                            await Task.Delay(500);
                            
                            Logger.LogToFile("DATABASE UPDATE SOLIBRI: Triggering validation after UpdateGraph", "solibri.log");
                            
                            var solibriService = new SolibriValidationService();
                            var allChangedIds = processedChanges
                                .SelectMany(c => c.AddedElements.Select(e => e.Id.Value)
                                    .Concat(c.ModifiedElements.Select(e => e.Id.Value)))
                                .Distinct()
                                .ToList();
                            
                            var allDeletedIds = processedChanges
                                .SelectMany(c => c.DeletedElementIds.Select(id => id.Value))
                                .Distinct()
                                .ToList();
                            
                            Logger.LogToFile($"DATABASE UPDATE SOLIBRI: Validating {allChangedIds.Count} changed and {allDeletedIds.Count} deleted elements", "solibri.log");
                            
                            if (allChangedIds.Count > 0 || allDeletedIds.Count > 0)
                            {
                                var changedElementIds = allChangedIds.Select(id => new ElementId(id)).ToList();
                                var deletedElementIds = allDeletedIds.Select(id => new ElementId(id)).ToList();
                                
                                await solibriService.ValidateChangeLogElementsAsync(app.ActiveUIDocument.Document, changedElementIds, deletedElementIds, new List<string>());
                                Logger.LogToFile("DATABASE UPDATE SOLIBRI: Validation completed successfully", "solibri.log");
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DATABASE UPDATE SOLIBRI: Validation failed: {ex.Message}", "solibri.log");
                        }
                    });
                }
                else
                {
                    Logger.LogToFile("DATABASE UPDATE: Push event is pending or null, skipping immediate push", "sync.log");
                }

            }
            catch (Exception ex)
            {
                // Absichern gegen jeden Fehler im Handler
                Logger.LogCrash("ExternalEvent", ex);
            }


            try
            {
                // 1. Betroffene + Kontext-ElementIds sammeln
                var deltaIds = processedChanges
                    .SelectMany(c => c.AddedElements.Select(e => e.Id)
                        .Concat(c.ModifiedElements.Select(e => e.Id)))
                    .Distinct()
                    .ToList();

                var removedGuids = processedChanges
               .SelectMany(c => c.DeletedUids)
               .Distinct()
               .ToList();


                // Beispiel: für Räume angrenzende Wände ergänzen
                foreach (var change in processedChanges)
                {
                    foreach (var room in change.AddedElements.OfType<Room>()
                        .Concat(change.ModifiedElements.OfType<Room>()))
                    {
                        var boundaries = room.GetBoundarySegments(new SpatialElementBoundaryOptions());
                        foreach (var segList in boundaries)
                            foreach (var seg in segList)
                                if (!deltaIds.Contains(seg.ElementId))
                                    deltaIds.Add(seg.ElementId);
                    }
                }

                // 2. IFC-Subset exportieren
                string ifcPath = _extractor.ExportIfcSubset(app.ActiveUIDocument.Document, deltaIds);
                if (string.IsNullOrWhiteSpace(ifcPath) || !File.Exists(ifcPath))
                {
                    Logger.LogToFile("IFC-Export fehlgeschlagen. Versuche erneut.", "solibri.log");
                    ifcPath = _extractor.ExportIfcSubset(app.ActiveUIDocument.Document, deltaIds);
                    if (string.IsNullOrWhiteSpace(ifcPath) || !File.Exists(ifcPath))
                    {
                        Logger.LogToFile("IFC-Export weiterhin fehlgeschlagen, Solibri-Aufrufe werden \u00fcbersprungen.", "solibri.log");
                        return;
                    }
                }
                var guidMap = _extractor.MapIfcGuidsToRevitIds(ifcPath, deltaIds);

                // 3. Solibri REST API-Aufrufe asynchron verarbeiten
                _ = Task.Run(async () =>
              {
                  Dictionary<ElementId, string> severityMap = new();

                  try
                  {
                      // NOTE: Solibri integration is now handled by SolibriValidationService via event-based system
                      Logger.LogToFile("Solibri validation is now handled by SolibriValidationService via Neo4j completion events", "solibri.log");
                      
                      // All Solibri functionality has been moved to SolibriValidationService
                      // This ensures better timing and integration with the ChangeLog system
                      await Task.Delay(1); // Minimal delay to make async proper
                  }
                  catch (Exception ex)
                  {
                      Logger.LogToFile($"Skipped deprecated Solibri validation: {ex.Message}", "solibri.log");
                  }
                  finally
                  {
                  if (severityMap.Count > 0 && _markEvent != null && !_markEvent.IsPending)
                  {
                      _markHandler.SeverityMap = severityMap;
                      _markEvent.Raise();
                  }
                      if (_pushEvent != null && !_pushEvent.IsPending)
                          _pushEvent.Raise();
                  }
              });
            }

            catch (Exception ex)
            {
                Logger.LogCrash("Solibri Delta-Prüfung", ex);
            }
        }

        private static (IssueSeverity Severity, Dictionary<ElementId, string> Map) ProcessClashResults(IEnumerable<ClashResult> results, Dictionary<string, ElementId> guidMap)
        {
            IssueSeverity worst = IssueSeverity.None;
            var severityMap = new Dictionary<ElementId, string>();
            var session = SessionManager.OpenSessions.Values.FirstOrDefault();
            var doc = session?.Document;

            foreach (var clash in results)
            {
                if (string.IsNullOrEmpty(clash.ComponentGuid))
                    continue;

                string sevText = clash.Severity ?? string.Empty;
                IssueSeverity sev = IssueSeverity.None;
                sevText = sevText.Trim().ToUpperInvariant();
                if (sevText == "ROT" || sevText == "RED" || sevText == "ERROR" || sevText == "HIGH" || sevText == "CRITICAL")
                    sev = IssueSeverity.Error;
                else if (sevText == "GELB" || sevText == "YELLOW" || sevText == "WARNING" || sevText == "MEDIUM")
                    sev = IssueSeverity.Warning;

                if (sev > worst)
                    worst = sev;

                guidMap.TryGetValue(clash.ComponentGuid, out var revitId);
                string idPart = revitId != ElementId.InvalidElementId ? $", elementId: {revitId.Value}" : string.Empty;
                string msg = clash.Message?.Replace("'", "\'") ?? string.Empty;
                string cy = $@"MERGE (e {{ ifcGuid: '{clash.ComponentGuid}'{idPart} }})
MERGE (i:Issue {{ title: '{msg}', description: '{msg}' }})
MERGE (e)-[:HAS_ISSUE]->(i)";
                CommandManager.Instance.cypherCommands.Enqueue(cy);

                if (doc != null)
                {
                    var elem = doc.GetElement(clash.ComponentGuid);
                    if (elem != null)
                    {
                        string color = sev == IssueSeverity.Error ? "RED" :
                            sev == IssueSeverity.Warning ? "YELLOW" : "GREEN";
                        if (severityMap.TryGetValue(elem.Id, out string existing))
                        {
                            if (existing == "YELLOW" && color == "RED")
                                severityMap[elem.Id] = color;
                        }
                        else if (color != "GREEN")
                        {
                            severityMap[elem.Id] = color;
                        }
                    }
                }
            }

            return (worst, severityMap);

        }


        // Name des ExternalEvents.
        public string GetName() => "NeoCollab Real-Time Sync";

        // Reicht eine Änderung zur späteren Verarbeitung ein und startet das Event.
        public void EnqueueChange(ChangeData data)
        {
            _changeQueue.Enqueue(data);
            RaiseEvent();
        }

        private enum IssueSeverity { None, Warning, Error }
        // Wertet eine BCF-Datei aus, schreibt gefundene Issues nach Neo4j und
        // gibt die schwerste aufgetretene Stufe zurück.
        public void Initialize()
        {
            _externalEvent = ExternalEvent.Create(this);
            _pushEvent = ExternalEvent.Create(_pushHandler);
            _markEvent = ExternalEvent.Create(_markHandler);
        }

        /// <summary>
        /// Exposes the push event so other classes can trigger an immediate
        /// transfer of queued Cypher commands to Neo4j.
        /// </summary>
        // Stößt einen sofortigen Push aller gesammelten Cypher-Befehle an.

        public void TriggerPush()
        {
            if (_pushEvent != null && !_pushEvent.IsPending)
            {
                _pushEvent.Raise();
            }
        }

        // Konstruktor, benötigt einen SpaceExtractor zur Graph-Aktualisierung.

        public DatabaseUpdateHandler(SpaceExtractor extractor)
        {

            _extractor = extractor;
            _externalEvent = ExternalEvent.Create(this);
            _pushEvent = ExternalEvent.Create(_pushHandler);
            _markEvent = ExternalEvent.Create(_markHandler);
        }

        // Hebt das ExternalEvent an, falls es nicht bereits aussteht
        private void RaiseEvent()
        {
            if (_externalEvent != null && !_externalEvent.IsPending)
            {
                _externalEvent.Raise();
            }
        }
    }

    public class ChangeData
    {
        public List<Element> AddedElements { get; set; } = new List<Element>();
        public List<ElementId> DeletedElementIds { get; set; } = new List<ElementId>();
        public List<string> DeletedUids { get; set; } = new List<string>();

        public List<Element> ModifiedElements { get; set; } = new List<Element>();
    }

}