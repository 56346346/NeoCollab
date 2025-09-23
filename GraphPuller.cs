using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Versioning;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Plumbing;
using Autodesk.Revit.DB.Structure;
using System.Linq;
using Autodesk.Revit.UI;
using Neo4j.Driver;
using System.IO;
using static System.Environment;




namespace NeoCollab;

[SupportedOSPlatform("windows")]
public class GraphPuller
{
    private bool _pullInProgress = false;
    private string _currentTargetSessionId = string.Empty; // Session ID of the target session for current pull operation

    private static readonly string _logDir =
       Path.Combine(GetFolderPath(Environment.SpecialFolder.ApplicationData),
       "NeoCollab", "log");

    static GraphPuller()
    {
        if (!Directory.Exists(_logDir))
            Directory.CreateDirectory(_logDir);
        MethodLogger.InitializeLog(nameof(GraphPuller));
    }

    private static void LogMethodCall(string methodName, Dictionary<string, object> parameters)
    {
        MethodLogger.Log(nameof(GraphPuller), methodName, parameters);
    }
    private readonly Neo4jConnector _connector;
    private readonly SolibriValidationService _solibriValidationService;
    public DateTime LastPulledAt { get; private set; } = DateTime.MinValue;
    // Erzeugt den Puller mit optionalem Connector

    public GraphPuller(Neo4jConnector connector = null)
    {
        _connector = connector ?? CommandManager.Instance.Neo4jConnector;
        _solibriValidationService = new SolibriValidationService();
    }

    // Applies pending changes from ChangeLog entries for this session (all element types)
    public void ApplyPendingWallChanges(Document doc, string sessionId)
    {
        var startTime = DateTime.Now;
        try
        {
            // Store target sessionId for use in marking methods
            _currentTargetSessionId = sessionId;
            
            Logger.LogToFile($"PULL APPLY STARTED: ApplyPendingElementChanges for session {sessionId} on document '{doc.Title}' at {startTime:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");

            // 1) Load pending changes using new ChangeLog method
            Logger.LogToFile("PULL LOADING CHANGES: Calling GetPendingChangeLogsAsync", "sync.log");
            var changes = _connector.GetPendingChangeLogsAsync(sessionId).GetAwaiter().GetResult();
            Logger.LogToFile($"PULL CHANGES LOADED: Found {changes.Count} pending element changes for session {sessionId}", "sync.log");

            if (changes.Count == 0)
            {
                // DISABLED: Test entries creation disabled while fixing sync issues
                Logger.LogToFile("PULL NO CHANGES: No changes found. Test entry creation temporarily disabled.", "sync.log");
                Logger.LogToFile("PULL DEBUG: If this persists, check if ChangeLog entries are being created properly in other session", "sync.log");
                return; // Exit early instead of creating test entries
                
                /* DISABLED - was creating ElementId 999 test entries
                // Try to create test entries for debugging
                Logger.LogToFile("PULL NO CHANGES: No changes found, creating test ChangeLog entries for debugging...", "sync.log");
                _connector.CreateTestChangeLogEntriesAsync(sessionId).GetAwaiter().GetResult();

                // Try again after creating test entries
                Logger.LogToFile("PULL RETRY: Retrying GetPendingChangeLogsAsync after creating test entries", "sync.log");
                changes = _connector.GetPendingChangeLogsAsync(sessionId).GetAwaiter().GetResult();
                Logger.LogToFile($"PULL TEST ENTRIES: After creating test entries, found {changes.Count} pending element changes", "sync.log");
                */
            }

            // 2) Apply each change
            int processedCount = 0;
            var processedElementIds = new List<long>(); // Track local ElementIds for Solibri
            
            foreach (var (changeId, op, elementProperties) in changes)
            {
                processedCount++;
                try
                {
                    var elementId = elementProperties.ContainsKey("elementId") ?
                        Convert.ToInt32(elementProperties["elementId"]) : -1;
                    
                    // Determine element type from properties
                    string elementType = DetermineElementType(elementProperties);
                    
                    // DETAILED CHANGELOG LOGGING
                    // &
                    Logger.LogToFile($"CHANGELOG ENTRY: ChangeId={changeId}, Operation={op}, ElementId={elementId}, Type={elementType}", "sync.log");
                    Logger.LogToFile($"CHANGELOG ENTRY: processedCount={processedCount}/{changes.Count}", "sync.log");
                    Logger.LogToFile($"CHANGELOG ENTRY: targetSession={sessionId}", "sync.log");
                    
                    // Log all properties for ProvisionalSpaces
                    if (elementType == "ProvisionalSpace")
                    {
                        // Logger.LogToFile($"PROVISIONAL SPACE CHANGELOG: All properties for ElementId={elementId}:", "sync.log");
                        foreach (var prop in elementProperties)
                        {
                            Logger.LogToFile($"  PROP: {prop.Key} = {prop.Value ?? "NULL"}", "sync.log");
                        }
                    }
                    
                    // Logger.LogToFile($"PULL PROCESSING CHANGE {processedCount}/{changes.Count}: ChangeId={changeId}, Operation={op}, ElementId={elementId}, Type={elementType}", "sync.log");

                    // Validate elementProperties before processing
                    if (elementProperties == null || elementProperties.Count == 0)
                    {
                        Logger.LogToFile($"WARNING: Empty element properties for change {changeId}, acknowledging anyway", "sync.log");
                    }
                    else if (elementId == -1)
                    {
                        Logger.LogToFile($"WARNING: Invalid ElementId for change {changeId}, acknowledging anyway", "sync.log");
                    }
                    else
                    {
                        switch (op)
                        {
                            case "Create":
                            case "Insert":
                            case "Add":
                                Logger.LogToFile($"Processing {op} operation for {elementType} {elementId}", "sync.log");
                                if (!elementProperties.ContainsKey("__deleted__"))
                                {
                                    ApplyElementUpsert(doc, elementProperties, elementType);
                                    Logger.LogToFile($"Successfully applied {op} for {elementType} {elementId}", "sync.log");
                                }
                                break;

                            case "Modify":
                                Logger.LogToFile($"Processing Modify operation for {elementType} {elementId}", "sync.log");
                                if (!elementProperties.ContainsKey("__deleted__"))
                                {
                                    // CRITICAL FIX: Modify should only update existing elements, not create new ones
                                    // This is different from Create/Insert which use ApplyElementUpsert
                                    long localElementId;
                                    var updateResult = ApplyElementUpdate(doc, elementProperties, elementType, out localElementId);
                                    if (updateResult)
                                    {
                                        // Track the local ElementId for Solibri if successfully updated
                                        if (localElementId != -1)
                                        {
                                            processedElementIds.Add(localElementId);
                                            Logger.LogToFile($"MODIFY SUCCESS: Added local ElementId {localElementId} to Solibri tracking", "sync.log");
                                        }
                                        Logger.LogToFile($"Successfully applied {op} for {elementType} {elementId}", "sync.log");
                                    }
                                    else
                                    {
                                        Logger.LogToFile($"WARNING: Modify operation failed - element {elementType} {elementId} does not exist for update", "sync.log");
                                    }
                                }
                                break;

                            case "Delete":
                                Logger.LogToFile($"Processing Delete operation for {elementType} {elementId}", "sync.log");
                                // Check if we have geographic properties for reliable deletion
                                var hasGeographicData = HasRequiredGeographicProperties(elementProperties, elementType);
                                
                                if (hasGeographicData)
                                {
                                    Logger.LogToFile($"Delete: Using geographic deletion for {elementType} {elementId} (has geographic data)", "sync.log");
                                    DeleteElementByGeographicProperties(doc, elementProperties, elementType);
                                }
                                else
                                {
                                    Logger.LogToFile($"Delete: Missing geographic data for {elementType} {elementId}, attempting to load from Neo4j", "sync.log");
                                    // Try to load full properties from Neo4j for geographic deletion
                                    var fullProperties = LoadElementPropertiesFromNeo4j(elementId, elementType);
                                    if (fullProperties != null && HasRequiredGeographicProperties(fullProperties, elementType))
                                    {
                                        Logger.LogToFile($"Delete: Loaded geographic data from Neo4j for {elementType} {elementId}", "sync.log");
                                        DeleteElementByGeographicProperties(doc, fullProperties, elementType);
                                    }
                                    else
                                    {
                                        Logger.LogToFile($"Delete: Fallback to ElementId-based deletion for {elementType} {elementId}", "sync.log");
                                        DeleteElementByRemoteElementId(doc, elementId, elementType, elementProperties);
                                    }
                                }
                                Logger.LogToFile($"Successfully processed delete operation for {elementType} {elementId}", "sync.log");
                                break;

                            default:
                                Logger.LogToFile($"Unknown operation: {op} for change {changeId}, acknowledging anyway", "sync.log");
                                break;
                        }
                    }

                    // 3) Acknowledge this specific ChangeLog entry
                    Logger.LogToFile($"PULL ACKNOWLEDGING: Acknowledging ChangeLog entry {changeId}", "sync.log");
                    _connector.AcknowledgeChangeLogAsync(changeId).GetAwaiter().GetResult();
                    Logger.LogToFile($"PULL ACKNOWLEDGED: Successfully acknowledged change {changeId}", "sync.log");

                    Logger.LogToFile($"PULL CHANGE COMPLETE: Applied and acknowledged change {changeId} ({op}) for element {elementId}", "sync.log");
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"PULL CHANGE ERROR: Error applying wall change {changeId} - {ex.Message}", "sync.log");
                    Logger.LogCrash($"Error applying wall change {changeId}", ex);
                    // Continue with next change instead of failing completely
                }
            }

            var duration = DateTime.Now - startTime;
            Logger.LogToFile($"PULL APPLY COMPLETED: ApplyPendingWallChanges finished. Processed {processedCount} changes in {duration.TotalMilliseconds:F0}ms", "sync.log");

            // CRITICAL FIX: Fire OnNeo4jOperationsCompleted event after pull to trigger Solibri validation
            // This ensures Solibri gets the pulled elements immediately, not one push cycle later
            if (processedCount > 0)
            {
                try
                {
                    Logger.LogToFile($"PULL SOLIBRI TRIGGER: Firing OnNeo4jOperationsCompleted for {processedCount} pulled elements", "sync.log");
                    Logger.LogToFile($"PULL SOLIBRI ELEMENTS: Tracking {processedElementIds.Count} local ElementIds for Solibri", "sync.log");
                    
                    // Use the collected local ElementIds for Solibri validation
                    CommandManager.TriggerNeo4jOperationsCompleted(doc, processedElementIds, new List<long>());
                    Logger.LogToFile($"PULL SOLIBRI TRIGGER: Successfully triggered Solibri validation after pull", "sync.log");
                }
                catch (Exception solibriEx)
                {
                    Logger.LogToFile($"PULL SOLIBRI TRIGGER ERROR: Failed to trigger Solibri after pull: {solibriEx.Message}", "sync.log");
                    Logger.LogCrash("Pull Solibri trigger failed", solibriEx);
                }
            }
        }
        catch (Exception ex)
        {
            var duration = DateTime.Now - startTime;
            Logger.LogToFile($"PULL APPLY FAILED: ApplyPendingWallChanges failed after {duration.TotalMilliseconds:F0}ms - {ex.Message}", "sync.log");
            Logger.LogCrash("ApplyPendingWallChanges failed", ex);
            throw;
        }
        finally
        {
            // Clear target sessionId after pull operation
            _currentTargetSessionId = string.Empty;
        }
    }

    /// <summary>
    /// Upserts a wall from ChangeLog properties dictionary
    /// </summary>
    private void UpsertWallFromGraphProperties(Document doc, Dictionary<string, object> w)
    {
        try
        {
            Logger.LogToFile($"UpsertWallFromGraphProperties called with {w.Keys.Count} properties", "sync.log");

            // --- 1) Validate required fields ---
            if (!w.ContainsKey("elementId"))
            {
                Logger.LogToFile("Wall properties missing elementId, skipping", "sync.log");
                return;
            }

            // Validate geometry fields
            var requiredGeometryFields = new[] { "x1", "y1", "z1", "x2", "y2", "z2" };
            var missingGeometryFields = requiredGeometryFields.Where(field => !w.ContainsKey(field)).ToList();
            if (missingGeometryFields.Any())
            {
                Logger.LogToFile($"Wall properties missing required geometry fields: {string.Join(", ", missingGeometryFields)}, skipping", "sync.log");
                return;
            }

            // Validate level information
            if (!w.ContainsKey("baseLevelUid") && !w.ContainsKey("levelId"))
            {
                Logger.LogToFile("Wall properties missing both baseLevelUid and levelId, skipping", "sync.log");
                return;
            }

            // Validate wall type information
            if (!w.ContainsKey("typeName") && !w.ContainsKey("familyName") && !w.ContainsKey("typeId") && !w.ContainsKey("thickness_m") && !w.ContainsKey("thickness_mm"))
            {
                Logger.LogToFile("Wall properties missing wall type information (typeName, familyName, typeId, or thickness), skipping", "sync.log");
                return;
            }

            // Log all available properties for debugging
            // Logger.LogToFile($"Wall properties available: {string.Join(", ", w.Keys)}", "sync.log");
            // Logger.LogToFile($"Geometry: x1={w.GetValueOrDefault("x1", "N/A")}, y1={w.GetValueOrDefault("y1", "N/A")}, z1={w.GetValueOrDefault("z1", "N/A")}, x2={w.GetValueOrDefault("x2", "N/A")}, y2={w.GetValueOrDefault("y2", "N/A")}, z2={w.GetValueOrDefault("z2", "N/A")}", "sync.log");

            // baseLevelUid is now required and should be stored
            if (!w.ContainsKey("baseLevelUid"))
            {
                Logger.LogToFile("Wall properties missing baseLevelUid, trying to reconstruct from levelId", "sync.log");

                // Try to reconstruct baseLevelUid from levelId
                if (w.ContainsKey("levelId"))
                {
                    var levelId = Convert.ToInt64(w["levelId"]);
                    var level = doc.GetElement(new ElementId(levelId)) as Level;
                    if (level != null)
                    {
                        w["baseLevelUid"] = level.UniqueId;
                        Logger.LogToFile($"Reconstructed baseLevelUid: {level.UniqueId}", "sync.log");
                    }
                }

                if (!w.ContainsKey("baseLevelUid"))
                {
                    Logger.LogToFile("Could not determine baseLevelUid, skipping", "sync.log");
                    return;
                }
            }

            // --- 2) Extract ElementId properly ---
            var remoteElementId = Convert.ToInt32(w["elementId"]);
            Logger.LogToFile($"WALLDUP: Processing wall with ElementId {remoteElementId}", "sync.log");

            // --- 3) Find existing wall by remote tag OR original ElementId ---
            Logger.LogToFile($"WALLDUP: Searching for existing wall with remoteElementId={remoteElementId}", "sync.log");
            var existingWall = FindElementByRemoteElementId<Wall>(doc, remoteElementId);

            if (existingWall != null)
            {
                Logger.LogToFile($"WALLDUP: FOUND existing wall {existingWall.Id} for remote ElementId {remoteElementId}", "sync.log");
                Logger.LogToFile($"Updating existing wall {existingWall.Id} for remote ElementId {remoteElementId}", "sync.log");
                
                // DUPLICATE DETECTION: Check if update is needed
                bool isIdentical = IsWallIdentical(existingWall, w);
                if (isIdentical)
                {
                    Logger.LogToFile($"WALLDUP: Wall {existingWall.Id} is IDENTICAL to Neo4j data - SKIPPING update", "sync.log");
                    Logger.LogToFile($"WALL DUPLICATE: Wall {existingWall.Id} is IDENTICAL to Neo4j data - SKIPPING update", "sync.log");
                    return; // Skip update for identical walls
                }
                
                Logger.LogToFile($"UPDATE WALL: Wall {existingWall.Id} needs update - proceeding", "sync.log");
                
                // Update existing wall geometry and properties
                UpdateWallGeometry(existingWall, w);
                UpdateWallProperties(existingWall, w);
                
                Logger.LogToFile($"Successfully updated wall {existingWall.Id}", "sync.log");
            }
            else
            {
                Logger.LogToFile($"WALLDUP: NO existing wall found for remoteElementId={remoteElementId}", "sync.log");
                // --- 4) Check for existing identical walls ONLY when creating new ones ---
                var identicalWall = FindIdenticalWall(doc, w, remoteElementId);
                if (identicalWall != null)
                {
                    Logger.LogToFile($"WALLDUP: Found identical untagged wall {identicalWall.Id} - will use instead of creating new", "sync.log");
                    Logger.LogToFile($"WALL DUPLICATE DETECTION: Found identical wall {identicalWall.Id} for remote ElementId {remoteElementId} - USING existing wall instead of creating duplicate", "sync.log");
                    
                    // Mark the identical wall with remote ID if not already tagged
                    var existingTag = identicalWall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    if (string.IsNullOrEmpty(existingTag) || !existingTag.Contains($"NeoCollab:ElementId={remoteElementId}"))
                    {
                        MarkWallWithRemoteId(identicalWall, remoteElementId);
                        Logger.LogToFile($"WALLDUP: Successfully marked identical wall {identicalWall.Id} with remote ElementId {remoteElementId}", "sync.log");
                        Logger.LogToFile($"WALL DUPLICATE: Marked existing identical wall {identicalWall.Id} with remote ElementId {remoteElementId}", "sync.log");
                    }
                    return; // Exit without creating duplicate
                }

                Logger.LogToFile($"WALLDUP: NO identical wall found - will create new wall for remoteElementId={remoteElementId}", "sync.log");
                Logger.LogToFile($"Creating new wall for remote ElementId {remoteElementId}", "sync.log");
                // Create new wall
                var newWall = CreateWallFromProperties(doc, w);
                if (newWall != null)
                {
                    MarkWallWithRemoteId(newWall, remoteElementId);
                    Logger.LogToFile($"WALLDUP: Successfully created and marked new wall {newWall.Id} with remote ElementId {remoteElementId}", "sync.log");
                    Logger.LogToFile($"Successfully created and marked wall {newWall.Id} with remote ElementId {remoteElementId}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"WALLDUP: FAILED to create wall for remote ElementId {remoteElementId}", "sync.log");
                    Logger.LogToFile($"Failed to create wall for remote ElementId {remoteElementId}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash("UpsertWallFromGraphProperties failed", ex);
            throw; // Re-throw to ensure proper error handling
        }
    }

    /// <summary>
    /// Creates a new wall from properties dictionary
    /// </summary>
    private Wall CreateWallFromProperties(Document doc, Dictionary<string, object> w)
    {
        try
        {
            // Logger.LogToFile($"Creating wall with properties: {string.Join(", ", w.Keys)}", "sync.log");

            // Extract coordinates (stored in meters)
            var x1 = Convert.ToDouble(w["x1"]);
            var y1 = Convert.ToDouble(w["y1"]);
            var z1 = Convert.ToDouble(w["z1"]);
            var x2 = Convert.ToDouble(w["x2"]);
            var y2 = Convert.ToDouble(w["y2"]);
            var z2 = Convert.ToDouble(w["z2"]);

            // Logger.LogToFile($"WALL GEOMETRY RAW: Start=({x1}, {y1}, {z1}), End=({x2}, {y2}, {z2}) (in meters)", "sync.log");

            // Convert from meters to Revit internal units (feet) with precision rounding
            var startPoint = new XYZ(ToFeetPreciseWithLogging(x1, "WallX1"), ToFeetPreciseWithLogging(y1, "WallY1"), ToFeetPreciseWithLogging(z1, "WallZ1"));
            var endPoint = new XYZ(ToFeetPreciseWithLogging(x2, "WallX2"), ToFeetPreciseWithLogging(y2, "WallY2"), ToFeetPreciseWithLogging(z2, "WallZ2"));
            var line = Line.CreateBound(startPoint, endPoint);

            // Logger.LogToFile($"WALL GEOMETRY CONVERTED: Start({startPoint.X:F10}, {startPoint.Y:F10}, {startPoint.Z:F10}) End({endPoint.X:F10}, {endPoint.Y:F10}, {endPoint.Z:F10}) (in feet)", "sync.log");

            // Find level by UID
            var baseLevelUid = w["baseLevelUid"].ToString();
            var level = FindLevelByUid(doc, baseLevelUid);
            if (level == null)
            {
                Logger.LogToFile($"Level with UID {baseLevelUid} not found, trying by levelId", "sync.log");

                // Fallback: try by levelId
                if (w.ContainsKey("levelId"))
                {
                    var levelId = Convert.ToInt64(w["levelId"]);
                    level = doc.GetElement(new ElementId(levelId)) as Level;
                }

                // Final fallback: find any level
                if (level == null)
                {
                    level = FindLevelByName(doc, "Level 1") ?? FindLevelByName(doc, "Ebene 1");
                    if (level == null)
                    {
                        level = new FilteredElementCollector(doc).OfClass(typeof(Level)).FirstElement() as Level;
                    }
                }
            }

            if (level == null)
            {
                Logger.LogToFile("No suitable level found, cannot create wall", "sync.log");
                return null;
            }

            Logger.LogToFile($"Using level: {level.Name} (Id: {level.Id})", "sync.log");

            // Find wall type - improved type matching
            WallType wallType = null;

            // Try by typeName and familyName first
            if (w.ContainsKey("typeName") && w.ContainsKey("familyName"))
            {
                var typeName = w["typeName"].ToString();
                var familyName = w["familyName"].ToString();

                wallType = new FilteredElementCollector(doc)
                    .OfClass(typeof(WallType))
                    .Cast<WallType>()
                    .FirstOrDefault(wt => wt.Name.Equals(typeName, StringComparison.OrdinalIgnoreCase) &&
                                         wt.FamilyName.Equals(familyName, StringComparison.OrdinalIgnoreCase));

                if (wallType != null)
                {
                    Logger.LogToFile($"Found wall type by name: {wallType.Name}", "sync.log");
                }
            }

            // Fallback: try by typeId
            if (wallType == null && w.ContainsKey("typeId"))
            {
                var typeId = Convert.ToInt64(w["typeId"]);
                wallType = doc.GetElement(new ElementId(typeId)) as WallType;

                if (wallType != null)
                {
                    Logger.LogToFile($"Found wall type by ID: {wallType.Name}", "sync.log");
                }
            }

            // Fallback: try by thickness with precise conversion
            if (wallType == null && (w.ContainsKey("thickness_m") || w.ContainsKey("thickness_mm")))
            {
                var thickness = w.ContainsKey("thickness_m") ?
                    ToFeetPreciseWithLogging(Convert.ToDouble(w["thickness_m"]), "WallThickness_m") :
                    ToFeetPreciseWithLogging(Convert.ToDouble(w["thickness_mm"]) / 1000.0, "WallThickness_mm"); // Convert mm to m first

                Logger.LogToFile($"WALL TYPE SEARCH: Looking for wall type with thickness {thickness:F10} feet", "sync.log");

                wallType = new FilteredElementCollector(doc)
                    .OfClass(typeof(WallType))
                    .Cast<WallType>()
                    .FirstOrDefault(wt => Math.Abs(wt.Width - thickness) < 0.001); // Reduced tolerance for precise matching

                if (wallType != null)
                {
                    Logger.LogToFile($"Found wall type by thickness: {wallType.Name}", "sync.log");
                }
            }

            // Final fallback: use first available wall type
            if (wallType == null)
            {
                wallType = new FilteredElementCollector(doc).OfClass(typeof(WallType)).FirstElement() as WallType;
                Logger.LogToFile($"Using default wall type: {wallType?.Name ?? "None"}", "sync.log");
            }

            if (wallType == null)
            {
                Logger.LogToFile("No valid wall type found, cannot create wall", "sync.log");
                return null;
            }

            // Get height with precise conversion (stored in meters or mm)
            var height = w.ContainsKey("height") ?
                ToFeetPreciseWithLogging(Convert.ToDouble(w["height"]), "WallHeight") :
                ToFeetPreciseWithLogging(Convert.ToDouble(w.GetValueOrDefault("height_mm", 3000.0)) / 1000.0, "WallHeightMm"); // Convert mm to m first

            Logger.LogToFile($"WALL HEIGHT: {height:F10} feet", "sync.log");

            // Get base offset (stored in mm) with precise conversion
            var baseOffsetMm = w.ContainsKey("base_offset_mm") ? Convert.ToDouble(w["base_offset_mm"]) : 0.0;
            var baseOffset = ToFeetPreciseWithLogging(baseOffsetMm / 1000.0, "WallBaseOffset"); // Convert mm to m first
            
            Logger.LogToFile($"WALL BASE OFFSET: {baseOffsetMm}mm = {baseOffset:F10}ft", "sync.log");

            // Get structural flag
            var structural = w.ContainsKey("structural") && Convert.ToBoolean(w["structural"]);

            // Get flipped flag  
            var flipped = w.ContainsKey("flipped") && Convert.ToBoolean(w["flipped"]);

            Logger.LogToFile($"Creating wall with: height={height:F3}ft, offset={baseOffset:F3}ft, structural={structural}, flipped={flipped}", "sync.log");

            // Create wall with correct parameters
            var wall = Wall.Create(doc, line, wallType.Id, level.Id, height, baseOffset, flipped, structural);

            // Apply additional properties after creation
            if (wall != null)
            {
                Logger.LogToFile($"Successfully created wall {wall.Id}", "sync.log");

                // KRITISCH: Markiere die Wand als NeoCollab-Element um Feedback-Loops zu vermeiden
                MarkWallWithRemoteId(wall, Convert.ToInt32(w["elementId"]));

                // KRITISCH: Setze IFC-GUID für Solibri-Synchronisation
                if (w.ContainsKey("uid"))
                {
                    var uid = w["uid"].ToString();
                    var ifcGuidParam = wall.get_Parameter(BuiltInParameter.IFC_GUID);
                    if (ifcGuidParam != null && !ifcGuidParam.IsReadOnly)
                    {
                        ifcGuidParam.Set(uid);
                        Logger.LogToFile($"Set wall IFC-GUID to {uid} for Solibri synchronization", "sync.log");
                    }
                }

                // Auto-join with nearby walls to prevent overlap conflicts
                try
                {
                    AutoJoinWallWithNearbyWalls(wall, doc);
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"Wall auto-join failed for wall {wall.Id}: {ex.Message}", "sync.log");
                }

                // Set location line
                if (w.ContainsKey("location_line"))
                {
                    var locationLine = Convert.ToInt32(w["location_line"]);
                    wall.get_Parameter(BuiltInParameter.WALL_KEY_REF_PARAM)?.Set(locationLine);
                    Logger.LogToFile($"Set wall location line to {locationLine}", "sync.log");
                }

                // Set room bounding property if available
                if (w.ContainsKey("roomBounding"))
                {
                    var roomBounding = Convert.ToBoolean(w["roomBounding"]);
                    var param = wall.get_Parameter(BuiltInParameter.WALL_ATTR_ROOM_BOUNDING);
                    if (param != null && !param.IsReadOnly)
                    {
                        param.Set(roomBounding ? 1 : 0);
                        Logger.LogToFile($"Set wall room bounding to {roomBounding}", "sync.log");
                    }
                }

                // Apply height constraints if available
                if (w.ContainsKey("topLevelUid") && w.ContainsKey("top_offset_m"))
                {
                    var topLevelUid = w["topLevelUid"].ToString();
                    var topLevel = FindLevelByUid(doc, topLevelUid);
                    if (topLevel != null)
                    {
                        var topOffset = ToFeet(Convert.ToDouble(w["top_offset_m"]));
                        var topConstraintParam = wall.get_Parameter(BuiltInParameter.WALL_HEIGHT_TYPE);
                        var topOffsetParam = wall.get_Parameter(BuiltInParameter.WALL_TOP_OFFSET);

                        if (topConstraintParam != null && !topConstraintParam.IsReadOnly)
                        {
                            topConstraintParam.Set(topLevel.Id);
                            Logger.LogToFile($"Set wall top constraint to level {topLevel.Name}", "sync.log");
                        }

                        if (topOffsetParam != null && !topOffsetParam.IsReadOnly)
                        {
                            topOffsetParam.Set(topOffset);
                            Logger.LogToFile($"Set wall top offset to {topOffset:F3}ft", "sync.log");
                        }
                    }
                }
            }

            return wall;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("CreateWallFromProperties failed", ex);
            return null;
        }
    }

    /// <summary>
    /// Updates wall geometry from properties
    /// </summary>
    private void UpdateWallGeometry(Wall wall, Dictionary<string, object> w)
    {
        try
        {
            var x1 = Convert.ToDouble(w["x1"]);
            var y1 = Convert.ToDouble(w["y1"]);
            var z1 = Convert.ToDouble(w["z1"]);
            var x2 = Convert.ToDouble(w["x2"]);
            var y2 = Convert.ToDouble(w["y2"]);
            var z2 = Convert.ToDouble(w["z2"]);

            Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - Neo4j coordinates: Start({x1:F3}, {y1:F3}, {z1:F3}) End({x2:F3}, {y2:F3}, {z2:F3}) (meters)", "sync.log");
           
            var startPoint = new XYZ(ToFeetPreciseWithLogging(x1, "WallUpdateX1"), ToFeetPreciseWithLogging(y1, "WallUpdateY1"), ToFeetPreciseWithLogging(z1, "WallUpdateZ1"));
            var endPoint = new XYZ(ToFeetPreciseWithLogging(x2, "WallUpdateX2"), ToFeetPreciseWithLogging(y2, "WallUpdateY2"), ToFeetPreciseWithLogging(z2, "WallUpdateZ2"));
            var newLine = Line.CreateBound(startPoint, endPoint);

            Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - Revit coordinates: Start({startPoint.X:F6}, {startPoint.Y:F6}, {startPoint.Z:F6}) End({endPoint.X:F6}, {endPoint.Y:F6}, {endPoint.Z:F6}) (feet)", "sync.log");

            // Get current geometry for comparison
            var locationCurve = wall.Location as LocationCurve;
            if (locationCurve != null)
            {
                var currentLine = locationCurve.Curve as Line;
                if (currentLine != null)
                {
                    Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - Current geometry: Start({currentLine.GetEndPoint(0).X:F6}, {currentLine.GetEndPoint(0).Y:F6}, {currentLine.GetEndPoint(0).Z:F6}) End({currentLine.GetEndPoint(1).X:F6}, {currentLine.GetEndPoint(1).Y:F6}, {currentLine.GetEndPoint(1).Z:F6})", "sync.log");
                }
                
                // Update location line
                locationCurve.Curve = newLine;
                Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - Successfully updated geometry", "sync.log");
            }
            else
            {
                Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - ERROR: No LocationCurve found", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"UPDATE WALL GEOMETRY: Wall {wall.Id} - FAILED: {ex.Message}", "sync.log");
            Logger.LogCrash("UpdateWallGeometry failed", ex);
        }
    }

    /// <summary>
    /// Updates wall properties from properties dictionary
    /// </summary>
    private void UpdateWallProperties(Wall wall, Dictionary<string, object> w)
    {
        try
        {
            Logger.LogToFile($"Updating properties for wall {wall.Id}", "sync.log");

            // Update height if provided with precise conversion
            if (w.ContainsKey("height"))
            {
                var heightParam = wall.get_Parameter(BuiltInParameter.WALL_USER_HEIGHT_PARAM);
                if (heightParam != null && !heightParam.IsReadOnly)
                {
                    var heightMeters = Convert.ToDouble(w["height"]);
                    var newHeight = ToFeetPreciseWithLogging(heightMeters, "WallUpdateHeight");
                    heightParam.Set(newHeight);
                    Logger.LogToFile($"WALL UPDATE HEIGHT: {heightMeters}m = {newHeight:F10}ft", "sync.log");
                }
            }

            // Update base offset if provided with precise conversion
            if (w.ContainsKey("base_offset_mm"))
            {
                var baseOffsetParam = wall.get_Parameter(BuiltInParameter.WALL_BASE_OFFSET);
                if (baseOffsetParam != null && !baseOffsetParam.IsReadOnly)
                {
                    var baseOffsetMm = Convert.ToDouble(w["base_offset_mm"]);
                    var newOffset = ToFeetPreciseWithLogging(baseOffsetMm / 1000.0, "WallUpdateBaseOffset");
                    baseOffsetParam.Set(newOffset);
                    Logger.LogToFile($"WALL UPDATE BASE OFFSET: {baseOffsetMm}mm = {newOffset:F10}ft", "sync.log");
                }
            }

            // Update location line if provided
            if (w.ContainsKey("location_line"))
            {
                var locationLineParam = wall.get_Parameter(BuiltInParameter.WALL_KEY_REF_PARAM);
                if (locationLineParam != null && !locationLineParam.IsReadOnly)
                {
                    var newLocationLine = Convert.ToInt32(w["location_line"]);
                    locationLineParam.Set(newLocationLine);
                    Logger.LogToFile($"Updated wall location line to {newLocationLine}", "sync.log");
                }
            }

            // Update flipped state if needed
            if (w.ContainsKey("flipped"))
            {
                var shouldBeFlipped = Convert.ToBoolean(w["flipped"]);
                if (wall.Flipped != shouldBeFlipped)
                {
                    wall.Flip();
                    Logger.LogToFile($"Flipped wall to match target state: {shouldBeFlipped}", "sync.log");
                }
            }

            // Update room bounding if provided
            if (w.ContainsKey("roomBounding"))
            {
                var roomBoundingParam = wall.get_Parameter(BuiltInParameter.WALL_ATTR_ROOM_BOUNDING);
                if (roomBoundingParam != null && !roomBoundingParam.IsReadOnly)
                {
                    var newRoomBounding = Convert.ToBoolean(w["roomBounding"]);
                    roomBoundingParam.Set(newRoomBounding ? 1 : 0);
                    Logger.LogToFile($"Updated wall room bounding to {newRoomBounding}", "sync.log");
                }
            }

            Logger.LogToFile($"Completed property updates for wall {wall.Id}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("UpdateWallProperties failed", ex);
        }
    }

    /// <summary>
    /// Helper method to find level by UID
    /// </summary>
    private Level FindLevelByUid(Document doc, string uid)
    {
        var levels = new FilteredElementCollector(doc).OfClass(typeof(Level)).Cast<Level>();
        foreach (var level in levels)
        {
            // Use UniqueId instead of non-existent LEVEL_UID
            if (level.UniqueId == uid)
                return level;
        }
        return null;
    }

    /// <summary>
    /// Helper method to find level by name
    /// </summary>
    private Level FindLevelByName(Document doc, string name)
    {
        var levels = new FilteredElementCollector(doc).OfClass(typeof(Level)).Cast<Level>();
        return levels.FirstOrDefault(l => l.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
    }

    private Wall FindLocalWallByRemoteId(Document doc, int remoteId)
    {
        var walls = new FilteredElementCollector(doc).OfClass(typeof(Wall)).Cast<Wall>();
        foreach (var w in walls)
        {
            var p = w.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            var s = p?.AsString();
            if (!string.IsNullOrEmpty(s) && s.Contains($"NeoCollab:ElementId={remoteId}"))
                return w;
        }
        return null;
    }

    /// <summary>
    /// Generic method to find any element by remote ElementId
    /// CRITICAL: For MODIFY operations, we must prioritize ORIGINAL elements over tagged copies
    /// to ensure modifications are applied to the correct elements across sessions
    /// </summary>
    private T FindElementByRemoteElementId<T>(Document doc, int remoteId) where T : Element
    {
        var elements = new FilteredElementCollector(doc).OfClass(typeof(T)).Cast<T>();
        var searchTag = $"NeoCollab:ElementId={remoteId}";
        
        Logger.LogToFile($"FIND ELEMENT: Searching for element with remoteId={remoteId} among {elements.Count()} elements", "sync.log");
        
        // MODIFY FIX: For cross-session synchronization, we need to find the tagged copy
        // because the original ElementId doesn't exist in the target document
        // First: Search by NeoCollab tags (pulled elements)
        int elementsChecked = 0;
        foreach (var element in elements)
        {
            elementsChecked++;
            var p = element.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            var s = p?.AsString();
            
            if (!string.IsNullOrEmpty(s) && s.Contains(searchTag))
            {
                // Extract the ElementId number after "NeoCollab:ElementId=" and before delimiter
                var tagStart = "NeoCollab:ElementId=";
                var tagIndex = s.IndexOf(tagStart);
                if (tagIndex >= 0)
                {
                    var numberStart = tagIndex + tagStart.Length;
                    var numberEnd = numberStart;
                    
                    // Find end of number (at delimiter or end of string)
                    while (numberEnd < s.Length && char.IsDigit(s[numberEnd]))
                    {
                        numberEnd++;
                    }
                    
                    if (numberEnd > numberStart)
                    {
                        var extractedIdStr = s.Substring(numberStart, numberEnd - numberStart);
                        if (int.TryParse(extractedIdStr, out int extractedId) && extractedId == remoteId)
                        {
                            Logger.LogToFile($"FIND ELEMENT: FOUND TAGGED ELEMENT - Element {element.Id} with tag '{s}' (extracted ID: {extractedId})", "sync.log");
                            return element;
                        }
                    }
                }
            }
        }
        
        // Fallback: Check for direct ElementId match (same-document scenario)
        foreach (var element in elements)
        {
            if (element.Id.Value == remoteId)
            {
                Logger.LogToFile($"FIND ELEMENT: FOUND DIRECT MATCH - Element {element.Id} matches remoteId {remoteId} (same document)", "sync.log");
                return element;
            }
        }
        
        Logger.LogToFile($"FIND ELEMENT: NOT FOUND - No element found for remoteId={remoteId} after checking {elementsChecked} elements", "sync.log");
        return null;
    }

    private string GetStringProperty(Dictionary<string, object> properties, params string[] keys)
    {
        if (properties == null || keys == null)
        {
            return string.Empty;
        }

        foreach (var key in keys)
        {
            if (!string.IsNullOrEmpty(key) && properties.TryGetValue(key, out var value) && value != null)
            {
                var str = value.ToString();
                if (!string.IsNullOrWhiteSpace(str))
                {
                    return str;
                }
            }
        }

        return string.Empty;
    }

    private T FindElementByUid<T>(Document doc, string uid) where T : Element
    {
        if (string.IsNullOrEmpty(uid))
        {
            return null;
        }

        try
        {
            var elements = new FilteredElementCollector(doc).OfClass(typeof(T)).Cast<T>();
            foreach (var element in elements)
            {
                try
                {
                    var neoUid = ParameterUtils.GetNeo4jUid(element);
                    if (!string.IsNullOrEmpty(neoUid) && neoUid.Equals(uid, StringComparison.OrdinalIgnoreCase))
                    {
                        return element;
                    }

                    if (!string.IsNullOrEmpty(element.UniqueId) && element.UniqueId.Equals(uid, StringComparison.OrdinalIgnoreCase))
                    {
                        return element;
                    }

                    var comment = element.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    if (!string.IsNullOrEmpty(comment) && comment.IndexOf(uid, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        return element;
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"FIND ELEMENT BY UID WARNING: {ex.Message}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"FIND ELEMENT BY UID ERROR: {ex.Message}", "sync.log");
        }

        return null;
    }

    private T FindElementByIfcGuid<T>(Document doc, string guid) where T : Element
    {
        if (string.IsNullOrEmpty(guid))
        {
            return null;
        }

        try
        {
            var elements = new FilteredElementCollector(doc).OfClass(typeof(T)).Cast<T>();
            foreach (var element in elements)
            {
                try
                {
                    var guidParam = element.get_Parameter(BuiltInParameter.IFC_GUID);
                    var guidValue = guidParam?.AsString();
                    if (!string.IsNullOrEmpty(guidValue) && guidValue.Equals(guid, StringComparison.OrdinalIgnoreCase))
                    {
                        return element;
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"FIND ELEMENT BY GUID WARNING: {ex.Message}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"FIND ELEMENT BY GUID ERROR: {ex.Message}", "sync.log");
        }

        return null;
    }

    /// <summary>
    /// Automatically joins a wall with nearby walls to prevent overlap conflicts
    /// </summary>
    private void AutoJoinWallWithNearbyWalls(Wall wall, Document doc)
    {
        try
        {
            // Logger.LogToFile($"AUTO-JOIN: Starting auto-join for wall {wall.Id}", "sync.log");
            
            // Get wall curve for intersection detection
            LocationCurve wallLocation = wall.Location as LocationCurve;
            if (wallLocation?.Curve == null)
            {
                // Logger.LogToFile($"AUTO-JOIN: Wall {wall.Id} has no location curve", "sync.log");
                return;
            }

            Line wallLine = wallLocation.Curve as Line;
            if (wallLine == null)
            {
                // Logger.LogToFile($"AUTO-JOIN: Wall {wall.Id} is not a straight wall", "sync.log");
                return;
            }

            // Find nearby walls
            var nearbyWalls = new FilteredElementCollector(doc)
                .OfClass(typeof(Wall))
                .Cast<Wall>()
                .Where(w => w.Id != wall.Id) // Exclude the wall itself
                .ToList();

            int joinCount = 0;
            foreach (Wall nearbyWall in nearbyWalls)
            {
                try
                {
                    // Check if walls are on the same level
                    if (wall.LevelId != nearbyWall.LevelId)
                        continue;

                    LocationCurve nearbyLocation = nearbyWall.Location as LocationCurve;
                    if (nearbyLocation?.Curve == null)
                        continue;

                    // Check for geometric intersection or close proximity
                    XYZ wallStart = wallLine.GetEndPoint(0);
                    XYZ wallEnd = wallLine.GetEndPoint(1);
                    
                    Line nearbyLine = nearbyLocation.Curve as Line;
                    if (nearbyLine == null)
                        continue;

                    XYZ nearbyStart = nearbyLine.GetEndPoint(0);
                    XYZ nearbyEnd = nearbyLine.GetEndPoint(1);

                    // Check if endpoints are close (within 1 foot tolerance)
                    double tolerance = 1.0; // 1 foot
                    bool endpointsClose = 
                        wallStart.DistanceTo(nearbyStart) < tolerance ||
                        wallStart.DistanceTo(nearbyEnd) < tolerance ||
                        wallEnd.DistanceTo(nearbyStart) < tolerance ||
                        wallEnd.DistanceTo(nearbyEnd) < tolerance;

                    if (endpointsClose)
                    {
                        // Try to join the walls
                        if (!JoinGeometryUtils.AreElementsJoined(doc, wall, nearbyWall))
                        {
                            JoinGeometryUtils.JoinGeometry(doc, wall, nearbyWall);
                            joinCount++;
                            Logger.LogToFile($"AUTO-JOIN: Successfully joined wall {wall.Id} with wall {nearbyWall.Id}", "sync.log");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"AUTO-JOIN: Failed to join wall {wall.Id} with {nearbyWall.Id}: {ex.Message}", "sync.log");
                }
            }

            Logger.LogToFile($"AUTO-JOIN: Completed for wall {wall.Id}, joined with {joinCount} walls", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"AUTO-JOIN: General error for wall {wall.Id}: {ex.Message}", "sync.log");
        }
    }

    private void MarkWallWithRemoteId(Wall wall, int remoteId)
    {
        try
        {
            var p = wall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            if (p != null && !p.IsReadOnly)
            {
                var current = p.AsString();
                var tag = $"NeoCollab:ElementId={remoteId}";
                
                // Zusätzlich: Pull-Marker hinzufügen
                var pullMarker = $"NeoCollab:PulledFrom={CommandManager.Instance.SessionId}";
                
                if (string.IsNullOrEmpty(current))
                {
                    p.Set($"{tag}; {pullMarker}");
                }
                else if (!current.Contains(tag))
                {
                    p.Set($"{current}; {tag}; {pullMarker}");
                }
                
                Logger.LogToFile($"WALL MARKING: Marked wall {wall.Id} with remoteId {remoteId} and pull marker", "sync.log");
            }
            else
            {
                Logger.LogToFile($"WARNING: Could not mark wall {wall.Id} - Comments parameter is not available or read-only", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"ERROR: Failed to mark wall {wall.Id} with remoteId {remoteId} - {ex.Message}", "sync.log");
        }
    }


    private void DeleteWallByRemoteElementId(Document doc, int remoteId)
    {
        var wall = FindLocalWallByRemoteId(doc, remoteId);
        if (wall != null)
        {
            // Get wall GUID for Solibri deletion before deleting from Revit
            var wallGuidParam = wall.get_Parameter(BuiltInParameter.IFC_GUID);
            var wallGuid = wallGuidParam?.AsString();
            
            doc.Delete(wall.Id);
            Logger.LogToFile($"Deleted wall for remote ElementId {remoteId}", "sync.log");
            
            // Delete from Solibri if we have a GUID
            if (!string.IsNullOrEmpty(wallGuid))
            {
                try
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Deleting wall component from Solibri with GUID {wallGuid}", "sync.log");
                    
                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(new List<string> { wallGuid });
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Successfully deleted wall component from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Failed to delete wall component from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Error initiating Solibri wall deletion: {ex.Message}", "sync.log");
                }
            }
        }
    }

    /// <summary>
    /// Deletes a wall based on geographic properties (coordinates, thickness, height) instead of ElementId
    /// This method finds walls at the same location and deletes them, handling cases where ElementIds differ between sessions
    /// </summary>
    private void DeleteWallByGeographicProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
            Logger.LogToFile($"DELETE BY GEOGRAPHY: Starting geographic deletion for wall with remoteElementId={remoteElementId}", "sync.log");

            // Validate that we have the minimum required geographic data
            var requiredGeometryFields = new[] { "x1", "y1", "z1", "x2", "y2", "z2" };
            var missingGeometryFields = requiredGeometryFields.Where(field => !properties.ContainsKey(field)).ToList();
            if (missingGeometryFields.Any())
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: Missing required geometry fields: {string.Join(", ", missingGeometryFields)}, cannot delete geographically", "sync.log");
                return;
            }

            // Find all walls that match the geographic properties
            var matchingWalls = FindWallsByGeographicProperties(doc, properties, remoteElementId);
            
            if (matchingWalls.Count == 0)
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: No walls found at the specified geographic location for remoteElementId={remoteElementId}", "sync.log");
                return;
            }

            // Delete all matching walls
            var deletedGuids = new List<string>();
            foreach (var wall in matchingWalls)
            {
                try
                {
                    var wallTag = wall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    
                    // Get wall GUID for Solibri deletion
                    var wallGuidParam = wall.get_Parameter(BuiltInParameter.IFC_GUID);
                    var wallGuid = wallGuidParam?.AsString();
                    if (!string.IsNullOrEmpty(wallGuid))
                    {
                        deletedGuids.Add(wallGuid);
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Wall {wall.Id} has GUID {wallGuid} for Solibri deletion", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Wall {wall.Id} has no GUID, cannot delete from Solibri", "sync.log");
                    }
                    
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting wall {wall.Id} with tag '{wallTag ?? "no tag"}'", "sync.log");
                    
                    doc.Delete(wall.Id);
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted wall {wall.Id} based on geographic match", "sync.log");
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete wall {wall.Id}: {ex.Message}", "sync.log");
                }
            }

            // Delete components from Solibri if we have GUIDs
            if (deletedGuids.Any())
            {
                try
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting {deletedGuids.Count} components from Solibri: [{string.Join(", ", deletedGuids)}]", "sync.log");
                    
                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(deletedGuids);
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted {deletedGuids.Count} components from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete components from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Error initiating Solibri deletion: {ex.Message}", "sync.log");
                }
            }

            Logger.LogToFile($"DELETE BY GEOGRAPHY: Completed geographic deletion, deleted {matchingWalls.Count} walls for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("DeleteWallByGeographicProperties failed", ex);
        }
    }

    /// <summary>
    /// Finds walls that match the geographic properties (location, thickness, height)
    /// Excludes the original wall with the same ElementId to prevent self-deletion
    /// </summary>
    private List<Wall> FindWallsByGeographicProperties(Document doc, Dictionary<string, object> properties, int remoteElementId)
    {
        var matchingWalls = new List<Wall>();
        
        try
        {
            Logger.LogToFile($"FIND BY GEOGRAPHY: Searching for walls matching geographic properties (excluding original ElementId {remoteElementId})", "sync.log");
            
            var walls = new FilteredElementCollector(doc).OfClass(typeof(Wall)).Cast<Wall>();
            int checkedCount = 0;
            
            foreach (var wall in walls)
            {
                checkedCount++;
                
                // CRITICAL: Skip the original wall with the same ElementId to prevent self-deletion
                if (wall.Id.Value == remoteElementId)
                {
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Skipping original wall {wall.Id} (same ElementId as remoteElementId)", "sync.log");
                    continue;
                }
                
                // Check if this wall matches the geographic properties
                if (IsWallGeographicallyIdentical(wall, properties))
                {
                    var wallTag = wall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Found matching wall {wall.Id} with tag '{wallTag ?? "no tag"}'", "sync.log");
                    matchingWalls.Add(wall);
                }
            }
            
            Logger.LogToFile($"FIND BY GEOGRAPHY: Checked {checkedCount} walls, found {matchingWalls.Count} matches for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("FindWallsByGeographicProperties failed", ex);
        }
        
        return matchingWalls;
    }

    /// <summary>
    /// Checks if a wall is geographically identical to Neo4j properties
    /// Uses the same logic as IsWallIdentical but focuses on geographic matching for deletion
    /// </summary>
    private bool IsWallGeographicallyIdentical(Wall wall, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance for coordinates
            
            // Get existing wall location curve
            var locationCurve = wall.Location as LocationCurve;
            if (locationCurve?.Curve == null)
            {
                return false;
            }
            
            var line = locationCurve.Curve as Line;
            if (line == null)
            {
                return false;
            }
            
            var existingStart = line.GetEndPoint(0);
            var existingEnd = line.GetEndPoint(1);
            
            // Convert Neo4j coordinates to feet
            double neoX1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x1", 0.0)), "DeleteGeographyX1");
            double neoY1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y1", 0.0)), "DeleteGeographyY1");
            double neoZ1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z1", 0.0)), "DeleteGeographyZ1");
            double neoX2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x2", 0.0)), "DeleteGeographyX2");
            double neoY2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y2", 0.0)), "DeleteGeographyY2");
            double neoZ2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z2", 0.0)), "DeleteGeographyZ2");
            
            // Check if geometry matches (either direction)
            bool geometryMatch1 = Math.Abs(existingStart.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ1) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ2) < TOLERANCE;
            
            bool geometryMatch2 = Math.Abs(existingStart.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ2) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ1) < TOLERANCE;
            
            bool geometryMatches = geometryMatch1 || geometryMatch2;
            
            if (!geometryMatches)
            {
                return false;
            }
            
            // Check wall type/thickness if available for more precise matching
            if (properties.ContainsKey("thickness_m"))
            {
                double neoThickness = ToFeetPreciseWithLogging(Convert.ToDouble(properties["thickness_m"]), "DeleteGeographyThickness");
                double existingThickness = wall.Width;
                
                if (Math.Abs(existingThickness - neoThickness) >= TOLERANCE)
                {
                    return false;
                }
            }
            
            // Check height if available for more precise matching
            if (properties.ContainsKey("height") || properties.ContainsKey("height_mm"))
            {
                var heightParam = wall.get_Parameter(BuiltInParameter.WALL_USER_HEIGHT_PARAM);
                if (heightParam != null)
                {
                    double existingHeight = heightParam.AsDouble();
                    double neoHeight = properties.ContainsKey("height") ? 
                        ToFeetPreciseWithLogging(Convert.ToDouble(properties["height"]), "DeleteGeographyHeight") :
                        ToFeetPreciseWithLogging(Convert.ToDouble(properties["height_mm"]) / 1000.0, "DeleteGeographyHeightMm");
                    
                    if (Math.Abs(existingHeight - neoHeight) >= TOLERANCE)
                    {
                        return false;
                    }
                }
            }
            
            Logger.LogToFile($"DELETE GEOGRAPHY: Wall {wall.Id} matches geographic properties", "sync.log");
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsWallGeographicallyIdentical failed", ex);
            return false;
        }
    }

    private static double ToFeet(double meters) =>
        UnitUtils.ConvertToInternalUnits(meters, UnitTypeId.Meters);

    /// <summary>
    /// Converts meters to feet with precision rounding and detailed logging
    /// </summary>
    private static double ToFeetPreciseWithLogging(double meters, string context)
    {
        double feetRaw = UnitUtils.ConvertToInternalUnits(meters, UnitTypeId.Meters);
        double feetRounded = Math.Round(feetRaw, 10);

        if (Math.Abs(feetRaw - feetRounded) > 1e-12) // Log only if rounding actually changed something
        {
            // Removed verbose precision logging - coordinates work correctly
            // Logger.LogToFile($"PRECISION FIX {context}: {meters}m → {feetRaw}ft → {feetRounded}ft (rounded)", "sync.log");
        }

        return feetRounded;
    }
    // LEGACY METHOD - Redirects to ChangeLog-based synchronization
    public void PullRemoteChanges(Document doc, string currentUserId)
    {
        var startTime = DateTime.Now;
        Logger.LogToFile($"PULL REMOTE STARTED: PullRemoteChanges called for document '{doc.Title}' with user {currentUserId} at {startTime:yyyy-MM-dd HH:mm:ss.fff}", "sync.log");

        // KRITISCH: Pull-Modus aktivieren für direkte PullRemoteChanges Aufrufe
        bool wasPullInProgress = CommandManager.Instance.IsPullInProgress;
        if (!wasPullInProgress)
        {
            CommandManager.Instance.IsPullInProgress = true;
        }

        // For MVP, redirect to ChangeLog-based wall synchronization only
        try
        {
            Logger.LogToFile("PULL REMOTE TRANSACTION: Starting Revit transaction for ChangeLog pull", "sync.log");
            using (var tx = new Transaction(doc, "NeoCollab ChangeLog Pull"))
            {
                tx.Start();
                Logger.LogToFile("PULL REMOTE APPLYING: Calling ApplyPendingWallChanges within transaction", "sync.log");
                ApplyPendingWallChanges(doc, currentUserId);
                tx.Commit();
                var duration = DateTime.Now - startTime;
                Logger.LogToFile($"PULL REMOTE COMPLETED: ChangeLog-based pull completed successfully in {duration.TotalMilliseconds:F0}ms", "sync.log");
            }
        }
        catch (Exception ex)
        {
            var duration = DateTime.Now - startTime;
            Logger.LogToFile($"PULL REMOTE FAILED: PullRemoteChanges failed after {duration.TotalMilliseconds:F0}ms - {ex.Message}", "sync.log");
            Logger.LogCrash("PullRemoteChanges fallback failed", ex);
            throw;
        }
        finally
        {
            // KRITISCH: Pull-Modus nur deaktivieren wenn wir ihn aktiviert haben
            if (!wasPullInProgress)
            {
                CommandManager.Instance.IsPullInProgress = false;
                Logger.LogToFile("PULL REMOTE: Pull mode deactivated after direct PullRemoteChanges call", "sync.log");
            }
        }
    }

    /// <summary>
    /// Determines the element type from Neo4j properties
    /// </summary>
    private string DetermineElementType(Dictionary<string, object> properties)
    {
        if (properties.TryGetValue("elementType", out var elementTypeObj))
        {
            var elementType = elementTypeObj?.ToString();
            if (!string.IsNullOrEmpty(elementType) && elementType != "Unknown")
            {
                return elementType;
            }
        }

        if (properties.TryGetValue("__changeLogElementType__", out var changeLogTypeObj))
        {
            var changeLogType = changeLogTypeObj?.ToString();
            if (!string.IsNullOrEmpty(changeLogType) && changeLogType != "Unknown")
            {
                return changeLogType;
            }
        }

        // Check for explicit type from Neo4j query first
        if (properties.ContainsKey("__element_type__"))
        {
            var elementType = properties["__element_type__"].ToString();
            if (!string.IsNullOrEmpty(elementType) && elementType != "Unknown")
            {
                return elementType;
            }
        }
        
        // Check for explicit type indicators
        if (properties.ContainsKey("rvtClass"))
        {
            return properties["rvtClass"].ToString();
        }
        
        // Infer from presence of specific properties
        if (properties.ContainsKey("x1") && properties.ContainsKey("y1") && properties.ContainsKey("z1") && 
            properties.ContainsKey("x2") && properties.ContainsKey("y2") && properties.ContainsKey("z2"))
        {
            if (properties.ContainsKey("thickness_m") || properties.ContainsKey("thickness_mm"))
                return "Wall";
            if (properties.ContainsKey("diameter"))
                return "Pipe";
        }
        
        if (properties.ContainsKey("hostId") || properties.ContainsKey("hostUid"))
        {
            if (properties.ContainsKey("width") && properties.ContainsKey("height"))
                return "Door";
        }
        
        if (properties.ContainsKey("guid") || properties.ContainsKey("familyName"))
            return "ProvisionalSpace";
            
        // Default fallback
        Logger.LogToFile($"WARNING: Could not determine element type from properties, defaulting to Wall", "sync.log");
        return "Wall";
    }

    /// <summary>
    /// Apply element update for MODIFY operations (update-only, no creation)
    /// Unlike ApplyElementUpsert, this method only updates existing elements and returns false if element doesn't exist
    /// </summary>
    private bool ApplyElementUpdate(Document doc, Dictionary<string, object> properties, string elementType, out long localElementId)
    {
        localElementId = -1;
        Logger.LogToFile($"APPLY ELEMENT UPDATE: Starting update-only operation for {elementType}", "sync.log");
        
        try
        {
            var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
            if (remoteElementId == -1)
            {
                Logger.LogToFile($"APPLY ELEMENT UPDATE: Invalid ElementId for {elementType}, cannot update", "sync.log");
                return false;
            }

            switch (elementType)
            {
                case "Wall":
                    return UpdateWallFromGraphProperties(doc, properties, out localElementId);
                case "Door":
                    // For non-wall elements, just call original method and don't track elementId yet
                    localElementId = -1;
                    return UpdateDoorFromGraphProperties(doc, properties);
                case "Pipe":
                    localElementId = -1;
                    return UpdatePipeFromGraphProperties(doc, properties);
                case "ProvisionalSpace":
                    localElementId = -1;
                    return UpdateProvisionalSpaceFromGraphProperties(doc, properties);
                default:
                    Logger.LogToFile($"APPLY ELEMENT UPDATE: Unknown element type {elementType}, treating as Wall", "sync.log");
                    return UpdateWallFromGraphProperties(doc, properties, out localElementId);
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"APPLY ELEMENT UPDATE: Failed to update {elementType}: {ex.Message}", "sync.log");
            Logger.LogCrash("ApplyElementUpdate failed", ex);
            return false;
        }
    }

    /// <summary>
    /// Update-only method for walls (used by Modify operations)
    /// Returns true if wall was found and updated, false if wall doesn't exist
    /// </summary>
    private bool UpdateWallFromGraphProperties(Document doc, Dictionary<string, object> w, out long localElementId)
    {
        localElementId = -1;
        try
        {
            var remoteElementId = Convert.ToInt32(w["elementId"]);
            Logger.LogToFile($"UPDATE WALL: Starting MODIFY operation for remoteElementId={remoteElementId}", "sync.log");

            // Find existing wall by remote tag OR original ElementId
            var existingWall = FindElementByRemoteElementId<Wall>(doc, remoteElementId);

            if (existingWall != null)
            {
                // Log which type of element was found
                var tag = existingWall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                bool isOriginal = existingWall.Id.Value == remoteElementId;
                string elementType = isOriginal ? "ORIGINAL" : "TAGGED COPY";
                
                Logger.LogToFile($"UPDATE WALL: Found {elementType} wall {existingWall.Id} for remote ElementId {remoteElementId}", "sync.log");
                Logger.LogToFile($"UPDATE WALL: Element tag: '{tag ?? "no tag"}'", "sync.log");
                
                // CRITICAL FIX: For MODIFY operations, ALWAYS update without identity check
                // The IsWallIdentical check is for CREATE operations (duplicate prevention),
                // but MODIFY operations should always apply the changes from Neo4j
                Logger.LogToFile($"UPDATE WALL: MODIFY operation - applying update without identity check to {elementType}", "sync.log");
                
                // Update existing wall geometry and properties
                UpdateWallGeometry(existingWall, w);
                UpdateWallProperties(existingWall, w);
                
                localElementId = existingWall.Id.Value; // Return local ElementId for Solibri
                Logger.LogToFile($"UPDATE WALL: Successfully updated {elementType} wall {existingWall.Id} from MODIFY operation", "sync.log");
                return true;
            }
            else
            {
                Logger.LogToFile($"UPDATE WALL: Wall not found for remoteElementId={remoteElementId} - cannot update (modify-only)", "sync.log");
                return false; // Element doesn't exist, cannot update
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash("UpdateWallFromGraphProperties failed", ex);
            return false;
        }
    }

    /// <summary>
    /// Update-only method for doors (used by Modify operations)
    /// Returns true if door was found and updated, false if door doesn't exist
    /// </summary>
    private bool UpdateDoorFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"UPDATE DOOR: Looking for existing door with remoteElementId={remoteElementId}", "sync.log");

            // Find existing door by NeoCollab tag
            FamilyInstance existingDoor = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);

            if (existingDoor != null)
            {
                Logger.LogToFile($"UPDATE DOOR: Found existing door {existingDoor.Id} for remote ElementId {remoteElementId}", "sync.log");
                UpdateDoorFromGraphProperties(existingDoor, properties);
                Logger.LogToFile($"UPDATE DOOR: Successfully updated door {existingDoor.Id}", "sync.log");
                return true;
            }
            else
            {
                Logger.LogToFile($"UPDATE DOOR: Door not found for remoteElementId={remoteElementId} - cannot update (modify-only)", "sync.log");
                return false; // Element doesn't exist, cannot update
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"UpdateDoorFromGraphProperties failed: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// Update-only method for pipes (used by Modify operations)
    /// Returns true if pipe was found and updated, false if pipe doesn't exist
    /// </summary>
    private bool UpdatePipeFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"UPDATE PIPE: Looking for existing pipe with remoteElementId={remoteElementId}", "sync.log");

            // Find existing pipe by NeoCollab tag
            var existingPipe = FindElementByRemoteElementId<Pipe>(doc, remoteElementId);

            if (existingPipe != null)
            {
                Logger.LogToFile($"UPDATE PIPE: Found existing pipe {existingPipe.Id} for remote ElementId {remoteElementId}", "sync.log");
                // Call existing UpdatePipeFromGraphProperties method with correct signature
                UpdatePipeFromGraphProperties(existingPipe, properties, doc);
                Logger.LogToFile($"UPDATE PIPE: Successfully updated pipe {existingPipe.Id}", "sync.log");
                return true;
            }
            else
            {
                Logger.LogToFile($"UPDATE PIPE: Pipe not found for remoteElementId={remoteElementId} - cannot update (modify-only)", "sync.log");
                return false; // Element doesn't exist, cannot update
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"UpdatePipeFromGraphProperties failed: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// Update-only method for provisional spaces (used by Modify operations)
    /// Returns true if space was found and updated, false if space doesn't exist
    /// </summary>
    private bool UpdateProvisionalSpaceFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"UPDATE PROVISIONAL SPACE: Looking for existing space with remoteElementId={remoteElementId}", "sync.log");

            // Find existing provisional space by NeoCollab tag
            var existingSpace = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);

            if (existingSpace != null)
            {
                Logger.LogToFile($"UPDATE PROVISIONAL SPACE: Found existing space {existingSpace.Id} for remote ElementId {remoteElementId}", "sync.log");
                // Call existing UpdateProvisionalSpaceFromGraphProperties method with correct signature
                UpdateProvisionalSpaceFromGraphProperties(existingSpace, properties, doc);
                Logger.LogToFile($"UPDATE PROVISIONAL SPACE: Successfully updated space {existingSpace.Id}", "sync.log");
                return true;
            }
            else
            {
                Logger.LogToFile($"UPDATE PROVISIONAL SPACE: Space not found for remoteElementId={remoteElementId} - cannot update (modify-only)", "sync.log");
                return false; // Element doesn't exist, cannot update
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"UpdateProvisionalSpaceFromGraphProperties failed: {ex.Message}", ex);
            return false;
        }
    }

    /// <summary>
    /// Apply element upsert based on type
    /// </summary>
    private void ApplyElementUpsert(Document doc, Dictionary<string, object> properties, string elementType)
    {
        switch (elementType)
        {
            case "Wall":
                UpsertWallFromGraphProperties(doc, properties);
                break;
            case "Door":
                UpsertDoorFromGraphProperties(doc, properties);
                break;
            case "Pipe":
                UpsertPipeFromGraphProperties(doc, properties);
                break;
            case "ProvisionalSpace":
                UpsertProvisionalSpaceFromGraphProperties(doc, properties);
                break;
            default:
                Logger.LogToFile($"WARNING: Unknown element type {elementType}, treating as Wall", "sync.log");
                UpsertWallFromGraphProperties(doc, properties);
                break;
        }
    }

    /// <summary>
    /// Delete element by remote ElementId based on type
    /// </summary>
    private void DeleteElementByRemoteElementId(Document doc, int remoteElementId, string elementType, Dictionary<string, object> properties)
    {
        switch (elementType)
        {
            case "Wall":
                DeleteWallByRemoteElementId(doc, remoteElementId);
                break;
            case "Door":
                DeleteDoorByRemoteElementId(doc, remoteElementId);
                break;
            case "Pipe":
                DeletePipeByRemoteElementId(doc, remoteElementId, properties);
                break;
            case "ProvisionalSpace":
                DeleteProvisionalSpaceByRemoteElementId(doc, remoteElementId, properties);
                break;
            default:
                Logger.LogToFile($"WARNING: Unknown element type {elementType} for deletion", "sync.log");
                break;
        }
    }

    /// <summary>
    /// Delete element by geographic properties based on type - NEW GEOGRAPHIC DELETION METHOD
    /// Uses geographic data (coordinates, dimensions) instead of ElementId for more reliable cross-session deletion
    /// </summary>
    private void DeleteElementByGeographicProperties(Document doc, Dictionary<string, object> properties, string elementType)
    {
        var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
        Logger.LogToFile($"GEOGRAPHIC DELETE: Starting geographic deletion for {elementType} with remoteElementId={remoteElementId}", "sync.log");
        
        switch (elementType)
        {
            case "Wall":
                DeleteWallByGeographicProperties(doc, properties);
                break;
            case "Door":
                DeleteDoorByGeographicProperties(doc, properties);
                break;
            case "Pipe":
                DeletePipeByGeographicProperties(doc, properties);
                break;
            case "ProvisionalSpace":
                DeleteProvisionalSpaceByGeographicProperties(doc, properties);
                break;
            default:
                Logger.LogToFile($"WARNING: Unknown element type {elementType} for geographic deletion", "sync.log");
                break;
        }
        
        Logger.LogToFile($"GEOGRAPHIC DELETE: Completed geographic deletion for {elementType} with remoteElementId={remoteElementId}", "sync.log");
    }

    /// <summary>
    /// Checks if the properties contain the required geographic data for reliable deletion
    /// </summary>
    private bool HasRequiredGeographicProperties(Dictionary<string, object> properties, string elementType)
    {
        if (properties == null || properties.Count == 0)
            return false;

        switch (elementType)
        {
            case "Wall":
                // Walls need line geometry
                return properties.ContainsKey("x1") && properties.ContainsKey("y1") && properties.ContainsKey("z1") &&
                       properties.ContainsKey("x2") && properties.ContainsKey("y2") && properties.ContainsKey("z2");
            
            case "Door":
                // Doors need host information
                return properties.ContainsKey("hostUid") || properties.ContainsKey("hostId");
            
            case "Pipe":
                // Pipes need line geometry
                return properties.ContainsKey("x1") && properties.ContainsKey("y1") && properties.ContainsKey("z1") &&
                       properties.ContainsKey("x2") && properties.ContainsKey("y2") && properties.ContainsKey("z2");
            
            case "ProvisionalSpace":
                // Provisional spaces need GUID or position
                return properties.ContainsKey("guid") || 
                       (properties.ContainsKey("x") && properties.ContainsKey("y") && properties.ContainsKey("z"));
            
            default:
                return false;
        }
    }

    /// <summary>
    /// Loads full element properties from Neo4j for geographic deletion
    /// </summary>
    private Dictionary<string, object> LoadElementPropertiesFromNeo4j(int elementId, string elementType)
    {
        try
        {
            Logger.LogToFile($"LOAD PROPERTIES: Loading full properties for {elementType} {elementId} from Neo4j", "sync.log");
            
            // Query Neo4j for the full element properties
            string cypher = "";
            switch (elementType)
            {
                case "Wall":
                    cypher = @"
                        MATCH (w:Wall {elementId: $elementId})
                        RETURN w.elementId as elementId, w.x1 as x1, w.y1 as y1, w.z1 as z1, 
                               w.x2 as x2, w.y2 as y2, w.z2 as z2, w.thickness_m as thickness_m,
                               w.height as height, w.height_mm as height_mm, w.baseLevelUid as baseLevelUid,
                               w.typeName as typeName, w.familyName as familyName";
                    break;
                    
                case "Door":
                    cypher = @"
                        MATCH (d:Door {elementId: $elementId})
                        RETURN d.elementId as elementId, d.hostUid as hostUid, d.hostId as hostId,
                               d.x as x, d.y as y, d.z as z, d.width as width, d.height as height";
                    break;
                    
                case "Pipe":
                    cypher = @"
                        MATCH (p:Pipe {elementId: $elementId})
                        RETURN p.elementId as elementId, p.x1 as x1, p.y1 as y1, p.z1 as z1,
                               p.x2 as x2, p.y2 as y2, p.z2 as z2, p.diameter as diameter";
                    break;
                    
                case "ProvisionalSpace":
                    cypher = @"
                        MATCH (s:ProvisionalSpace {elementId: $elementId})
                        RETURN s.elementId as elementId, s.guid as guid, s.x as x, s.y as y, s.z as z,
                               s.familyName as familyName";
                    break;
                    
                default:
                    Logger.LogToFile($"LOAD PROPERTIES: Unknown element type {elementType}", "sync.log");
                    return null;
            }

            var records = _connector.RunReadQueryAsync(cypher, new { elementId = elementId }).GetAwaiter().GetResult();
            var record = records.FirstOrDefault();
            
            if (record != null)
            {
                var properties = new Dictionary<string, object>();
                foreach (var key in record.Keys)
                {
                    properties[key] = record[key].As<object>();
                }
                
                Logger.LogToFile($"LOAD PROPERTIES: Successfully loaded {properties.Count} properties for {elementType} {elementId}", "sync.log");
                return properties;
            }
            else
            {
                Logger.LogToFile($"LOAD PROPERTIES: No properties found for {elementType} {elementId} in Neo4j", "sync.log");
                return null;
            }
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"LOAD PROPERTIES: Failed to load properties for {elementType} {elementId}: {ex.Message}", "sync.log");
            Logger.LogCrash("LoadElementPropertiesFromNeo4j failed", ex);
            return null;
        }
    }

    #region Door Methods

    /// <summary>
    /// Creates or updates a door from Neo4j graph properties
    /// </summary>
    private void UpsertDoorFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"DOOR UPSERT: Processing door with remoteElementId={remoteElementId}", "sync.log");

            // Find existing door by NeoCollab tag
            FamilyInstance existingDoor = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);

            if (existingDoor != null)
            {
                Logger.LogToFile($"DOOR UPSERT: Updating existing door ElementId={existingDoor.Id}", "sync.log");
                UpdateDoorFromGraphProperties(existingDoor, properties);
            }
            else
            {
                Logger.LogToFile($"DOOR UPSERT: Creating new door", "sync.log");
                CreateDoorFromGraphProperties(doc, properties);
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpsertDoorFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void CreateDoorFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            
            // Get door type and family information  
            // CRITICAL FIX: Use correct property names from DoorSerializer
            string typeName = properties.ContainsKey("symbolName") ? properties["symbolName"].ToString() : 
                             properties.ContainsKey("typeName") ? properties["typeName"].ToString() : "Standard";
            string familyName = properties.ContainsKey("familyName") ? properties["familyName"].ToString() : "Door";
            
            Logger.LogToFile($"DOOR TYPE SEARCH: Looking for family='{familyName}', symbolName='{typeName}'", "sync.log");
            
            // Find door family symbol with precise matching
            FamilySymbol doorSymbol = new FilteredElementCollector(doc)
                .OfClass(typeof(FamilySymbol))
                .OfCategory(BuiltInCategory.OST_Doors)
                .Cast<FamilySymbol>()
                .FirstOrDefault(fs => fs.Family.Name == familyName && fs.Name == typeName);

            if (doorSymbol == null)
            {
                // Try fallback: match by type name only
                Logger.LogToFile($"DOOR TYPE FALLBACK: Exact match failed, trying type name only: '{typeName}'", "sync.log");
                doorSymbol = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilySymbol))
                    .OfCategory(BuiltInCategory.OST_Doors)
                    .Cast<FamilySymbol>()
                    .FirstOrDefault(fs => fs.Name == typeName);
            }
            
            if (doorSymbol == null)
            {
                // Try fallback: match by family name only
                Logger.LogToFile($"DOOR TYPE FALLBACK: Type name failed, trying family name only: '{familyName}'", "sync.log");
                doorSymbol = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilySymbol))
                    .OfCategory(BuiltInCategory.OST_Doors)
                    .Cast<FamilySymbol>()
                    .FirstOrDefault(fs => fs.Family.Name == familyName);
            }
            
            if (doorSymbol == null)
            {
                // List available door types for debugging
                var availableTypes = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilySymbol))
                    .OfCategory(BuiltInCategory.OST_Doors)
                    .Cast<FamilySymbol>()
                    .Take(5)
                    .Select(fs => $"{fs.Family.Name}:{fs.Name}")
                    .ToList();
                    
                Logger.LogToFile($"DOOR TYPE ERROR: No door symbol found for family='{familyName}', type='{typeName}'", "sync.log");
                Logger.LogToFile($"DOOR TYPE AVAILABLE: Available types: {string.Join(", ", availableTypes)}", "sync.log");
                return;
            }
            
            Logger.LogToFile($"DOOR TYPE FOUND: Using {doorSymbol.Family.Name}:{doorSymbol.Name}", "sync.log");

            if (!doorSymbol.IsActive)
                doorSymbol.Activate();

            // Get host wall information
            int hostElementId = properties.ContainsKey("hostId") ? Convert.ToInt32(properties["hostId"]) : 0;
            Wall hostWall = null;
            
            if (hostElementId > 0)
            {
                hostWall = FindElementByRemoteElementId<Wall>(doc, hostElementId);
            }

            if (hostWall == null)
            {
                Logger.LogToFile($"ERROR: Host wall not found for door remoteElementId={remoteElementId}, hostElementId={hostElementId}", "sync.log");
                return;
            }

            Logger.LogToFile($"DOOR CREATION: Found host wall ElementId={hostWall.Id} for door remoteElementId={remoteElementId}", "sync.log");

            // Create door location point with precision rounding
            double x = Convert.ToDouble(properties.ContainsKey("x") ? properties["x"] : 0);
            double y = Convert.ToDouble(properties.ContainsKey("y") ? properties["y"] : 0);
            double z = Convert.ToDouble(properties.ContainsKey("z") ? properties["z"] : 0);
            
            // Removed verbose door location logging - door creation works correctly
            // Logger.LogToFile($"DOOR LOCATION RAW: x={x}, y={y}, z={z} (in meters from Neo4j)", "sync.log");
            
            XYZ location = new XYZ(
                ToFeetPreciseWithLogging(x, "DoorX"),
                ToFeetPreciseWithLogging(y, "DoorY"),
                ToFeetPreciseWithLogging(z, "DoorZ")
            );

            // Removed verbose door creation coordinates logging  
            // Logger.LogToFile($"DOOR CREATION: Creating door at location ({x:F6}, {y:F6}, {z:F6})m = ({location.X:F10}, {location.Y:F10}, {location.Z:F10})ft with host wall ElementId={hostWall.Id}", "sync.log");

            // Create the door with detailed error handling
            FamilyInstance door = null;
            try
            {
                door = doc.Create.NewFamilyInstance(location, doorSymbol, hostWall, StructuralType.NonStructural);
                Logger.LogToFile($"DOOR CREATION SUCCESS: Created door ElementId={door.Id}", "sync.log");
                
                // CRITICAL FIX: Restore exact Z-coordinate from Neo4j after creation
                // The hostWall parameter forces the door to the wall's elevation, overriding our Z-coordinate
                LocationPoint doorLocation = door.Location as LocationPoint;
                if (doorLocation != null)
                {
                    XYZ currentPos = doorLocation.Point;
                    XYZ correctedPos = new XYZ(currentPos.X, currentPos.Y, location.Z);
                    
                    Logger.LogToFile($"DOOR Z-CORRECTION: Wall forced Z={currentPos.Z:F10}ft, correcting to Neo4j Z={location.Z:F10}ft", "sync.log");
                    doorLocation.Point = correctedPos;
                    
                    // Verify correction
                    XYZ finalPos = doorLocation.Point;
                    Logger.LogToFile($"DOOR Z-VERIFICATION: Final position ({finalPos.X:F10}, {finalPos.Y:F10}, {finalPos.Z:F10})ft", "sync.log");
                }
            }
            catch (Exception doorEx)
            {
                Logger.LogToFile($"DOOR CREATION FAILED: {doorEx.Message} for door remoteElementId={remoteElementId}", "sync.log");
                Logger.LogToFile($"DOOR ERROR DETAILS: DoorType={typeName}, Family={familyName}, Host={hostWall.Id}, Location=({x:F3}, {y:F3}, {z:F3})", "sync.log");
                throw new Exception($"Door creation failed: {doorEx.Message}. Check if door type '{typeName}' is compatible with host wall and location is valid.", doorEx);
            }
            
            // Mark with NeoCollab tag
            MarkDoorWithRemoteId(door, remoteElementId);
            
            // KRITISCH: Setze IFC-GUID für Solibri-Synchronisation
            if (properties.ContainsKey("uid"))
            {
                var uid = properties["uid"].ToString();
                var ifcGuidParam = door.get_Parameter(BuiltInParameter.IFC_GUID);
                if (ifcGuidParam != null && !ifcGuidParam.IsReadOnly)
                {
                    ifcGuidParam.Set(uid);
                    Logger.LogToFile($"Set door IFC-GUID to {uid} for Solibri synchronization", "sync.log");
                }
            }
            
            Logger.LogToFile($"DOOR CREATE: Created door ElementId={door.Id} for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in CreateDoorFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void UpdateDoorFromGraphProperties(FamilyInstance door, Dictionary<string, object> properties)
    {
        try
        {
            Logger.LogToFile($"DOOR UPDATE: Updating door ElementId={door.Id}", "sync.log");
            
            // CRITICAL FIX: Update door position (including Z-coordinate) from Neo4j
            if (properties.ContainsKey("x") && properties.ContainsKey("y") && properties.ContainsKey("z"))
            {
                double x = Convert.ToDouble(properties["x"]);
                double y = Convert.ToDouble(properties["y"]);
                double z = Convert.ToDouble(properties["z"]);
                
                XYZ newLocation = new XYZ(
                    ToFeetPreciseWithLogging(x, "DoorUpdateX"),
                    ToFeetPreciseWithLogging(y, "DoorUpdateY"),
                    ToFeetPreciseWithLogging(z, "DoorUpdateZ")
                );
                
                LocationPoint doorLocation = door.Location as LocationPoint;
                if (doorLocation != null)
                {
                    XYZ oldLocation = doorLocation.Point;
                    Logger.LogToFile($"DOOR POSITION UPDATE: Old({oldLocation.X:F10}, {oldLocation.Y:F10}, {oldLocation.Z:F10})ft", "sync.log");
                    Logger.LogToFile($"DOOR POSITION UPDATE: New({newLocation.X:F10}, {newLocation.Y:F10}, {newLocation.Z:F10})ft", "sync.log");
                    
                    doorLocation.Point = newLocation;
                    
                    // Verify update
                    XYZ verifyLocation = doorLocation.Point;
                    Logger.LogToFile($"DOOR POSITION VERIFY: Final({verifyLocation.X:F10}, {verifyLocation.Y:F10}, {verifyLocation.Z:F10})ft", "sync.log");
                }
            }
            
            // Update door parameters if they exist with precision rounding from METERS
            if (properties.ContainsKey("width"))
            {
                double width = Convert.ToDouble(properties["width"]);
                Logger.LogToFile($"DOOR UPDATE WIDTH: Raw width={width}m from Neo4j", "sync.log");
                Parameter widthParam = door.LookupParameter("Width");
                if (widthParam != null && !widthParam.IsReadOnly)
                {
                    double widthFeet = ToFeetPreciseWithLogging(width, "DoorWidth");
                    widthParam.Set(widthFeet);
                    Logger.LogToFile($"DOOR UPDATE WIDTH SUCCESS: Set width to {widthFeet}ft", "sync.log");
                }
            }
            
            if (properties.ContainsKey("height"))
            {
                double height = Convert.ToDouble(properties["height"]);
                Logger.LogToFile($"DOOR UPDATE HEIGHT: Raw height={height}m from Neo4j", "sync.log");
                Parameter heightParam = door.LookupParameter("Height");
                if (heightParam != null && !heightParam.IsReadOnly)
                {
                    double heightFeet = ToFeetPreciseWithLogging(height, "DoorHeight");
                    heightParam.Set(heightFeet);
                    Logger.LogToFile($"DOOR UPDATE HEIGHT SUCCESS: Set height to {heightFeet}ft", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpdateDoorFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void MarkDoorWithRemoteId(FamilyInstance door, int remoteElementId)
    {
        try
        {
            // Use unified tagging format for all elements (same as walls)
            var p = door.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            if (p != null && !p.IsReadOnly)
            {
                var current = p.AsString();
                var tag = $"NeoCollab:ElementId={remoteElementId}";
                
                // Use same format as walls: add PulledFrom marker
                var pullMarker = $"NeoCollab:PulledFrom={CommandManager.Instance.SessionId}";
                
                if (string.IsNullOrEmpty(current))
                {
                    p.Set($"{tag}; {pullMarker}");
                    Logger.LogToFile($"DOOR MARK: Marked door ElementId={door.Id} with unified tag={tag}; {pullMarker}", "sync.log");
                }
                else if (!current.Contains(tag))
                {
                    p.Set($"{current}; {tag}; {pullMarker}");
                    Logger.LogToFile($"DOOR MARK: Added to existing tag - ElementId={door.Id} with {tag}; {pullMarker}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"DOOR MARK: Tag already exists for ElementId={door.Id}, current tag={current}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in MarkDoorWithRemoteId: {ex.Message}", ex);
        }
    }

    private void DeleteDoorByRemoteElementId(Document doc, int remoteElementId)
    {
        try
        {
            FamilyInstance door = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);
            if (door != null)
            {
                // Get door GUID for Solibri deletion before deleting from Revit
                var doorGuidParam = door.get_Parameter(BuiltInParameter.IFC_GUID);
                var doorGuid = doorGuidParam?.AsString();
                
                Logger.LogToFile($"DOOR DELETE: Deleting door ElementId={door.Id} for remoteElementId={remoteElementId}", "sync.log");
                doc.Delete(door.Id);
                
                // Delete from Solibri if we have a GUID
                if (!string.IsNullOrEmpty(doorGuid))
                {
                    try
                    {
                        Logger.LogToFile($"DELETE BY ELEMENT ID: Deleting door component from Solibri with GUID {doorGuid}", "sync.log");
                        
                        Task.Run(async () =>
                        {
                            try
                            {
                                await _solibriValidationService.DeleteComponentsAsync(new List<string> { doorGuid });
                                Logger.LogToFile($"DELETE BY ELEMENT ID: Successfully deleted door component from Solibri", "sync.log");
                            }
                            catch (Exception ex)
                            {
                                Logger.LogToFile($"DELETE BY ELEMENT ID: Failed to delete door component from Solibri: {ex.Message}", "sync.log");
                            }
                        });
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"DELETE BY ELEMENT ID: Error initiating Solibri door deletion: {ex.Message}", "sync.log");
                    }
                }
            }
            else
            {
                Logger.LogToFile($"DOOR DELETE: Door not found for remoteElementId={remoteElementId}", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in DeleteDoorByRemoteElementId: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Deletes doors based on geographic properties (host wall, position, dimensions) instead of ElementId
    /// </summary>
    private void DeleteDoorByGeographicProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
            Logger.LogToFile($"DELETE BY GEOGRAPHY: Starting geographic deletion for door with remoteElementId={remoteElementId}", "sync.log");

            // Validate that we have the minimum required geographic data for doors
            if (!properties.ContainsKey("hostUid") && !properties.ContainsKey("hostId"))
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: Missing host information for door, cannot delete geographically", "sync.log");
                return;
            }

            // Find all doors that match the geographic properties
            var matchingDoors = FindDoorsByGeographicProperties(doc, properties, remoteElementId);
            
            if (matchingDoors.Count == 0)
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: No doors found at the specified geographic location for remoteElementId={remoteElementId}", "sync.log");
                return;
            }

            // Delete all matching doors
            var deletedGuids = new List<string>();
            foreach (var door in matchingDoors)
            {
                try
                {
                    var doorTag = door.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    
                    // Get door GUID for Solibri deletion
                    var doorGuidParam = door.get_Parameter(BuiltInParameter.IFC_GUID);
                    var doorGuid = doorGuidParam?.AsString();
                    if (!string.IsNullOrEmpty(doorGuid))
                    {
                        deletedGuids.Add(doorGuid);
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Door {door.Id} has GUID {doorGuid} for Solibri deletion", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Door {door.Id} has no GUID, cannot delete from Solibri", "sync.log");
                    }
                    
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting door {door.Id} with tag '{doorTag ?? "no tag"}'", "sync.log");
                    
                    doc.Delete(door.Id);
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted door {door.Id} based on geographic match", "sync.log");
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete door {door.Id}: {ex.Message}", "sync.log");
                }
            }

            // Delete components from Solibri if we have GUIDs
            if (deletedGuids.Any())
            {
                try
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting {deletedGuids.Count} door components from Solibri: [{string.Join(", ", deletedGuids)}]", "sync.log");
                    
                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(deletedGuids);
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted {deletedGuids.Count} door components from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete door components from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Error initiating Solibri door deletion: {ex.Message}", "sync.log");
                }
            }

            Logger.LogToFile($"DELETE BY GEOGRAPHY: Completed geographic deletion, deleted {matchingDoors.Count} doors for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("DeleteDoorByGeographicProperties failed", ex);
        }
    }

    /// <summary>
    /// Finds doors that match the geographic properties (host wall, position, dimensions)
    /// </summary>
    private List<FamilyInstance> FindDoorsByGeographicProperties(Document doc, Dictionary<string, object> properties, int remoteElementId)
    {
        var matchingDoors = new List<FamilyInstance>();
        
        try
        {
            Logger.LogToFile($"FIND BY GEOGRAPHY: Searching for doors matching geographic properties (excluding original ElementId {remoteElementId})", "sync.log");
            
            var doors = new FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_Doors).WhereElementIsNotElementType().Cast<FamilyInstance>();
            int checkedCount = 0;
            
            foreach (var door in doors)
            {
                checkedCount++;
                
                // CRITICAL: Skip the original door with the same ElementId to prevent self-deletion
                if (door.Id.Value == remoteElementId)
                {
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Skipping original door {door.Id} (same ElementId as remoteElementId)", "sync.log");
                    continue;
                }
                
                // Check if this door matches the geographic properties
                if (IsDoorGeographicallyIdentical(door, properties))
                {
                    var doorTag = door.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Found matching door {door.Id} with tag '{doorTag ?? "no tag"}'", "sync.log");
                    matchingDoors.Add(door);
                }
            }
            
            Logger.LogToFile($"FIND BY GEOGRAPHY: Checked {checkedCount} doors, found {matchingDoors.Count} matches for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("FindDoorsByGeographicProperties failed", ex);
        }
        
        return matchingDoors;
    }

    /// <summary>
    /// Checks if a door is geographically identical to Neo4j properties
    /// </summary>
    private bool IsDoorGeographicallyIdentical(FamilyInstance door, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance
            
            // Check host wall match
            if (properties.ContainsKey("hostUid"))
            {
                var hostUid = properties["hostUid"].ToString();
                var hostWall = door.Host;
                if (hostWall == null || hostWall.UniqueId != hostUid)
                {
                    return false;
                }
            }
            else if (properties.ContainsKey("hostId"))
            {
                var hostId = Convert.ToInt64(properties["hostId"]);
                var hostWall = door.Host;
                if (hostWall == null || hostWall.Id.Value != hostId)
                {
                    return false;
                }
            }
            
            // Check position if available
            if (properties.ContainsKey("x") && properties.ContainsKey("y") && properties.ContainsKey("z"))
            {
                var doorLocation = (door.Location as LocationPoint)?.Point;
                if (doorLocation != null)
                {
                    double neoX = ToFeetPreciseWithLogging(Convert.ToDouble(properties["x"]), "DeleteDoorGeographyX");
                    double neoY = ToFeetPreciseWithLogging(Convert.ToDouble(properties["y"]), "DeleteDoorGeographyY");
                    double neoZ = ToFeetPreciseWithLogging(Convert.ToDouble(properties["z"]), "DeleteDoorGeographyZ");
                    
                    if (Math.Abs(doorLocation.X - neoX) >= TOLERANCE ||
                        Math.Abs(doorLocation.Y - neoY) >= TOLERANCE ||
                        Math.Abs(doorLocation.Z - neoZ) >= TOLERANCE)
                    {
                        return false;
                    }
                }
            }
            
            // Check dimensions if available
            if (properties.ContainsKey("width") && properties.ContainsKey("height"))
            {
                var widthParam = door.get_Parameter(BuiltInParameter.DOOR_WIDTH);
                var heightParam = door.get_Parameter(BuiltInParameter.DOOR_HEIGHT);
                
                if (widthParam != null && heightParam != null)
                {
                    double neoWidth = ToFeetPreciseWithLogging(Convert.ToDouble(properties["width"]), "DeleteDoorGeographyWidth");
                    double neoHeight = ToFeetPreciseWithLogging(Convert.ToDouble(properties["height"]), "DeleteDoorGeographyHeight");
                    
                    if (Math.Abs(widthParam.AsDouble() - neoWidth) >= TOLERANCE ||
                        Math.Abs(heightParam.AsDouble() - neoHeight) >= TOLERANCE)
                    {
                        return false;
                    }
                }
            }
            
            Logger.LogToFile($"DELETE GEOGRAPHY: Door {door.Id} matches geographic properties", "sync.log");
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsDoorGeographicallyIdentical failed", ex);
            return false;
        }
    }

    #endregion

    #region Pipe Methods

    /// <summary>
    /// Creates or updates a pipe from Neo4j graph properties
    /// </summary>
    private void UpsertPipeFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"PIPE UPSERT: Processing pipe with remoteElementId={remoteElementId}", "sync.log");

            // Find existing pipe by NeoCollab tag
            Pipe existingPipe = FindElementByRemoteElementId<Pipe>(doc, remoteElementId);

            if (existingPipe != null)
            {
                Logger.LogToFile($"PIPE UPSERT: Updating existing pipe ElementId={existingPipe.Id}", "sync.log");
                UpdatePipeFromGraphProperties(existingPipe, properties, doc);
            }
            else
            {
                Logger.LogToFile($"PIPE UPSERT: Creating new pipe", "sync.log");
                CreatePipeFromGraphProperties(doc, properties);
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpsertPipeFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void CreatePipeFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            
            // Get pipe coordinates
            double x1 = Convert.ToDouble(properties["x1"]);
            double y1 = Convert.ToDouble(properties["y1"]);
            double z1 = Convert.ToDouble(properties["z1"]);
            double x2 = Convert.ToDouble(properties["x2"]);
            double y2 = Convert.ToDouble(properties["y2"]);
            double z2 = Convert.ToDouble(properties["z2"]);
            
            Logger.LogToFile($"PIPE GEOMETRY RAW: Start=({x1}, {y1}, {z1}), End=({x2}, {y2}, {z2}) (in meters)", "sync.log");
            
            XYZ startPoint = new XYZ(
                ToFeetPreciseWithLogging(x1, "PipeX1"),
                ToFeetPreciseWithLogging(y1, "PipeY1"),
                ToFeetPreciseWithLogging(z1, "PipeZ1")
            );
            
            XYZ endPoint = new XYZ(
                ToFeetPreciseWithLogging(x2, "PipeX2"),
                ToFeetPreciseWithLogging(y2, "PipeY2"),
                ToFeetPreciseWithLogging(z2, "PipeZ2")
            );

            // Get pipe type
            string typeName = properties.ContainsKey("typeName") ? properties["typeName"].ToString() : "Default";
            PipeType pipeType = new FilteredElementCollector(doc)
                .OfClass(typeof(PipeType))
                .Cast<PipeType>()
                .FirstOrDefault(pt => pt.Name == typeName) ??
                new FilteredElementCollector(doc)
                .OfClass(typeof(PipeType))
                .Cast<PipeType>()
                .FirstOrDefault();

            if (pipeType == null)
            {
                Logger.LogToFile($"ERROR: No pipe type found for typeName={typeName}", "sync.log");
                return;
            }

            // Get mechanical system (required for pipe creation)
            MEPSystem system = new FilteredElementCollector(doc)
                .OfClass(typeof(PipingSystem))
                .Cast<MEPSystem>()
                .FirstOrDefault() ??
                new FilteredElementCollector(doc)
                .OfClass(typeof(MEPSystem))
                .Cast<MEPSystem>()
                .FirstOrDefault();

            if (system == null)
            {
                Logger.LogToFile($"ERROR: No mechanical system found for pipe creation", "sync.log");
                return;
            }

            // Get level for pipe placement
            Level level = new FilteredElementCollector(doc)
                .OfClass(typeof(Level))
                .Cast<Level>()
                .FirstOrDefault();

            if (level == null)
            {
                Logger.LogToFile($"ERROR: No level found for pipe creation", "sync.log");
                return;
            }

            // Create the pipe
            Pipe pipe = Pipe.Create(doc, system.GetTypeId(), pipeType.Id, level.Id, startPoint, endPoint);
            
            if (pipe != null)
            {
                // CRITICAL FIX: Set pipe diameter from Neo4j properties
                if (properties.ContainsKey("diameter"))
                {
                    double diameterMeters = Convert.ToDouble(properties["diameter"]);
                    double diameterFeet = ToFeetPreciseWithLogging(diameterMeters, "PipeDiameter");
                    
                    Parameter diameterParam = pipe.get_Parameter(BuiltInParameter.RBS_PIPE_DIAMETER_PARAM);
                    if (diameterParam != null && !diameterParam.IsReadOnly)
                    {
                        diameterParam.Set(diameterFeet);
                        Logger.LogToFile($"PIPE DIAMETER SET: Set diameter to {diameterMeters}m = {diameterFeet}ft", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"PIPE DIAMETER WARNING: Diameter parameter not accessible for pipe {pipe.Id}", "sync.log");
                    }
                }
                
                // Mark with NeoCollab tag
                MarkPipeWithRemoteId(pipe, remoteElementId);
                
                // KRITISCH: Setze IFC-GUID für Solibri-Synchronisation
                if (properties.ContainsKey("uid"))
                {
                    var uid = properties["uid"].ToString();
                    var ifcGuidParam = pipe.get_Parameter(BuiltInParameter.IFC_GUID);
                    if (ifcGuidParam != null && !ifcGuidParam.IsReadOnly)
                    {
                        ifcGuidParam.Set(uid);
                        Logger.LogToFile($"Set pipe IFC-GUID to {uid} for Solibri synchronization", "sync.log");
                    }
                }
                
                // CRITICAL FIX: Update relationships when Pipe is created/modified via pull
                Logger.LogToFile($"PIPE RELATIONSHIPS: Updating relationships for new/modified Pipe {pipe.Id}", "sync.log");
                UpdatePipeRelationships(pipe, doc);
                
                Logger.LogToFile($"PIPE CREATE: Created pipe ElementId={pipe.Id} for remoteElementId={remoteElementId}", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in CreatePipeFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void UpdatePipeFromGraphProperties(Pipe pipe, Dictionary<string, object> properties, Document doc)
    {
        try
        {
            Logger.LogToFile($"PIPE UPDATE: Updating pipe ElementId={pipe.Id}", "sync.log");
            
            // Update pipe geometry with precision rounding
            if (properties.ContainsKey("x1") && properties.ContainsKey("y1") && properties.ContainsKey("z1") &&
                properties.ContainsKey("x2") && properties.ContainsKey("y2") && properties.ContainsKey("z2"))
            {
                double x1 = Convert.ToDouble(properties["x1"]);
                double y1 = Convert.ToDouble(properties["y1"]);
                double z1 = Convert.ToDouble(properties["z1"]);
                double x2 = Convert.ToDouble(properties["x2"]);
                double y2 = Convert.ToDouble(properties["y2"]);
                double z2 = Convert.ToDouble(properties["z2"]);
                
                Logger.LogToFile($"PIPE UPDATE GEOMETRY: Start=({x1}, {y1}, {z1}), End=({x2}, {y2}, {z2}) (meters)", "sync.log");
                
                XYZ startPoint = new XYZ(
                    ToFeetPreciseWithLogging(x1, "PipeUpdateX1"),
                    ToFeetPreciseWithLogging(y1, "PipeUpdateY1"),
                    ToFeetPreciseWithLogging(z1, "PipeUpdateZ1")
                );
                
                XYZ endPoint = new XYZ(
                    ToFeetPreciseWithLogging(x2, "PipeUpdateX2"),
                    ToFeetPreciseWithLogging(y2, "PipeUpdateY2"),
                    ToFeetPreciseWithLogging(z2, "PipeUpdateZ2")
                );

                // Update pipe location curve
                LocationCurve locationCurve = pipe.Location as LocationCurve;
                if (locationCurve != null)
                {
                    Line newLine = Line.CreateBound(startPoint, endPoint);
                    locationCurve.Curve = newLine;
                }
            }
            
            // CRITICAL FIX: Update pipe diameter if provided
            if (properties.ContainsKey("diameter"))
            {
                double diameterMeters = Convert.ToDouble(properties["diameter"]);
                double diameterFeet = ToFeetPreciseWithLogging(diameterMeters, "PipeUpdateDiameter");
                
                Parameter diameterParam = pipe.get_Parameter(BuiltInParameter.RBS_PIPE_DIAMETER_PARAM);
                if (diameterParam != null && !diameterParam.IsReadOnly)
                {
                    diameterParam.Set(diameterFeet);
                    Logger.LogToFile($"PIPE DIAMETER UPDATE: Set diameter to {diameterMeters}m = {diameterFeet}ft", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"PIPE DIAMETER UPDATE WARNING: Diameter parameter not accessible for pipe {pipe.Id}", "sync.log");
                }
            }
            
            // CRITICAL FIX: Update relationships when Pipe geometry changes
            Logger.LogToFile($"PIPE RELATIONSHIPS: Updating relationships for modified Pipe {pipe.Id}", "sync.log");
            UpdatePipeRelationships(pipe, doc);
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpdatePipeFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void MarkPipeWithRemoteId(Pipe pipe, int remoteElementId)
    {
        try
        {
            // Use unified tagging format for all elements (same as walls)
            var p = pipe.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            if (p != null && !p.IsReadOnly)
            {
                var current = p.AsString();
                var tag = $"NeoCollab:ElementId={remoteElementId}";
                
                // Use same format as walls: add PulledFrom marker
                var pullMarker = $"NeoCollab:PulledFrom={CommandManager.Instance.SessionId}";
                
                if (string.IsNullOrEmpty(current))
                {
                    p.Set($"{tag}; {pullMarker}");
                    Logger.LogToFile($"PIPE MARK: Marked pipe ElementId={pipe.Id} with unified tag={tag}; {pullMarker}", "sync.log");
                }
                else if (!current.Contains(tag))
                {
                    p.Set($"{current}; {tag}; {pullMarker}");
                    Logger.LogToFile($"PIPE MARK: Added to existing tag - ElementId={pipe.Id} with {tag}; {pullMarker}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"PIPE MARK: Tag already exists for ElementId={pipe.Id}, current tag={current}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in MarkPipeWithRemoteId: {ex.Message}", ex);
        }
    }

    private void DeletePipeByRemoteElementId(Document doc, int remoteElementId, Dictionary<string, object> properties)
    {
        try
        {
            Pipe pipe = FindElementByRemoteElementId<Pipe>(doc, remoteElementId);
            string remoteUid = GetStringProperty(properties, "elementUid", "uid");
            string remoteGuid = GetStringProperty(properties, "elementGuid", "guid", "ifcGuid");

            if (pipe == null && !string.IsNullOrEmpty(remoteUid))
            {
                pipe = FindElementByUid<Pipe>(doc, remoteUid);
                if (pipe != null)
                {
                    Logger.LogToFile($"PIPE DELETE: Found pipe by UID {remoteUid}", "sync.log");
                }
            }

            if (pipe == null && !string.IsNullOrEmpty(remoteGuid))
            {
                pipe = FindElementByIfcGuid<Pipe>(doc, remoteGuid);
                if (pipe != null)
                {
                    Logger.LogToFile($"PIPE DELETE: Found pipe by GUID {remoteGuid}", "sync.log");
                }
            }

            string solibriGuid = remoteGuid;
            if (pipe != null)
            {
                // Get pipe GUID for Solibri deletion before deleting from Revit
                var pipeGuidParam = pipe.get_Parameter(BuiltInParameter.IFC_GUID);
                var pipeGuid = pipeGuidParam?.AsString();
                if (!string.IsNullOrEmpty(pipeGuid))
                {
                    solibriGuid = pipeGuid;
                }

                Logger.LogToFile($"PIPE DELETE: Deleting pipe ElementId={pipe.Id} for remoteElementId={remoteElementId}", "sync.log");
                doc.Delete(pipe.Id);
            }
            else
            {
                Logger.LogToFile($"PIPE DELETE: Pipe not found for remoteElementId={remoteElementId}", "sync.log");
            }

            if (!string.IsNullOrEmpty(solibriGuid))
            {
                try
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Deleting pipe component from Solibri with GUID {solibriGuid}", "sync.log");

                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(new List<string> { solibriGuid });
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Successfully deleted pipe component from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Failed to delete pipe component from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Error initiating Solibri pipe deletion: {ex.Message}", "sync.log");
                }
            }
            else
            {
                Logger.LogToFile($"DELETE BY ELEMENT ID: No GUID available to delete pipe from Solibri (remoteElementId={remoteElementId})", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in DeletePipeByRemoteElementId: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Deletes pipes based on geographic properties (coordinates, diameter) instead of ElementId
    /// </summary>
    private void DeletePipeByGeographicProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
            Logger.LogToFile($"DELETE BY GEOGRAPHY: Starting geographic deletion for pipe with remoteElementId={remoteElementId}", "sync.log");

            // Validate that we have the minimum required geographic data for pipes
            var requiredGeometryFields = new[] { "x1", "y1", "z1", "x2", "y2", "z2" };
            var missingGeometryFields = requiredGeometryFields.Where(field => !properties.ContainsKey(field)).ToList();
            if (missingGeometryFields.Any())
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: Missing required geometry fields: {string.Join(", ", missingGeometryFields)}, cannot delete geographically", "sync.log");
                return;
            }

            // Find all pipes that match the geographic properties
            var matchingPipes = FindPipesByGeographicProperties(doc, properties, remoteElementId);
            
            if (matchingPipes.Count == 0)
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: No pipes found at the specified geographic location for remoteElementId={remoteElementId}", "sync.log");
                return;
            }

            // Delete all matching pipes
            var deletedGuids = new List<string>();
            foreach (var pipe in matchingPipes)
            {
                try
                {
                    var pipeTag = pipe.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    
                    // Get pipe GUID for Solibri deletion
                    var pipeGuidParam = pipe.get_Parameter(BuiltInParameter.IFC_GUID);
                    var pipeGuid = pipeGuidParam?.AsString();
                    if (!string.IsNullOrEmpty(pipeGuid))
                    {
                        deletedGuids.Add(pipeGuid);
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Pipe {pipe.Id} has GUID {pipeGuid} for Solibri deletion", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Pipe {pipe.Id} has no GUID, cannot delete from Solibri", "sync.log");
                    }
                    
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting pipe {pipe.Id} with tag '{pipeTag ?? "no tag"}'", "sync.log");
                    
                    doc.Delete(pipe.Id);
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted pipe {pipe.Id} based on geographic match", "sync.log");
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete pipe {pipe.Id}: {ex.Message}", "sync.log");
                }
            }

            // Delete components from Solibri if we have GUIDs
            if (deletedGuids.Any())
            {
                try
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting {deletedGuids.Count} pipe components from Solibri: [{string.Join(", ", deletedGuids)}]", "sync.log");
                    
                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(deletedGuids);
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted {deletedGuids.Count} pipe components from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete pipe components from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Error initiating Solibri pipe deletion: {ex.Message}", "sync.log");
                }
            }

            Logger.LogToFile($"DELETE BY GEOGRAPHY: Completed geographic deletion, deleted {matchingPipes.Count} pipes for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("DeletePipeByGeographicProperties failed", ex);
        }
    }

    /// <summary>
    /// Finds pipes that match the geographic properties (coordinates, diameter)
    /// </summary>
    private List<Pipe> FindPipesByGeographicProperties(Document doc, Dictionary<string, object> properties, int remoteElementId)
    {
        var matchingPipes = new List<Pipe>();
        
        try
        {
            Logger.LogToFile($"FIND BY GEOGRAPHY: Searching for pipes matching geographic properties (excluding original ElementId {remoteElementId})", "sync.log");
            
            var pipes = new FilteredElementCollector(doc).OfClass(typeof(Pipe)).Cast<Pipe>();
            int checkedCount = 0;
            
            foreach (var pipe in pipes)
            {
                checkedCount++;
                
                // CRITICAL: Skip the original pipe with the same ElementId to prevent self-deletion
                if (pipe.Id.Value == remoteElementId)
                {
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Skipping original pipe {pipe.Id} (same ElementId as remoteElementId)", "sync.log");
                    continue;
                }
                
                // Check if this pipe matches the geographic properties
                if (IsPipeGeographicallyIdentical(pipe, properties))
                {
                    var pipeTag = pipe.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Found matching pipe {pipe.Id} with tag '{pipeTag ?? "no tag"}'", "sync.log");
                    matchingPipes.Add(pipe);
                }
            }
            
            Logger.LogToFile($"FIND BY GEOGRAPHY: Checked {checkedCount} pipes, found {matchingPipes.Count} matches for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("FindPipesByGeographicProperties failed", ex);
        }
        
        return matchingPipes;
    }

    /// <summary>
    /// Checks if a pipe is geographically identical to Neo4j properties
    /// </summary>
    private bool IsPipeGeographicallyIdentical(Pipe pipe, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance
            
            // Get pipe location curve
            var locationCurve = pipe.Location as LocationCurve;
            if (locationCurve?.Curve == null)
            {
                return false;
            }
            
            var line = locationCurve.Curve as Line;
            if (line == null)
            {
                return false;
            }
            
            var existingStart = line.GetEndPoint(0);
            var existingEnd = line.GetEndPoint(1);
            
            // Convert Neo4j coordinates to feet
            double neoX1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x1", 0.0)), "DeletePipeGeographyX1");
            double neoY1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y1", 0.0)), "DeletePipeGeographyY1");
            double neoZ1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z1", 0.0)), "DeletePipeGeographyZ1");
            double neoX2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x2", 0.0)), "DeletePipeGeographyX2");
            double neoY2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y2", 0.0)), "DeletePipeGeographyY2");
            double neoZ2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z2", 0.0)), "DeletePipeGeographyZ2");
            
            // Check if geometry matches (either direction)
            bool geometryMatch1 = Math.Abs(existingStart.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ1) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ2) < TOLERANCE;
            
            bool geometryMatch2 = Math.Abs(existingStart.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ2) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ1) < TOLERANCE;
            
            bool geometryMatches = geometryMatch1 || geometryMatch2;
            
            if (!geometryMatches)
            {
                return false;
            }
            
            // Check diameter if available (using pipe's diameter parameter)
            if (properties.ContainsKey("diameter"))
            {
                var diameterParam = pipe.get_Parameter(BuiltInParameter.RBS_PIPE_DIAMETER_PARAM);
                if (diameterParam != null)
                {
                    double neoDiameter = ToFeetPreciseWithLogging(Convert.ToDouble(properties["diameter"]), "DeletePipeGeographyDiameter");
                    double existingDiameter = diameterParam.AsDouble();
                    
                    if (Math.Abs(existingDiameter - neoDiameter) >= TOLERANCE)
                    {
                        return false;
                    }
                }
            }
            
            Logger.LogToFile($"DELETE GEOGRAPHY: Pipe {pipe.Id} matches geographic properties", "sync.log");
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsPipeGeographicallyIdentical failed", ex);
            return false;
        }
    }

    #endregion

    #region ProvisionalSpace Methods

    /// <summary>
    /// Creates or updates a provisional space from Neo4j graph properties
    /// </summary>
    private void UpsertProvisionalSpaceFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            
            // DETAILED LOGGING: Log incoming ChangeLog data
            Logger.LogToFile($"=== PROCESSING PROVISIONAL SPACE CHANGELOG ===", "sync.log");
            Logger.LogToFile($"CHANGELOG PROCESSING: remoteElementId={remoteElementId}", "sync.log");
            Logger.LogToFile($"CHANGELOG PROCESSING: Properties count={properties.Count}", "sync.log");
            Logger.LogToFile($"CHANGELOG PROCESSING: Current session={CommandManager.Instance.SessionId}", "sync.log");
            Logger.LogToFile($"CHANGELOG PROCESSING: Pull in progress={CommandManager.Instance.IsPullInProgress}", "sync.log");
            
            // Log key properties for debugging
            var keyProps = new[] { "elementId", "x", "y", "z", "familyName", "typeName", "__element_type__" };
            foreach (var key in keyProps)
            {
                if (properties.ContainsKey(key))
                {
                    Logger.LogToFile($"CHANGELOG KEY PROP: {key} = {properties[key]}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"CHANGELOG MISSING: {key} is missing from properties", "sync.log");
                }
            }
            
            Logger.LogToFile($"PROVISIONALSPACE UPSERT: Processing provisional space with remoteElementId={remoteElementId}", "sync.log");

            // CRITICAL DEBUG: First check if ANY ProvisionalSpaces exist before searching
            var allProvisionalSpaces = new FilteredElementCollector(doc)
                .OfClass(typeof(FamilyInstance))
                .Cast<FamilyInstance>()
                .Where(fi => fi.Category.Id.Value == (int)BuiltInCategory.OST_GenericModel && ParameterUtils.IsProvisionalSpace(fi))
                .ToList();
                
            Logger.LogToFile($"PROVISIONALSPACE DEBUG: Found {allProvisionalSpaces.Count} total ProvisionalSpaces in document", "sync.log");
            
            foreach (var ps in allProvisionalSpaces.Take(3)) // Log first 3 for debugging
            {
                var tag = ps.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                Logger.LogToFile($"PROVISIONALSPACE DEBUG: Existing PS {ps.Id} has tag: '{tag}'", "sync.log");
            }

            // Find existing provisional space by NeoCollab tag
            FamilyInstance existingSpace = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);
            
            // ADDITIONAL CHECK: If FindElementByRemoteElementId failed, try direct ProvisionalSpace search
            if (existingSpace == null)
            {
                Logger.LogToFile($"PROVISIONALSPACE FALLBACK: FindElementByRemoteElementId failed, trying direct search", "sync.log");
                var searchTag = $"NeoCollab:ElementId={remoteElementId}";
                
                existingSpace = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilyInstance))
                    .Cast<FamilyInstance>()
                    .Where(fi => fi.Category.Id.Value == (int)BuiltInCategory.OST_GenericModel && ParameterUtils.IsProvisionalSpace(fi))
                    .FirstOrDefault(ps => {
                        var tag = ps.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                        return !string.IsNullOrEmpty(tag) && tag.Contains(searchTag);
                    });
                    
                if (existingSpace != null)
                {
                    Logger.LogToFile($"PROVISIONALSPACE FALLBACK: FOUND ProvisionalSpace {existingSpace.Id} with direct search!", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"PROVISIONALSPACE FALLBACK: Still not found with direct search", "sync.log");
                }
            }

            if (existingSpace != null)
            {
                Logger.LogToFile($"PROVISIONALSPACE UPSERT: FOUND existing provisional space ElementId={existingSpace.Id} for remoteId={remoteElementId}", "sync.log");
                
                // DUPLICATE DETECTION: Check if existing element is identical to Neo4j data
                bool isIdentical = IsProvisionalSpaceIdentical(existingSpace, properties);
                if (isIdentical)
                {
                    Logger.LogToFile($"PROVISIONALSPACE DUPLICATE: Element {existingSpace.Id} is IDENTICAL to Neo4j data - SKIPPING update", "sync.log");
                    return; // Skip update for identical elements
                }
                
                UpdateProvisionalSpaceFromGraphProperties(existingSpace, properties, doc);
            }
            else
            {
                Logger.LogToFile($"PROVISIONALSPACE UPSERT: NO existing space found for remoteId={remoteElementId}, creating NEW provisional space", "sync.log");
                CreateProvisionalSpaceFromGraphProperties(doc, properties);
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpsertProvisionalSpaceFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void CreateProvisionalSpaceFromGraphProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            
            // DETAILED LOGGING: Log all properties received from Neo4j
            Logger.LogToFile($"=== CREATING PROVISIONAL SPACE FROM NEO4J ===", "sync.log");
            Logger.LogToFile($"NEO4J DATA: remoteElementId={remoteElementId}", "sync.log");
            Logger.LogToFile($"NEO4J DATA: Total properties count={properties.Count}", "sync.log");
            
            foreach (var prop in properties)
            {
                Logger.LogToFile($"NEO4J PROPERTY: {prop.Key} = {prop.Value ?? "NULL"}", "sync.log");
            }
            
            // Get family information
            string familyName = properties.ContainsKey("familyName") ? properties["familyName"].ToString() : "ProvisionalSpace";
            string typeName = properties.ContainsKey("typeName") ? properties["typeName"].ToString() : "Default";
            
            Logger.LogToFile($"NEO4J FAMILY: familyName={familyName}, typeName={typeName}", "sync.log");
            
            // Find provisional space family symbol with enhanced search
            FamilySymbol spaceSymbol = null;
            
            // First try exact match
            spaceSymbol = new FilteredElementCollector(doc)
                .OfClass(typeof(FamilySymbol))
                .OfCategory(BuiltInCategory.OST_GenericModel)
                .Cast<FamilySymbol>()
                .FirstOrDefault(fs => fs.Family.Name == familyName && fs.Name == typeName);
            
            if (spaceSymbol != null)
            {
                Logger.LogToFile($"PROVISIONAL SPACE SYMBOL: Found exact match - Family={familyName}, Type={typeName}", "sync.log");
            }
            
            // Fallback to family name only
            if (spaceSymbol == null)
            {
                spaceSymbol = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilySymbol))
                    .OfCategory(BuiltInCategory.OST_GenericModel)
                    .Cast<FamilySymbol>()
                    .FirstOrDefault(fs => fs.Family.Name == familyName);
                    
                if (spaceSymbol != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE SYMBOL: Found family match - Family={familyName}, Type={spaceSymbol.Name}", "sync.log");
                }
            }
            
            // Final fallback to any Generic Model symbol
            if (spaceSymbol == null)
            {
                spaceSymbol = new FilteredElementCollector(doc)
                    .OfClass(typeof(FamilySymbol))
                    .OfCategory(BuiltInCategory.OST_GenericModel)
                    .Cast<FamilySymbol>()
                    .FirstOrDefault();
                    
                if (spaceSymbol != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE SYMBOL: Using fallback Generic Model - Family={spaceSymbol.Family.Name}, Type={spaceSymbol.Name}", "sync.log");
                }
            }

            if (spaceSymbol == null)
            {
                Logger.LogToFile($"ERROR: No provisional space symbol found for family={familyName}, type={typeName}", "sync.log");
                return;
            }

            if (!spaceSymbol.IsActive)
                spaceSymbol.Activate();

            // Create location point with precision rounding
            double x = Convert.ToDouble(properties.ContainsKey("x") ? properties["x"] : 0);
            double y = Convert.ToDouble(properties.ContainsKey("y") ? properties["y"] : 0);
            double z = Convert.ToDouble(properties.ContainsKey("z") ? properties["z"] : 0);
            
            // CRITICAL FIX: Neo4j coordinates are MIN coordinates (bottom-left corner)
            // Revit FamilyInstance placement also expects the insertion point (origin)
            // So we should use the MIN coordinates directly, NOT the center!
            double width = Convert.ToDouble(properties.ContainsKey("width") ? properties["width"] : 0);
            double height = Convert.ToDouble(properties.ContainsKey("height") ? properties["height"] : 0);
            double thickness = Convert.ToDouble(properties.ContainsKey("thickness") ? properties["thickness"] : 0);
            
            Logger.LogToFile($"PROVISIONAL SPACE MIN (ORIGIN): x={x}, y={y}, z={z} (meters from Neo4j)", "sync.log");
            Logger.LogToFile($"PROVISIONAL SPACE DIMENSIONS: width={width}, height={height}, thickness={thickness} (meters)", "sync.log");
            
            // Use MIN coordinates directly as insertion point
            // CRITICAL FIX: Use same precise coordinate conversion as Walls/Doors for consistency
            double xFeet = ToFeetPreciseWithLogging(x, "ProvisionalSpaceX");
            double yFeet = ToFeetPreciseWithLogging(y, "ProvisionalSpaceY");
            double zFeet = ToFeetPreciseWithLogging(z, "ProvisionalSpaceZ");
            
            Logger.LogToFile($"PROVISIONAL SPACE COORDINATES: Min({x:F6}, {y:F6}, {z:F6})m → Revit({xFeet:F10}, {yFeet:F10}, {zFeet:F10})ft", "sync.log");
            Logger.LogToFile($"PROVISIONAL SPACE Z-COORDINATE: Using precise Z={z}m = {zFeet:F10}ft from Neo4j", "sync.log");
            
            XYZ location = new XYZ(xFeet, yFeet, zFeet);

            Logger.LogToFile($"PROVISIONAL SPACE CREATION: Creating space at MIN location ({x:F6}, {y:F6}, {z:F6})m = ({location.X:F10}, {location.Y:F10}, {location.Z:F10})ft", "sync.log");

            // Get level for provisional space placement - CRITICAL FIX with intelligent level selection!
            Level level = null;
            
            // Try to get level from properties first
            if (properties.ContainsKey("levelUid"))
            {
                string levelUid = properties["levelUid"].ToString();
                level = FindLevelByUid(doc, levelUid);
                if (level != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE LEVEL: Found level by UID {levelUid}: {level.Name}", "sync.log");
                }
            }
            
            if (level == null && properties.ContainsKey("levelId"))
            {
                var levelId = Convert.ToInt64(properties["levelId"]);
                level = doc.GetElement(new ElementId(levelId)) as Level;
                if (level != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE LEVEL: Found level by ID {levelId}: {level.Name}", "sync.log");
                }
            }
            
            // Fallback to default level
            if (level == null)
            {
                level = new FilteredElementCollector(doc)
                    .OfClass(typeof(Level))
                    .Cast<Level>()
                    .FirstOrDefault();
                    
                if (level != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE LEVEL: Using default level: {level.Name} (Id: {level.Id})", "sync.log");
                }
            }

            if (level == null)
            {
                Logger.LogToFile($"ERROR: No level found for provisional space creation", "sync.log");
                return;
            }

            Logger.LogToFile($"PROVISIONAL SPACE LEVEL: Using level {level.Name} (Id: {level.Id}) for space creation", "sync.log");

            // CRITICAL FIX: ProvisionalSpaces should keep their original Z-coordinates, not be placed on level elevation
            // CRITICAL FIX: Skip overlap check during PULL operations
            // During pull, we want to create ProvisionalSpaces from other sessions even if they "overlap"
            // Overlap check should only apply to NEW LOCAL ProvisionalSpaces, not pulled ones
            Logger.LogToFile($"PROVISIONAL SPACE PULL: Creating ProvisionalSpace from remote session - skipping overlap check", "sync.log");

            // Unlike walls, ProvisionalSpaces are 3D objects that can be positioned anywhere in space
            // To preserve exact Z-coordinate, we must create WITHOUT level parameter
            Logger.LogToFile($"PROVISIONAL SPACE LOCATION: Using exact 3D coordinates ({location.X:F10}, {location.Y:F10}, {location.Z:F10})ft", "sync.log");

            // Create the provisional space WITH level, then correct Z-coordinate - CRITICAL FIX
            FamilyInstance space = null;
            try 
            {
                // CRITICAL FIX: Must create with level to get valid position, then correct Z-coordinate
                space = doc.Create.NewFamilyInstance(location, spaceSymbol, level, StructuralType.NonStructural);
                Logger.LogToFile($"PROVISIONAL SPACE CREATION: Created with level {level.Name} at intended position", "sync.log");
                
                if (space == null)
                {
                    Logger.LogToFile($"ERROR: Failed to create provisional space with level", "sync.log");
                    return;
                }
                
                // CRITICAL FIX: Correct Z-coordinate after creation to preserve Neo4j value
                LocationPoint locationPoint = space.Location as LocationPoint;
                if (locationPoint != null)
                {
                    XYZ currentPos = locationPoint.Point;
                    XYZ correctedPos = new XYZ(currentPos.X, currentPos.Y, location.Z);
                    
                    Logger.LogToFile($"PROVISIONAL SPACE Z-CORRECTION: Level forced Z={currentPos.Z:F10}ft, correcting to Neo4j Z={location.Z:F10}ft", "sync.log");
                    locationPoint.Point = correctedPos;
                    
                    // Verify correction
                    XYZ finalPos = locationPoint.Point;
                    Logger.LogToFile($"PROVISIONAL SPACE Z-VERIFICATION: Final position ({finalPos.X:F10}, {finalPos.Y:F10}, {finalPos.Z:F10})ft", "sync.log");
                }
                
                // Verify actual position after correction
                var actualLocation = (space.Location as LocationPoint)?.Point;
                if (actualLocation != null)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE VERIFICATION: Final position ({actualLocation.X:F10}, {actualLocation.Y:F10}, {actualLocation.Z:F10})ft", "sync.log");
                    
                    // Check for coordinate issues
                    if (Math.Abs(actualLocation.X - location.X) > 0.01)
                    {
                        Logger.LogToFile($"WARNING: X-coordinate mismatch! Expected {location.X:F10}ft, got {actualLocation.X:F10}ft, delta={Math.Abs(actualLocation.X - location.X):F10}ft", "sync.log");
                    }
                    if (Math.Abs(actualLocation.Y - location.Y) > 0.01)
                    {
                        Logger.LogToFile($"WARNING: Y-coordinate mismatch! Expected {location.Y:F10}ft, got {actualLocation.Y:F10}ft, delta={Math.Abs(actualLocation.Y - location.Y):F10}ft", "sync.log");
                    }
                    if (Math.Abs(actualLocation.Z - location.Z) > 0.01)
                    {
                        Logger.LogToFile($"WARNING: Z-coordinate mismatch! Expected {location.Z:F10}ft, got {actualLocation.Z:F10}ft, delta={Math.Abs(actualLocation.Z - location.Z):F10}ft", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"SUCCESS: Z-coordinate correctly set to {actualLocation.Z:F10}ft", "sync.log");
                    }
                    
                    // CRITICAL DEBUG: Convert back to meters for comparison
                    double actualXMeters = UnitConversion.ToMeters(actualLocation.X);
                    double actualYMeters = UnitConversion.ToMeters(actualLocation.Y);
                    double actualZMeters = UnitConversion.ToMeters(actualLocation.Z);
                    Logger.LogToFile($"PROVISIONAL SPACE FINAL: Position in meters ({actualXMeters:F6}, {actualYMeters:F6}, {actualZMeters:F6})m", "sync.log");
                    Logger.LogToFile($"PROVISIONAL SPACE COMPARISON: Neo4j({x:F6}, {y:F6}, {z:F6})m vs Final({actualXMeters:F6}, {actualYMeters:F6}, {actualZMeters:F6})m", "sync.log");
                }
                
                Logger.LogToFile($"PROVISIONAL SPACE CREATED: Successfully created FamilyInstance ElementId={space.Id}", "sync.log");
                
                // VERIFICATION: Check actual position after creation
                if (space.Location is LocationPoint locPoint)
                {
                    var actualPos = locPoint.Point;
                    double actualXMeters = UnitConversion.ToMeters(actualPos.X);
                    double actualYMeters = UnitConversion.ToMeters(actualPos.Y);
                    double actualZMeters = UnitConversion.ToMeters(actualPos.Z);
                    
                    Logger.LogToFile($"PROVISIONAL SPACE VERIFICATION: Actual position after creation:", "sync.log");
                    Logger.LogToFile($"  ACTUAL REVIT: ({actualPos.X:F10}, {actualPos.Y:F10}, {actualPos.Z:F10}) ft", "sync.log");
                    Logger.LogToFile($"  ACTUAL METERS: ({actualXMeters:F6}, {actualYMeters:F6}, {actualZMeters:F6}) m", "sync.log");
                    Logger.LogToFile($"  EXPECTED METERS: ({x:F6}, {y:F6}, {z:F6}) m", "sync.log");
                    Logger.LogToFile($"  DIFFERENCE: ({Math.Abs(actualXMeters-x):F6}, {Math.Abs(actualYMeters-y):F6}, {Math.Abs(actualZMeters-z):F6}) m", "sync.log");
                }
            }
            catch (Exception createEx)
            {
                Logger.LogToFile($"ERROR: Exception during provisional space creation: {createEx.Message}", "sync.log");
                Logger.LogCrash($"Provisional space creation failed", createEx);
                return;
            }
            
            // CRITICAL FIX: Set height, width, thickness parameters from Neo4j data EXACTLY as stored
            Logger.LogToFile($"PROVISIONAL SPACE PARAMETERS: Setting height, width, thickness from Neo4j data", "sync.log");
            
            // Set height parameter - try multiple parameter names
            if (properties.ContainsKey("height"))
            {
                double heightMeters = Convert.ToDouble(properties["height"]);
                double heightFeet = ToFeetPreciseWithLogging(heightMeters, "ProvisionalSpaceCreateHeight");  // CONSISTENT conversion
                
                // Try multiple parameter names for height
                string[] heightNames = { "height", "Height", "HEIGHT", "h", "H", "Höhe" };
                bool heightSet = false;
                
                foreach (string paramName in heightNames)
                {
                    Parameter heightParam = space.LookupParameter(paramName);
                    if (heightParam != null && !heightParam.IsReadOnly)
                    {
                        heightParam.Set(heightFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE HEIGHT: Set parameter '{paramName}' = {heightMeters}m = {heightFeet:F10}ft", "sync.log");
                        heightSet = true;
                        break;
                    }
                }
                
                if (!heightSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE HEIGHT: No writable height parameter found. Tried: {string.Join(", ", heightNames)}", "sync.log");
                    
                    // Log all available parameters for debugging
                    Logger.LogToFile($"PROVISIONAL SPACE HEIGHT: Available parameters on element:", "sync.log");
                    foreach (Parameter param in space.Parameters)
                    {
                        if (param.Definition != null)
                        {
                            Logger.LogToFile($"  - '{param.Definition.Name}' (ReadOnly: {param.IsReadOnly})", "sync.log");
                        }
                    }
                }
            }
            else
            {
                Logger.LogToFile($"PROVISIONAL SPACE HEIGHT: No 'height' property in Neo4j data", "sync.log");
            }
            
            // Set width parameter - try multiple parameter names
            if (properties.ContainsKey("width"))
            {
                double widthMeters = Convert.ToDouble(properties["width"]);
                double widthFeet = ToFeetPreciseWithLogging(widthMeters, "ProvisionalSpaceCreateWidth");  // CONSISTENT conversion
                
                // Try multiple parameter names for width
                string[] widthNames = { "width", "Width", "WIDTH", "w", "W", "Breite" };
                bool widthSet = false;
                
                foreach (string paramName in widthNames)
                {
                    Parameter widthParam = space.LookupParameter(paramName);
                    if (widthParam != null && !widthParam.IsReadOnly)
                    {
                        widthParam.Set(widthFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE WIDTH: Set parameter '{paramName}' = {widthMeters}m = {widthFeet:F10}ft", "sync.log");
                        widthSet = true;
                        break;
                    }
                }
                
                if (!widthSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE WIDTH: No writable width parameter found. Tried: {string.Join(", ", widthNames)}", "sync.log");
                }
            }
            else
            {
                Logger.LogToFile($"PROVISIONAL SPACE WIDTH: No 'width' property in Neo4j data", "sync.log");
            }
            
            // Set thickness parameter - try multiple parameter names
            if (properties.ContainsKey("thickness"))
            {
                double thicknessMeters = Convert.ToDouble(properties["thickness"]);
                double thicknessFeet = ToFeetPreciseWithLogging(thicknessMeters, "ProvisionalSpaceCreateThickness");  // CONSISTENT conversion
                
                // Try multiple parameter names for thickness
                string[] thicknessNames = { "thickness", "Thickness", "THICKNESS", "t", "T", "Dicke", "Tiefe" };
                bool thicknessSet = false;
                
                foreach (string paramName in thicknessNames)
                {
                    Parameter thicknessParam = space.LookupParameter(paramName);
                    if (thicknessParam != null && !thicknessParam.IsReadOnly)
                    {
                        thicknessParam.Set(thicknessFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE THICKNESS: Set parameter '{paramName}' = {thicknessMeters}m = {thicknessFeet:F10}ft", "sync.log");
                        thicknessSet = true;
                        break;
                    }
                }
                
                if (!thicknessSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE THICKNESS: No writable thickness parameter found. Tried: {string.Join(", ", thicknessNames)}", "sync.log");
                }
            }
            else
            {
                Logger.LogToFile($"PROVISIONAL SPACE THICKNESS: No 'thickness' property in Neo4j data", "sync.log");
            }
            
            // Set custom parameters if they exist
            if (properties.ContainsKey("guid"))
            {
                Parameter guidParam = space.LookupParameter("GUID");
                if (guidParam != null && !guidParam.IsReadOnly)
                {
                    guidParam.Set(properties["guid"].ToString());
                }
            }
            
            // Mark with NeoCollab tag
            MarkProvisionalSpaceWithRemoteId(space, remoteElementId);
            
            // KRITISCH: Setze IFC-GUID für Solibri-Synchronisation (ProvisionalSpaces verwenden "guid")
            if (properties.ContainsKey("guid"))
            {
                var guid = properties["guid"].ToString();
                var ifcGuidParam = space.get_Parameter(BuiltInParameter.IFC_GUID);
                if (ifcGuidParam != null && !ifcGuidParam.IsReadOnly)
                {
                    ifcGuidParam.Set(guid);
                    Logger.LogToFile($"Set provisional space IFC-GUID to {guid} for Solibri synchronization", "sync.log");
                }
            }
            
            // CRITICAL FIX: Update pipe relationships when ProvisionalSpace is created/modified via pull
            Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Updating relationships for new/modified ProvisionalSpace {space.Id}", "sync.log");
            UpdateProvisionalSpaceRelationships(space, doc);
            
            Logger.LogToFile($"PROVISIONALSPACE CREATE: Created provisional space ElementId={space.Id} for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in CreateProvisionalSpaceFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void UpdateProvisionalSpaceFromGraphProperties(FamilyInstance space, Dictionary<string, object> properties, Document doc)
    {
        try
        {
            int remoteElementId = Convert.ToInt32(properties["elementId"]);
            Logger.LogToFile($"=== UPDATING PROVISIONAL SPACE ===", "sync.log");
            Logger.LogToFile($"PROVISIONALSPACE UPDATE: Updating ElementId={space.Id} for remoteElementId={remoteElementId}", "sync.log");
            Logger.LogToFile($"PROVISIONALSPACE UPDATE: Properties count={properties.Count}", "sync.log");
            
            bool updatePerformed = false;
            
            // Update location if coordinates are provided with precision rounding
            if (properties.ContainsKey("x") && properties.ContainsKey("y") && properties.ContainsKey("z"))
            {
                double x = Convert.ToDouble(properties["x"]);
                double y = Convert.ToDouble(properties["y"]);
                double z = Convert.ToDouble(properties["z"]);
                
                // CRITICAL FIX: Neo4j coordinates are MIN coordinates (bottom-left corner)
                // Revit FamilyInstance placement also expects the insertion point (origin)
                // So we should use the MIN coordinates directly, NOT the center!
                Logger.LogToFile($"PROVISIONAL SPACE UPDATE MIN (ORIGIN): ({x:F6}, {y:F6}, {z:F6}) meters", "sync.log");
                
                // CRITICAL FIX: Use same precise coordinate conversion as creation method
                double xFeet = ToFeetPreciseWithLogging(x, "ProvisionalSpaceUpdateX");
                double yFeet = ToFeetPreciseWithLogging(y, "ProvisionalSpaceUpdateY");
                double zFeet = ToFeetPreciseWithLogging(z, "ProvisionalSpaceUpdateZ");
                
                Logger.LogToFile($"PROVISIONAL SPACE UPDATE COORDINATES: Min({x:F6}, {y:F6}, {z:F6})m → Revit({xFeet:F10}, {yFeet:F10}, {zFeet:F10})ft", "sync.log");
                Logger.LogToFile($"PROVISIONAL SPACE UPDATE DETAIL: x={x} m = {xFeet} ft (factor: {xFeet/x:F4})", "sync.log");
                Logger.LogToFile($"PROVISIONAL SPACE UPDATE DETAIL: y={y} m = {yFeet} ft (factor: {yFeet/y:F4})", "sync.log");
                Logger.LogToFile($"PROVISIONAL SPACE UPDATE DETAIL: z={z} m = {zFeet} ft (factor: {zFeet/z:F4})", "sync.log");
                
                XYZ newLocation = new XYZ(xFeet, yFeet, zFeet);

                LocationPoint locationPoint = space.Location as LocationPoint;
                if (locationPoint != null)
                {
                    var oldLocation = locationPoint.Point;
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE: Old position ({oldLocation.X:F10}, {oldLocation.Y:F10}, {oldLocation.Z:F10})ft", "sync.log");
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE: New position ({newLocation.X:F10}, {newLocation.Y:F10}, {newLocation.Z:F10})ft", "sync.log");
                    
                    locationPoint.Point = newLocation;
                    updatePerformed = true;
                    
                    // Verify update using CONSISTENT conversion methods
                    var verifyLocation = locationPoint.Point;
                    double verifyXMeters = UnitConversion.ToMeters(verifyLocation.X);
                    double verifyYMeters = UnitConversion.ToMeters(verifyLocation.Y);
                    double verifyZMeters = UnitConversion.ToMeters(verifyLocation.Z);
                    
                    // Calculate deltas for verification
                    double deltaX = Math.Abs(verifyXMeters - x);
                    double deltaY = Math.Abs(verifyYMeters - y);
                    double deltaZ = Math.Abs(verifyZMeters - z);
                    
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE VERIFY: Final({verifyXMeters:F6}, {verifyYMeters:F6}, {verifyZMeters:F6})m vs Target({x:F6}, {y:F6}, {z:F6})m", "sync.log");
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE DELTAS: X={deltaX:F6}m, Y={deltaY:F6}m, Z={deltaZ:F6}m", "sync.log");
                    
                    if (deltaX > 0.001 || deltaY > 0.001 || deltaZ > 0.001)
                    {
                        Logger.LogToFile($"PROVISIONAL SPACE UPDATE WARNING: Position verification failed - deltas too large", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"PROVISIONAL SPACE UPDATE SUCCESS: Position correctly updated", "sync.log");
                    }
                }
            }
            
            // Update GUID parameter if provided
            if (properties.ContainsKey("guid"))
            {
                Parameter guidParam = space.LookupParameter("GUID");
                if (guidParam != null && !guidParam.IsReadOnly)
                {
                    guidParam.Set(properties["guid"].ToString());
                    updatePerformed = true;
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE GUID: Set GUID parameter", "sync.log");
                }
            }
            
            // CRITICAL FIX: Set height, width, thickness parameters from Neo4j data EXACTLY as stored
            Logger.LogToFile($"PROVISIONAL SPACE UPDATE PARAMETERS: Setting height, width, thickness from Neo4j data", "sync.log");
            
            // CRITICAL FIX: Set height, width, thickness parameters from Neo4j data EXACTLY as stored
            Logger.LogToFile($"PROVISIONAL SPACE UPDATE PARAMETERS: Setting height, width, thickness from Neo4j data", "sync.log");
            
            // Update height parameter - try multiple parameter names
            if (properties.ContainsKey("height"))
            {
                double heightMeters = Convert.ToDouble(properties["height"]);
                double heightFeet = ToFeetPreciseWithLogging(heightMeters, "ProvisionalSpaceUpdateHeight");  // CONSISTENT conversion
                
                // Try multiple parameter names for height
                string[] heightNames = { "height", "Height", "HEIGHT", "h", "H", "Höhe" };
                bool heightSet = false;
                
                foreach (string paramName in heightNames)
                {
                    Parameter heightParam = space.LookupParameter(paramName);
                    if (heightParam != null && !heightParam.IsReadOnly)
                    {
                        heightParam.Set(heightFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE UPDATE HEIGHT: Set parameter '{paramName}' = {heightMeters}m = {heightFeet:F10}ft", "sync.log");
                        heightSet = true;
                        updatePerformed = true;
                        break;
                    }
                }
                
                if (!heightSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE HEIGHT: No writable height parameter found. Tried: {string.Join(", ", heightNames)}", "sync.log");
                }
            }
            
            // Update width parameter - try multiple parameter names
            if (properties.ContainsKey("width"))
            {
                double widthMeters = Convert.ToDouble(properties["width"]);
                double widthFeet = ToFeetPreciseWithLogging(widthMeters, "ProvisionalSpaceUpdateWidth");  // CONSISTENT conversion
                
                // Try multiple parameter names for width
                string[] widthNames = { "width", "Width", "WIDTH", "w", "W", "Breite" };
                bool widthSet = false;
                
                foreach (string paramName in widthNames)
                {
                    Parameter widthParam = space.LookupParameter(paramName);
                    if (widthParam != null && !widthParam.IsReadOnly)
                    {
                        widthParam.Set(widthFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE UPDATE WIDTH: Set parameter '{paramName}' = {widthMeters}m = {widthFeet:F10}ft", "sync.log");
                        widthSet = true;
                        updatePerformed = true;
                        break;
                    }
                }
                
                if (!widthSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE WIDTH: No writable width parameter found. Tried: {string.Join(", ", widthNames)}", "sync.log");
                }
            }
            
            // Update thickness parameter - try multiple parameter names
            if (properties.ContainsKey("thickness"))
            {
                double thicknessMeters = Convert.ToDouble(properties["thickness"]);
                double thicknessFeet = ToFeetPreciseWithLogging(thicknessMeters, "ProvisionalSpaceUpdateThickness");  // CONSISTENT conversion
                
                // Try multiple parameter names for thickness
                string[] thicknessNames = { "thickness", "Thickness", "THICKNESS", "t", "T", "Dicke", "Tiefe" };
                bool thicknessSet = false;
                
                foreach (string paramName in thicknessNames)
                {
                    Parameter thicknessParam = space.LookupParameter(paramName);
                    if (thicknessParam != null && !thicknessParam.IsReadOnly)
                    {
                        thicknessParam.Set(thicknessFeet);
                        Logger.LogToFile($"PROVISIONAL SPACE UPDATE THICKNESS: Set parameter '{paramName}' = {thicknessMeters}m = {thicknessFeet:F10}ft", "sync.log");
                        thicknessSet = true;
                        updatePerformed = true;
                        break;
                    }
                }
                
                if (!thicknessSet)
                {
                    Logger.LogToFile($"PROVISIONAL SPACE UPDATE THICKNESS: No writable thickness parameter found. Tried: {string.Join(", ", thicknessNames)}", "sync.log");
                }
            }
            
            // CRITICAL FIX: Use ChangeLog-based update tracking instead of local flags
            // If any updates were performed, create ChangeLog entries for other sessions
            if (updatePerformed)
            {
                Logger.LogToFile($"PROVISIONALSPACE UPDATE PERFORMED: Creating ChangeLog entries for other sessions", "sync.log");
                
                // Create ChangeLog entries for all other sessions to notify about the update
                string currentSessionId = CommandManager.Instance.SessionId;
                var allSessions = SessionManager.OpenSessions.Keys.ToList();
                var targetSessions = allSessions.Where(s => s != currentSessionId).ToList();
                
                Logger.LogToFile($"PROVISIONALSPACE CHANGELOG: Creating ChangeLog for {targetSessions.Count} other sessions", "sync.log");
                
                var connector = CommandManager.Instance.Neo4jConnector;
                foreach (var targetSession in targetSessions)
                {
                    Task.Run(async () =>
                    {
                        try
                        {
                            await connector.CreateChangeLogEntryWithRelationshipsAsync(remoteElementId, "Modify", targetSession);
                            Logger.LogToFile($"PROVISIONALSPACE CHANGELOG: Created ChangeLog for remoteElementId={remoteElementId} in session {targetSession}", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"PROVISIONALSPACE CHANGELOG ERROR: Failed to create ChangeLog for remoteElementId={remoteElementId} in session {targetSession}: {ex.Message}", "sync.log");
                        }
                    });
                }
                
                // Update relationships after modification
                Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Updating relationships for modified ProvisionalSpace {space.Id}", "sync.log");
                UpdateProvisionalSpaceRelationships(space, doc);
                Logger.LogToFile($"PROVISIONALSPACE UPDATE COMPLETE: Successfully updated ProvisionalSpace {space.Id} and created ChangeLog entries", "sync.log");
            }
            else
            {
                Logger.LogToFile($"PROVISIONALSPACE UPDATE SKIPPED: No changes detected for ProvisionalSpace {space.Id}", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in UpdateProvisionalSpaceFromGraphProperties: {ex.Message}", ex);
        }
    }

    private void MarkProvisionalSpaceWithRemoteId(FamilyInstance space, int remoteElementId)
    {
        try
        {
            // Use unified tagging format for all elements (same as walls)
            var p = space.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
            if (p != null && !p.IsReadOnly)
            {
                var current = p.AsString();
                var tag = $"NeoCollab:ElementId={remoteElementId}";
                
                // CRITICAL FIX: Use target sessionId instead of current CommandManager sessionId
                var pullMarker = $"NeoCollab:PulledFrom={_currentTargetSessionId}";
                
                if (string.IsNullOrEmpty(current))
                {
                    p.Set($"{tag}; {pullMarker}");
                    Logger.LogToFile($"PROVISIONALSPACE MARK: Marked provisional space ElementId={space.Id} with unified tag={tag}; {pullMarker}", "sync.log");
                }
                else if (!current.Contains(tag))
                {
                    p.Set($"{current}; {tag}; {pullMarker}");
                    Logger.LogToFile($"PROVISIONALSPACE MARK: Added to existing tag - ElementId={space.Id} with {tag}; {pullMarker}", "sync.log");
                }
                else
                {
                    Logger.LogToFile($"PROVISIONALSPACE MARK: Tag already exists for ElementId={space.Id}, current tag={current}", "sync.log");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in MarkProvisionalSpaceWithRemoteId: {ex.Message}", ex);
        }
    }

    private void DeleteProvisionalSpaceByRemoteElementId(Document doc, int remoteElementId, Dictionary<string, object> properties)
    {
        try
        {
            FamilyInstance space = FindElementByRemoteElementId<FamilyInstance>(doc, remoteElementId);
            string remoteUid = GetStringProperty(properties, "elementUid", "uid", "guid");
            string remoteGuid = GetStringProperty(properties, "elementGuid", "guid", "ifcGuid");

            if (space == null && !string.IsNullOrEmpty(remoteUid))
            {
                space = FindElementByUid<FamilyInstance>(doc, remoteUid);
                if (space != null)
                {
                    Logger.LogToFile($"PROVISIONALSPACE DELETE: Found provisional space by UID {remoteUid}", "sync.log");
                }
            }

            if (space == null && !string.IsNullOrEmpty(remoteGuid))
            {
                space = FindElementByIfcGuid<FamilyInstance>(doc, remoteGuid);
                if (space != null)
                {
                    Logger.LogToFile($"PROVISIONALSPACE DELETE: Found provisional space by GUID {remoteGuid}", "sync.log");
                }
            }

            string solibriGuid = remoteGuid;
            if (space != null)
            {
                // Get provisional space GUID for Solibri deletion before deleting from Revit
                var spaceGuidParam = space.get_Parameter(BuiltInParameter.IFC_GUID);
                var spaceGuid = spaceGuidParam?.AsString();
                if (!string.IsNullOrEmpty(spaceGuid))
                {
                    solibriGuid = spaceGuid;
                }

                Logger.LogToFile($"PROVISIONALSPACE DELETE: Deleting provisional space ElementId={space.Id} for remoteElementId={remoteElementId}", "sync.log");
                doc.Delete(space.Id);
            }
            else
            {
                Logger.LogToFile($"PROVISIONALSPACE DELETE: Provisional space not found for remoteElementId={remoteElementId}", "sync.log");
            }

            if (!string.IsNullOrEmpty(solibriGuid))
            {
                try
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Deleting provisional space component from Solibri with GUID {solibriGuid}", "sync.log");

                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(new List<string> { solibriGuid });
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Successfully deleted provisional space component from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY ELEMENT ID: Failed to delete provisional space component from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY ELEMENT ID: Error initiating Solibri provisional space deletion: {ex.Message}", "sync.log");
                }
            }
            else
            {
                Logger.LogToFile($"DELETE BY ELEMENT ID: No GUID available to delete provisional space from Solibri (remoteElementId={remoteElementId})", "sync.log");
            }
        }
        catch (Exception ex)
        {
            Logger.LogCrash($"ERROR in DeleteProvisionalSpaceByRemoteElementId: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Deletes provisional spaces based on geographic properties (position, GUID) instead of ElementId
    /// </summary>
    private void DeleteProvisionalSpaceByGeographicProperties(Document doc, Dictionary<string, object> properties)
    {
        try
        {
            var remoteElementId = Convert.ToInt32(properties.GetValueOrDefault("elementId", -1));
            Logger.LogToFile($"DELETE BY GEOGRAPHY: Starting geographic deletion for provisional space with remoteElementId={remoteElementId}", "sync.log");

            // Validate that we have the minimum required geographic data for provisional spaces
            if (!properties.ContainsKey("guid") && !properties.ContainsKey("x") && !properties.ContainsKey("y") && !properties.ContainsKey("z"))
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: Missing geographic identification data (GUID or position) for provisional space, cannot delete geographically", "sync.log");
                return;
            }

            // Find all provisional spaces that match the geographic properties
            var matchingSpaces = FindProvisionalSpacesByGeographicProperties(doc, properties, remoteElementId);
            
            if (matchingSpaces.Count == 0)
            {
                Logger.LogToFile($"DELETE BY GEOGRAPHY: No provisional spaces found at the specified geographic location for remoteElementId={remoteElementId}", "sync.log");
                return;
            }

            // Delete all matching provisional spaces
            var deletedGuids = new List<string>();
            foreach (var space in matchingSpaces)
            {
                try
                {
                    var spaceTag = space.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    
                    // Get provisional space GUID for Solibri deletion
                    var spaceGuidParam = space.get_Parameter(BuiltInParameter.IFC_GUID);
                    var spaceGuid = spaceGuidParam?.AsString();
                    if (!string.IsNullOrEmpty(spaceGuid))
                    {
                        deletedGuids.Add(spaceGuid);
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Provisional space {space.Id} has GUID {spaceGuid} for Solibri deletion", "sync.log");
                    }
                    else
                    {
                        Logger.LogToFile($"DELETE BY GEOGRAPHY: Provisional space {space.Id} has no GUID, cannot delete from Solibri", "sync.log");
                    }
                    
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting provisional space {space.Id} with tag '{spaceTag ?? "no tag"}'", "sync.log");
                    
                    doc.Delete(space.Id);
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted provisional space {space.Id} based on geographic match", "sync.log");
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete provisional space {space.Id}: {ex.Message}", "sync.log");
                }
            }

            // Delete components from Solibri if we have GUIDs
            if (deletedGuids.Any())
            {
                try
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Deleting {deletedGuids.Count} provisional space components from Solibri: [{string.Join(", ", deletedGuids)}]", "sync.log");
                    
                    Task.Run(async () =>
                    {
                        try
                        {
                            await _solibriValidationService.DeleteComponentsAsync(deletedGuids);
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Successfully deleted {deletedGuids.Count} provisional space components from Solibri", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"DELETE BY GEOGRAPHY: Failed to delete provisional space components from Solibri: {ex.Message}", "sync.log");
                        }
                    });
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"DELETE BY GEOGRAPHY: Error initiating Solibri provisional space deletion: {ex.Message}", "sync.log");
                }
            }

            Logger.LogToFile($"DELETE BY GEOGRAPHY: Completed geographic deletion, deleted {matchingSpaces.Count} provisional spaces for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("DeleteProvisionalSpaceByGeographicProperties failed", ex);
        }
    }

    /// <summary>
    /// Finds provisional spaces that match the geographic properties (position, GUID, family name)
    /// </summary>
    private List<FamilyInstance> FindProvisionalSpacesByGeographicProperties(Document doc, Dictionary<string, object> properties, int remoteElementId)
    {
        var matchingSpaces = new List<FamilyInstance>();
        
        try
        {
            Logger.LogToFile($"FIND BY GEOGRAPHY: Searching for provisional spaces matching geographic properties (excluding original ElementId {remoteElementId})", "sync.log");
            
            var spaces = new FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_GenericModel).WhereElementIsNotElementType().Cast<FamilyInstance>();
            int checkedCount = 0;
            
            foreach (var space in spaces)
            {
                checkedCount++;
                
                // CRITICAL: Skip the original space with the same ElementId to prevent self-deletion
                if (space.Id.Value == remoteElementId)
                {
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Skipping original provisional space {space.Id} (same ElementId as remoteElementId)", "sync.log");
                    continue;
                }
                
                // Check if this space matches the geographic properties
                if (IsProvisionalSpaceGeographicallyIdentical(space, properties))
                {
                    var spaceTag = space.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                    Logger.LogToFile($"FIND BY GEOGRAPHY: Found matching provisional space {space.Id} with tag '{spaceTag ?? "no tag"}'", "sync.log");
                    matchingSpaces.Add(space);
                }
            }
            
            Logger.LogToFile($"FIND BY GEOGRAPHY: Checked {checkedCount} provisional spaces, found {matchingSpaces.Count} matches for remoteElementId={remoteElementId}", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogCrash("FindProvisionalSpacesByGeographicProperties failed", ex);
        }
        
        return matchingSpaces;
    }

    /// <summary>
    /// Checks if a provisional space is geographically identical to Neo4j properties
    /// </summary>
    private bool IsProvisionalSpaceGeographicallyIdentical(FamilyInstance space, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance
            
            // Check GUID first if available (most reliable identifier)
            if (properties.ContainsKey("guid"))
            {
                var neoGuid = properties["guid"].ToString();
                var existingGuid = space.UniqueId;
                if (existingGuid == neoGuid)
                {
                    Logger.LogToFile($"DELETE GEOGRAPHY: Provisional space {space.Id} matches by GUID", "sync.log");
                    return true;
                }
            }
            
            // Check family name match
            if (properties.ContainsKey("familyName"))
            {
                var neoFamilyName = properties["familyName"].ToString();
                var existingFamilyName = space.Symbol?.FamilyName;
                if (existingFamilyName != neoFamilyName)
                {
                    return false;
                }
            }
            
            // Check position if available
            if (properties.ContainsKey("x") && properties.ContainsKey("y") && properties.ContainsKey("z"))
            {
                var spaceLocation = (space.Location as LocationPoint)?.Point;
                if (spaceLocation != null)
                {
                    double neoX = ToFeetPreciseWithLogging(Convert.ToDouble(properties["x"]), "DeleteProvisionalSpaceGeographyX");
                    double neoY = ToFeetPreciseWithLogging(Convert.ToDouble(properties["y"]), "DeleteProvisionalSpaceGeographyY");
                    double neoZ = ToFeetPreciseWithLogging(Convert.ToDouble(properties["z"]), "DeleteProvisionalSpaceGeographyZ");
                    
                    if (Math.Abs(spaceLocation.X - neoX) >= TOLERANCE ||
                        Math.Abs(spaceLocation.Y - neoY) >= TOLERANCE ||
                        Math.Abs(spaceLocation.Z - neoZ) >= TOLERANCE)
                    {
                        return false;
                    }
                }
            }
            
            Logger.LogToFile($"DELETE GEOGRAPHY: Provisional space {space.Id} matches geographic properties", "sync.log");
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsProvisionalSpaceGeographicallyIdentical failed", ex);
            return false;
        }
    }

    #endregion

    #region Relationship Management

    /// <summary>
    /// Helper method to check if two bounding boxes intersect in 3D space
    /// </summary>
    private static bool Intersects(BoundingBoxXYZ a, BoundingBoxXYZ b)
    {
        return a.Min.X <= b.Max.X && a.Max.X >= b.Min.X &&
               a.Min.Y <= b.Max.Y && a.Max.Y >= b.Min.Y &&
               a.Min.Z <= b.Max.Z && a.Max.Z >= b.Min.Z;
    }

    /// <summary>
    /// Updates pipe relationships when a pipe is created or modified
    /// </summary>
    private void UpdatePipeRelationships(Pipe pipe, Document doc)
    {
        try
        {
            var bbPipe = pipe.get_BoundingBox(null);
            if (bbPipe == null) return;

            Logger.LogToFile($"PIPE RELATIONSHIPS: Updating relationships for pipe {pipe.Id}", "sync.log");

            var psCollector = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_GenericModel)
                .OfClass(typeof(FamilyInstance));

            int relationshipCount = 0;
            foreach (FamilyInstance ps in psCollector.Cast<FamilyInstance>())
            {
                if (!IsProvisionalSpaceByCategory(ps))
                    continue;

                var bbPs = ps.get_BoundingBox(null);
                if (bbPs == null) continue;

                bool intersects = Intersects(bbPipe, bbPs);
                string cypher;

                if (intersects)
                {
                    cypher = $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}}), (ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) MERGE (pi)-[:CONTAINED_IN]->(ps)";
                    relationshipCount++;
                    Logger.LogToFile($"PIPE RELATIONSHIPS: Creating CONTAINED_IN relationship between pipe {pipe.Id} and ProvisionalSpace {ps.Id}", "sync.log");
                }
                else
                {
                    cypher = $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}})-[r:CONTAINED_IN]->(ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) DELETE r";
                    Logger.LogToFile($"PIPE RELATIONSHIPS: Removing CONTAINED_IN relationship between pipe {pipe.Id} and ProvisionalSpace {ps.Id}", "sync.log");
                }

                _connector.RunWriteQueryAsync(cypher).Wait();
            }

            Logger.LogToFile($"PIPE RELATIONSHIPS: Updated relationships for pipe {pipe.Id}, created {relationshipCount} CONTAINED_IN relationships", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"PIPE RELATIONSHIPS ERROR: Failed to update relationships for pipe {pipe.Id}: {ex.Message}", "sync.log");
            Logger.LogCrash($"UpdatePipeRelationships failed for pipe {pipe.Id}", ex);
        }
    }

    /// <summary>
    /// Updates provisional space relationships when a provisional space is created or modified
    /// </summary>
    private void UpdateProvisionalSpaceRelationships(FamilyInstance ps, Document doc)
    {
        try
        {
            if (!IsProvisionalSpaceByCategory(ps))
                return;

            var bbPs = ps.get_BoundingBox(null);
            if (bbPs == null) return;

            Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Updating relationships for ProvisionalSpace {ps.Id}", "sync.log");

            var catFilter = new LogicalOrFilter(new List<ElementFilter>
            {
                new ElementCategoryFilter(BuiltInCategory.OST_PipeCurves),
                new ElementCategoryFilter(BuiltInCategory.OST_PipeSegments)
            });

            var pipes = new FilteredElementCollector(doc)
                .WherePasses(catFilter)
                .OfClass(typeof(Pipe));

            int relationshipCount = 0;
            foreach (Pipe pipe in pipes.Cast<Pipe>())
            {
                var bbPipe = pipe.get_BoundingBox(null);
                if (bbPipe == null) continue;

                bool intersects = Intersects(bbPipe, bbPs);
                string cypher;

                if (intersects)
                {
                    cypher = $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}}), (ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) MERGE (pi)-[:CONTAINED_IN]->(ps)";
                    relationshipCount++;
                    Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Creating CONTAINED_IN relationship between pipe {pipe.Id} and ProvisionalSpace {ps.Id}", "sync.log");
                }
                else
                {
                    cypher = $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}})-[r:CONTAINED_IN]->(ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) DELETE r";
                    Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Removing CONTAINED_IN relationship between pipe {pipe.Id} and ProvisionalSpace {ps.Id}", "sync.log");
                }

                _connector.RunWriteQueryAsync(cypher).Wait();
            }

            Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS: Updated relationships for ProvisionalSpace {ps.Id}, created {relationshipCount} CONTAINED_IN relationships", "sync.log");
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"PROVISIONALSPACE RELATIONSHIPS ERROR: Failed to update relationships for ProvisionalSpace {ps.Id}: {ex.Message}", "sync.log");
            Logger.LogCrash($"UpdateProvisionalSpaceRelationships failed for ProvisionalSpace {ps.Id}", ex);
        }
    }

    /// <summary>
    /// Helper method to check if a FamilyInstance is a ProvisionalSpace
    /// Uses category-based detection as fallback
    /// </summary>
    private bool IsProvisionalSpaceByCategory(FamilyInstance inst)
    {
        if (inst.Category?.Id.Value != (int)BuiltInCategory.OST_GenericModel)
            return false;

        // Try ParameterUtils first if available
        try
        {
            return ParameterUtils.IsProvisionalSpace(inst);
        }
        catch
        {
            // Fallback: check by family name pattern
            var familyName = inst.Symbol?.FamilyName ?? string.Empty;
            return familyName.Contains("Prov", StringComparison.OrdinalIgnoreCase) || 
                   familyName.Contains("Space", StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// Checks if a new ProvisionalSpace would overlap with existing ones
    /// </summary>
    private bool CheckForOverlappingProvisionalSpaces(Document doc, XYZ location, double widthMeters, double heightMeters, double thicknessMeters)
    {
        try
        {
            Logger.LogToFile($"OVERLAP CHECK: Checking for overlaps at ({location.X:F6}, {location.Y:F6}, {location.Z:F6})ft", "sync.log");
            
            // Convert dimensions to feet for bounding box calculation
            double widthFeet = UnitConversion.FromMeters(widthMeters);
            double heightFeet = UnitConversion.FromMeters(heightMeters);  
            double thicknessFeet = UnitConversion.FromMeters(thicknessMeters);
            
            // Create bounding box for new ProvisionalSpace (assuming location is MIN point)
            XYZ minPoint = location;
            XYZ maxPoint = new XYZ(
                location.X + widthFeet,
                location.Y + thicknessFeet,  // thickness is Y-dimension
                location.Z + heightFeet      // height is Z-dimension
            );
            
            Logger.LogToFile($"OVERLAP CHECK: New space bounds Min({minPoint.X:F6}, {minPoint.Y:F6}, {minPoint.Z:F6}) Max({maxPoint.X:F6}, {maxPoint.Y:F6}, {maxPoint.Z:F6})", "sync.log");
            
            // Get all existing ProvisionalSpaces
            var existingSpaces = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_GenericModel)
                .OfClass(typeof(FamilyInstance))
                .Cast<FamilyInstance>()
                .Where(fi => ParameterUtils.IsProvisionalSpace(fi))
                .ToList();
                
            Logger.LogToFile($"OVERLAP CHECK: Found {existingSpaces.Count} existing ProvisionalSpaces to check", "sync.log");
            
            foreach (var existingSpace in existingSpaces)
            {
                var existingBounds = existingSpace.get_BoundingBox(null);
                if (existingBounds == null) continue;
                
                Logger.LogToFile($"OVERLAP CHECK: Existing space {existingSpace.Id} bounds Min({existingBounds.Min.X:F6}, {existingBounds.Min.Y:F6}, {existingBounds.Min.Z:F6}) Max({existingBounds.Max.X:F6}, {existingBounds.Max.Y:F6}, {existingBounds.Max.Z:F6})", "sync.log");
                
                // Check for 3D overlap
                bool xOverlap = !(maxPoint.X < existingBounds.Min.X || minPoint.X > existingBounds.Max.X);
                bool yOverlap = !(maxPoint.Y < existingBounds.Min.Y || minPoint.Y > existingBounds.Max.Y);
                bool zOverlap = !(maxPoint.Z < existingBounds.Min.Z || minPoint.Z > existingBounds.Max.Z);
                
                if (xOverlap && yOverlap && zOverlap)
                {
                    Logger.LogToFile($"OVERLAP CHECK: OVERLAP DETECTED with existing space {existingSpace.Id}", "sync.log");
                    return true;
                }
            }
            
            Logger.LogToFile($"OVERLAP CHECK: No overlaps detected", "sync.log");
            return false;
        }
        catch (Exception ex)
        {
            Logger.LogToFile($"OVERLAP CHECK ERROR: {ex.Message}", "sync.log");
            return false; // Continue creation if overlap check fails
        }
    }

    /// <summary>
    /// Checks if an existing ProvisionalSpace is identical to Neo4j data to prevent unnecessary updates
    /// </summary>
    private bool IsProvisionalSpaceIdentical(FamilyInstance existingSpace, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance for coordinates
            
            Logger.LogToFile($"IDENTITY CHECK: Checking if ProvisionalSpace {existingSpace.Id} is identical to Neo4j data", "sync.log");
            
            // Get existing space location
            var location = existingSpace.Location as LocationPoint;
            if (location?.Point == null)
            {
                Logger.LogToFile($"IDENTITY CHECK: Cannot get location for space {existingSpace.Id}", "sync.log");
                return false;
            }
            
            var existingPoint = location.Point;
            
            // Convert Neo4j coordinates to feet
            double neoX = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x", 0.0)), "IdentityCheckX");
            double neoY = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y", 0.0)), "IdentityCheckY");
            double neoZ = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z", 0.0)), "IdentityCheckZ");
            
            // Check coordinates
            bool coordsMatch = Math.Abs(existingPoint.X - neoX) < TOLERANCE &&
                              Math.Abs(existingPoint.Y - neoY) < TOLERANCE &&
                              Math.Abs(existingPoint.Z - neoZ) < TOLERANCE;
            
            Logger.LogToFile($"IDENTITY CHECK: Coordinates - Existing({existingPoint.X:F6}, {existingPoint.Y:F6}, {existingPoint.Z:F6}) vs Neo4j({neoX:F6}, {neoY:F6}, {neoZ:F6}) - Match: {coordsMatch}", "sync.log");
            
            if (!coordsMatch)
            {
                Logger.LogToFile($"IDENTITY CHECK: Coordinates differ - NOT identical", "sync.log");
                return false;
            }
            
            // Check dimensions (width, height, thickness)
            double neoWidth = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("width", 0.0)), "IdentityCheckWidth");
            double neoHeight = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("height", 0.0)), "IdentityCheckHeight");
            double neoThickness = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("thickness", 0.0)), "IdentityCheckThickness");
            
            // Get existing dimensions
            var widthParam = existingSpace.LookupParameter("Width") ?? existingSpace.LookupParameter("width");
            var heightParam = existingSpace.LookupParameter("Height") ?? existingSpace.LookupParameter("height");
            
            bool dimensionsMatch = true;
            
            if (widthParam != null && !widthParam.IsReadOnly)
            {
                double existingWidth = widthParam.AsDouble();
                if (Math.Abs(existingWidth - neoWidth) >= TOLERANCE)
                {
                    Logger.LogToFile($"IDENTITY CHECK: Width differs - Existing: {existingWidth:F6} vs Neo4j: {neoWidth:F6}", "sync.log");
                    dimensionsMatch = false;
                }
            }
            
            if (heightParam != null && !heightParam.IsReadOnly)
            {
                double existingHeight = heightParam.AsDouble();
                if (Math.Abs(existingHeight - neoHeight) >= TOLERANCE)
                {
                    Logger.LogToFile($"IDENTITY CHECK: Height differs - Existing: {existingHeight:F6} vs Neo4j: {neoHeight:F6}", "sync.log");
                    dimensionsMatch = false;
                }
            }
            
            Logger.LogToFile($"IDENTITY CHECK: Result - Coordinates: {coordsMatch}, Dimensions: {dimensionsMatch}, Overall: {coordsMatch && dimensionsMatch}", "sync.log");
            
            return coordsMatch && dimensionsMatch;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsProvisionalSpaceIdentical", ex);
            return false;
        }
    }

    /// <summary>
    /// Checks if an existing Wall is identical to Neo4j data to prevent unnecessary updates
    /// </summary>
    private bool IsWallIdentical(Wall existingWall, Dictionary<string, object> properties)
    {
        try
        {
            const double TOLERANCE = 0.001; // 1mm tolerance for coordinates
            
            Logger.LogToFile($"WALLDUP: IsWallIdentical checking Wall {existingWall.Id} vs Neo4j data", "sync.log");
            Logger.LogToFile($"WALL IDENTITY CHECK: Checking if Wall {existingWall.Id} is identical to Neo4j data", "sync.log");
            
            // Get existing wall location curve
            var locationCurve = existingWall.Location as LocationCurve;
            if (locationCurve?.Curve == null)
            {
                Logger.LogToFile($"WALL IDENTITY CHECK: Cannot get location curve for wall {existingWall.Id}", "sync.log");
                return false;
            }
            
            var line = locationCurve.Curve as Line;
            if (line == null)
            {
                Logger.LogToFile($"WALL IDENTITY CHECK: Wall {existingWall.Id} is not a straight line", "sync.log");
                return false;
            }
            
            var existingStart = line.GetEndPoint(0);
            var existingEnd = line.GetEndPoint(1);
            
            // Convert Neo4j coordinates to feet
            double neoX1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x1", 0.0)), "WallIdentityCheckX1");
            double neoY1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y1", 0.0)), "WallIdentityCheckY1");
            double neoZ1 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z1", 0.0)), "WallIdentityCheckZ1");
            double neoX2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("x2", 0.0)), "WallIdentityCheckX2");
            double neoY2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("y2", 0.0)), "WallIdentityCheckY2");
            double neoZ2 = ToFeetPreciseWithLogging(Convert.ToDouble(properties.GetValueOrDefault("z2", 0.0)), "WallIdentityCheckZ2");
            
            // Check if geometry matches (either direction)
            bool geometryMatch1 = Math.Abs(existingStart.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ1) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ2) < TOLERANCE;
            
            bool geometryMatch2 = Math.Abs(existingStart.X - neoX2) < TOLERANCE &&
                                 Math.Abs(existingStart.Y - neoY2) < TOLERANCE &&
                                 Math.Abs(existingStart.Z - neoZ2) < TOLERANCE &&
                                 Math.Abs(existingEnd.X - neoX1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Y - neoY1) < TOLERANCE &&
                                 Math.Abs(existingEnd.Z - neoZ1) < TOLERANCE;
            
            bool geometryMatches = geometryMatch1 || geometryMatch2;
            
            Logger.LogToFile($"WALL IDENTITY CHECK: Geometry - Existing({existingStart.X:F6}, {existingStart.Y:F6}, {existingStart.Z:F6})-({existingEnd.X:F6}, {existingEnd.Y:F6}, {existingEnd.Z:F6}) vs Neo4j({neoX1:F6}, {neoY1:F6}, {neoZ1:F6})-({neoX2:F6}, {neoY2:F6}, {neoZ2:F6}) - Match: {geometryMatches}", "sync.log");
            
            if (!geometryMatches)
            {
                Logger.LogToFile($"WALLDUP: Geometry differs - NOT identical", "sync.log");
                Logger.LogToFile($"WALL IDENTITY CHECK: Geometry differs - NOT identical", "sync.log");
                return false;
            }
            
            // Check wall type/thickness if available
            if (properties.ContainsKey("thickness_m"))
            {
                double neoThickness = ToFeetPreciseWithLogging(Convert.ToDouble(properties["thickness_m"]), "WallIdentityCheckThickness");
                double existingThickness = existingWall.Width;
                
                if (Math.Abs(existingThickness - neoThickness) >= TOLERANCE)
                {
                    Logger.LogToFile($"WALL IDENTITY CHECK: Thickness differs - Existing: {existingThickness:F6} vs Neo4j: {neoThickness:F6}", "sync.log");
                    return false;
                }
            }
            
            // Check height if available
            if (properties.ContainsKey("height") || properties.ContainsKey("height_mm"))
            {
                var heightParam = existingWall.get_Parameter(BuiltInParameter.WALL_USER_HEIGHT_PARAM);
                if (heightParam != null)
                {
                    double existingHeight = heightParam.AsDouble();
                    double neoHeight = properties.ContainsKey("height") ? 
                        ToFeetPreciseWithLogging(Convert.ToDouble(properties["height"]), "WallIdentityCheckHeight") :
                        ToFeetPreciseWithLogging(Convert.ToDouble(properties["height_mm"]) / 1000.0, "WallIdentityCheckHeightMm");
                    
                    if (Math.Abs(existingHeight - neoHeight) >= TOLERANCE)
                    {
                        Logger.LogToFile($"WALL IDENTITY CHECK: Height differs - Existing: {existingHeight:F6} vs Neo4j: {neoHeight:F6}", "sync.log");
                        return false;
                    }
                }
            }
            
            Logger.LogToFile($"WALLDUP: Wall {existingWall.Id} is IDENTICAL to Neo4j data", "sync.log");
            Logger.LogToFile($"WALL IDENTITY CHECK: Wall is IDENTICAL to Neo4j data", "sync.log");
            return true;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("IsWallIdentical", ex);
            return false;
        }
    }

    /// <summary>
    /// Finds a wall that is identical to the Neo4j properties (but NOT the original wall with same ElementId)
    /// This prevents creation of duplicate walls at the same location
    /// </summary>
    private Wall FindIdenticalWall(Document doc, Dictionary<string, object> properties, int remoteElementId)
    {
        try
        {
            Logger.LogToFile($"WALLDUP: FindIdenticalWall searching for identical wall to remoteElementId={remoteElementId} (excluding original and tagged)", "sync.log");
            Logger.LogToFile($"WALL DUPLICATE SEARCH: Looking for identical wall to remoteElementId={remoteElementId} (excluding original)", "sync.log");
            
            var walls = new FilteredElementCollector(doc).OfClass(typeof(Wall)).Cast<Wall>();
            
            // CRITICAL FIX: Skip the original wall with the same ElementId
            // We only want to find OTHER walls at the same location
            foreach (var wall in walls)
            {
                // Skip the original wall - we don't want to "duplicate detect" the original wall itself
                if (wall.Id.Value == remoteElementId)
                {
                    Logger.LogToFile($"WALLDUP: Skipping original wall {wall.Id} (same ElementId as remoteElementId)", "sync.log");
                    Logger.LogToFile($"WALL DUPLICATE SEARCH: Skipping original wall {wall.Id} (same ElementId as remoteElementId)", "sync.log");
                    continue;
                }
                
                // Skip walls that are already tagged with ANY remote IDs (including this one)
                // We only want to find untagged walls that happen to be at the same location
                var tag = wall.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS)?.AsString();
                if (!string.IsNullOrEmpty(tag) && tag.Contains("NeoCollab:ElementId="))
                {
                    Logger.LogToFile($"WALLDUP: Skipping already tagged wall {wall.Id} with tag '{tag}'", "sync.log");
                    Logger.LogToFile($"WALL DUPLICATE SEARCH: Skipping already tagged wall {wall.Id} with tag '{tag}'", "sync.log");
                    continue; // This wall already belongs to a remote element (any remote element)
                }
                
                if (IsWallIdentical(wall, properties))
                {
                    Logger.LogToFile($"WALLDUP: FOUND identical untagged wall {wall.Id} for remoteElementId={remoteElementId}", "sync.log");
                    Logger.LogToFile($"WALL DUPLICATE SEARCH: Found identical wall by geometry - Wall {wall.Id} (NOT the original)", "sync.log");
                    return wall;
                }
            }
            
            Logger.LogToFile($"WALLDUP: NO identical untagged wall found for remoteElementId={remoteElementId}", "sync.log");
            Logger.LogToFile($"WALL DUPLICATE SEARCH: No identical wall found for remoteElementId={remoteElementId}", "sync.log");
            return null;
        }
        catch (Exception ex)
        {
            Logger.LogCrash("FindIdenticalWall", ex);
            return null;
        }
    }

    #endregion

}
