using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using Autodesk.Revit.DB.Architecture;
using Autodesk.Revit.DB.Plumbing;
using Autodesk.Revit.DB.Events;
using System.Linq;
using System.Globalization;
using System.Text.RegularExpressions;
using Autodesk.Revit.UI;



namespace NeoCollab
{
    public class SpaceExtractor
    {


        private readonly CommandManager _cmdManager;
        // Separate Logdatei f�r ProvisionalSpaces
        private const string ProvLog = "provisional_spaces.log";

        private static int _wallCounter = 0;
        private readonly Dictionary<(string baseLevel, string topLevel), int> _stairCounters
          = new Dictionary<(string baseLevel, string topLevel), int>();

        private string GenerateStairName(string baseLevelName, string topLevelName)
        {
            var key = (baseLevelName, topLevelName);
            if (!_stairCounters.TryGetValue(key, out var count))
            {
                count = 0;
            }
            count++;
            _stairCounters[key] = count;
            return $"Treppe {baseLevelName} {topLevelName} {count}";
        }
        // Konstruktor, erh�lt den CommandManager zum Einreihen von Cypher-Befehlen.
        public SpaceExtractor(CommandManager cmdManager)
        {

            _cmdManager = cmdManager;
        }

        private void ProcessWalls(Document doc, Level level)
        {
            var wallFilter = new ElementLevelFilter(level.Id);
            var collector = new FilteredElementCollector(doc).OfClass(typeof(Wall)).WherePasses(wallFilter);

            foreach (Wall wall in collector)
            {
                ProcessWall(wall, doc);
            }
        }



        private void ProcessWall(Element wall, Document doc)
        {
            if (wall.LevelId == ElementId.InvalidElementId) return;
            try
            {
                Dictionary<string, object> data = WallSerializer.ToNode((Wall)wall);
                var inv = CultureInfo.InvariantCulture;
                var setParts = new List<string>
                {
                    $"w.uid = '{ParameterUtils.EscapeForCypher(data["uid"].ToString())}'",
                    $"w.elementId = {wall.Id.Value}",
                    $"w.typeId = {data["typeId"]}",
                    $"w.typeName = '{ParameterUtils.EscapeForCypher(data["typeName"].ToString())}'",
                    $"w.familyName = '{ParameterUtils.EscapeForCypher(data["familyName"].ToString())}'",
                    $"w.Name = '{ParameterUtils.EscapeForCypher(data["Name"].ToString())}'",
                    $"w.levelId = {data["levelId"]}",
                    $"w.x1 = {((double)data["x1"]).ToString(inv)}",
                    $"w.y1 = {((double)data["y1"]).ToString(inv)}",
                    $"w.z1 = {((double)data["z1"]).ToString(inv)}",
                    $"w.x2 = {((double)data["x2"]).ToString(inv)}",
                    $"w.y2 = {((double)data["y2"]).ToString(inv)}",
                    $"w.z2 = {((double)data["z2"]).ToString(inv)}",
                    $"w.height_mm = {((double)data["height_mm"]).ToString(inv)}",
                    $"w.thickness_mm = {((double)data["thickness_mm"]).ToString(inv)}",
                    $"w.structural = {data["structural"]}",
                    $"w.flipped = {data["flipped"]}",
                    $"w.base_offset_mm = {((double)data["base_offset_mm"]).ToString(inv)}",
                    $"w.location_line = {data["location_line"]}",
                    $"w.user = '{ParameterUtils.EscapeForCypher(data["user"].ToString())}'",
                    $"w.created = datetime('{((DateTime)data["created"]).ToString("o")}')",
                    $"w.modified = datetime('{((DateTime)data["modified"]).ToString("o")}')",
                    $"w.lastModifiedUtc = datetime('{((DateTime)data["modified"]).ToString("o")}')"

                };

                // CRITICAL FIX: Use level Name instead of elementId to prevent duplicates across sessions
                var level = wall.Document.GetElement(wall.LevelId) as Level;
                string levelName = level?.Name ?? "Unknown Level";
                string levelNameEsc = ParameterUtils.EscapeForCypher(levelName);
                
                // Use MERGE with Name for Level to avoid duplicates and ensure existence
                string cy =
                  $"MERGE (l:Level {{Name: \"{levelNameEsc}\"}}) " +
                  $"ON CREATE SET l.elementId = {wall.LevelId.Value} " +
                  $"ON MATCH SET l.elementId = {wall.LevelId.Value} " +
                  $"WITH l " +
                  $"MERGE (w:Wall {{elementId: {wall.Id.Value}}}) SET {string.Join(", ", setParts)} MERGE (l)-[:CONTAINS]->(w)";


                _cmdManager.cypherCommands.Enqueue(cy);
                Debug.WriteLine("[Neo4j] Created Wall node: " + cy);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Wall Processing Error] {ex.Message}");
            }
        }

        private void ProcessDoor(Element door, Document doc)
        {
            if (door.Category?.Id.Value != (int)BuiltInCategory.OST_Doors)
                return;
            try
            {
                // 1. Neo4j Cypher-Query
                string doorName = door.get_Parameter(BuiltInParameter.DOOR_NUMBER)?.AsString() ?? "Unbenannt";
                FamilyInstance doorInstance = door as FamilyInstance;
                Element hostWall = doorInstance?.Host;
                var sym = doc.GetElement(door.GetTypeId()) as FamilySymbol;
                Dictionary<string, object> data = doorInstance != null ? DoorSerializer.ToNode(doorInstance) : new();
                var inv = CultureInfo.InvariantCulture;
                var setParts = new List<string>
                {
                    // 1) T�r mit Wand und Level verkn�pfen
                    $"d.uid = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("uid", door.UniqueId).ToString())}'",
                    $"d.elementId = {door.Id.Value}",
                    $"d.name = '{ParameterUtils.EscapeForCypher(doorName)}'",
                    $"d.typeId = {door.GetTypeId().Value}",
                    $"d.familyName = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("familyName", string.Empty).ToString())}'",
                    $"d.symbolName = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("symbolName", string.Empty).ToString())}'",
                    $"d.levelId = {door.LevelId.Value}",
                    $"d.hostId = {doorInstance?.Host?.Id.Value ?? -1}",
                    $"d.hostUid = '{ParameterUtils.EscapeForCypher(doorInstance?.Host?.UniqueId ?? string.Empty)}'",
                    $"d.x = {((double)data.GetValueOrDefault("x", 0.0)).ToString(inv)}",
                    $"d.y = {((double)data.GetValueOrDefault("y", 0.0)).ToString(inv)}",
                    $"d.z = {((double)data.GetValueOrDefault("z", 0.0)).ToString(inv)}",
                    $"d.rotation = {((double)data.GetValueOrDefault("rotation", 0.0)).ToString(inv)}",
                    $"d.width = {((double)data.GetValueOrDefault("width", 0.0)).ToString(inv)}",
                    $"d.height = {((double)data.GetValueOrDefault("height", 0.0)).ToString(inv)}",
                    $"d.thickness = {((double)data.GetValueOrDefault("thickness", 0.0)).ToString(inv)}",
$"d.user = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("user", CommandManager.Instance.SessionId).ToString())}'",                    $"d.created = datetime('{((DateTime)data.GetValueOrDefault("created", DateTime.UtcNow)).ToString("o")}')",
                    $"d.modified = datetime('{((DateTime)data.GetValueOrDefault("modified", DateTime.UtcNow)).ToString("o")}')"
                };

                // CRITICAL FIX: Use level Name instead of elementId to prevent duplicates across sessions
                var level = door.Document.GetElement(door.LevelId) as Level;
                string levelName = level?.Name ?? "Unknown Level";
                string levelNameEsc = ParameterUtils.EscapeForCypher(levelName);
                
                // Use separate MERGE statements for Level and Wall to avoid syntax errors
                string cyBase = $"MERGE (l:Level {{Name: \"{levelNameEsc}\"}}) " +
                               $"ON CREATE SET l.elementId = {door.LevelId.Value} " +
                               $"ON MATCH SET l.elementId = {door.LevelId.Value}";
                if (hostWall != null)
                    cyBase += $" WITH l MERGE (w:Wall {{elementId: {hostWall.Id.Value}}})";
                string cyNode =
                    $"{cyBase} MERGE (d:Door {{elementId: {door.Id.Value}}}) SET {string.Join(", ", setParts)}";
                if (hostWall != null)
                    cyNode += " MERGE (l)-[:CONTAINS]->(d) MERGE (d)-[:INSTALLED_IN]->(w)";
                else
                    cyNode += " WITH l MERGE (l)-[:CONTAINS]->(d)";

                _cmdManager.cypherCommands.Enqueue(cyNode);
                Debug.WriteLine("[Neo4j] Created Door node: " + cyNode);

                // Set NeoCollab tag for local Door (matches pull logic)
                try
                {
                    var tag = $"NeoCollab:ElementId={door.Id.Value}";
                    var commentParam = door.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
                    if (commentParam != null && !commentParam.IsReadOnly)
                    {
                        commentParam.Set(tag);
                    }
                }
                catch (Exception)
                {
                    // Tag setting failed - non-critical
                }

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Door Processing Error] {ex.Message}");
            }
        }

       

        private void ProcessProvisionalSpace(FamilyInstance inst, Document doc)
        {
            try
            {
                bool isProv = ParameterUtils.IsProvisionalSpace(inst);
                if (!isProv)
                {
                    return;
                }
                var host = inst.Host as Wall;
                if (host == null)
                {
                    BoundingBoxXYZ bb = inst.get_BoundingBox(null);
                    if (bb != null)
                    {
                        Outline outl = new Outline(bb.Min, bb.Max);
                        ElementFilter bbfilter = new BoundingBoxIntersectsFilter(outl);
                        host = new FilteredElementCollector(doc)
                            .OfClass(typeof(Wall))
                            .WherePasses(bbfilter)
                            .Cast<Wall>()
                            .FirstOrDefault();
                    }
                }
                var node = ProvisionalSpaceSerializer.ToProvisionalSpaceNode(inst, out var data);
                Logger.LogToFile($"Serialized data for {inst.UniqueId}", ProvLog);
                
                // DEBUG: Log elementId to identify -1 issues
                var elementId = data["elementId"];
                Logger.LogToFile($"PROVISIONAL SPACE ELEMENT ID: {elementId} for instance {inst.UniqueId} (Revit ID: {inst.Id.Value})", "sync.log");
                if (elementId.Equals(-1) || elementId.ToString() == "-1")
                {
                    Logger.LogToFile($"WARNING: ProvisionalSpace has elementId = -1! Instance: {inst.UniqueId}, Revit ID: {inst.Id.Value}", "sync.log");
                }

                var inv = CultureInfo.InvariantCulture;
                var setParts = new List<string>
                {
                    $"p.name = '{ParameterUtils.EscapeForCypher(data["name"].ToString())}'",
                    $"p.width = {((double)data["width"]).ToString(inv)}",
                    $"p.height = {((double)data["height"]).ToString(inv)}",
                    $"p.thickness = {((double)data["thickness"]).ToString(inv)}",
                    $"p.level = '{ParameterUtils.EscapeForCypher(data["level"].ToString())}'",
                    $"p.x = {((double)data["x"]).ToString(inv)}",
                    $"p.y = {((double)data["y"]).ToString(inv)}",
                    $"p.z = {((double)data["z"]).ToString(inv)}",
                    $"p.rotation = {((double)data["rotation"]).ToString(inv)}",
                    $"p.hostId = {data["hostId"]}",
                    $"p.elementId = {data["elementId"]}",  // FIXED: Changed from revitId to elementId for consistency
                    $"p.ifcType = '{ParameterUtils.EscapeForCypher(data["ifcType"].ToString())}'",
                    $"p.familyName = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("familyName", "").ToString())}'",
                    $"p.symbolName = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("symbolName", "").ToString())}'",
                    $"p.category = '{ParameterUtils.EscapeForCypher(data.GetValueOrDefault("category", "").ToString())}'",
                    $"p.phaseCreated = {data.GetValueOrDefault("phaseCreated", -1)}",
                    $"p.phaseDemolished = {data.GetValueOrDefault("phaseDemolished", -1)}",
                    $"p.bbMinX = {((double)data.GetValueOrDefault("bbMinX", 0.0)).ToString(inv)}",
                    $"p.bbMinY = {((double)data.GetValueOrDefault("bbMinY", 0.0)).ToString(inv)}",
                    $"p.bbMinZ = {((double)data.GetValueOrDefault("bbMinZ", 0.0)).ToString(inv)}",
                    $"p.bbMaxX = {((double)data.GetValueOrDefault("bbMaxX", 0.0)).ToString(inv)}",
                    $"p.bbMaxY = {((double)data.GetValueOrDefault("bbMaxY", 0.0)).ToString(inv)}",
                    $"p.bbMaxZ = {((double)data.GetValueOrDefault("bbMaxZ", 0.0)).ToString(inv)}",
                    $"p.uid = '{ParameterUtils.EscapeForCypher(inst.UniqueId)}'",
                    $"p.typeId = {inst.GetTypeId().Value}",
                    $"p.created = datetime('{((DateTime)data["created"]).ToString("o")}')",
                    $"p.modified = datetime('{((DateTime)data["modified"]).ToString("o")}')",
                    $"p.user = '{ParameterUtils.EscapeForCypher(data["user"].ToString())}'"
                };

                string cyNode =
                    $"MERGE (p:ProvisionalSpace {{guid:'{data["guid"]}'}}) " +
                          $"SET {string.Join(", ", setParts)}";
                _cmdManager.cypherCommands.Enqueue(cyNode);
                if (host != null)
                {
                    string cyRel =
                        $"MATCH (w:Wall {{elementId:{host.Id.Value}}}), (p:ProvisionalSpace {{guid:'{data["guid"]}'}}) " +
                        "MERGE (w)-[:HAS_PROV_SPACE]->(p)";
                    _cmdManager.cypherCommands.Enqueue(cyRel);
                    Debug.WriteLine("[Neo4j] Created ProvisionalSpace relation: " + cyRel);
                }

                Debug.WriteLine("[Neo4j] Created ProvisionalSpace node: " + cyNode);
                
                // Set NeoCollab tag for local ProvisionalSpace (matches pull logic)
                try
                {
                    var tag = $"NeoCollab:ElementId={inst.Id.Value}";
                    var commentParam = inst.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
                    if (commentParam != null && !commentParam.IsReadOnly)
                    {
                        commentParam.Set(tag);
                    }
                }
                catch (Exception)
                {
                    // Tag setting failed - non-critical
                }
                
                UpdateProvisionalSpaceRelations(inst, doc);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[ProvisionalSpace Error] {ex.Message}");
                Logger.LogCrash("ProcessProvisionalSpace", ex);
            }
        }

        private void ProcessPipe(MEPCurve pipe, Document doc)
        {
            try
            {
                var data = PipeSerializer.ToNode(pipe);
                var inv = CultureInfo.InvariantCulture;

                string cyNode =
                    $"MERGE (p:Pipe {{uid:'{data["uid"]}'}}) " +
                    $"SET p.elementId = {data["elementId"]}, " +
                    $"p.typeId = {data["typeId"]}, " +
                    $"p.systemTypeId = {data["systemTypeId"]}, " +
                    $"p.levelId = {data["levelId"]}, " +
                    $"p.x1 = {((double)data["x1"]).ToString(inv)}, p.y1 = {((double)data["y1"]).ToString(inv)}, p.z1 = {((double)data["z1"]).ToString(inv)}, " +
                    $"p.x2 = {((double)data["x2"]).ToString(inv)}, p.y2 = {((double)data["y2"]).ToString(inv)}, p.z2 = {((double)data["z2"]).ToString(inv)}, " +
  $"p.diameter = {((double)data["diameter"]).ToString(inv)}, " +
                    $"p.createdBy = coalesce(p.createdBy,'{ParameterUtils.EscapeForCypher(data["user"].ToString())}'), " +
                    $"p.createdAt = coalesce(p.createdAt, datetime('{((DateTime)data["created"]).ToString("o")}')), " +
                    $"p.lastModifiedUtc = datetime('{((DateTime)data["modified"]).ToString("o")}')"; _cmdManager.cypherCommands.Enqueue(cyNode);
                Debug.WriteLine("[Neo4j] Cypher erzeugt (Pipe Node): " + cyNode);

                // Set NeoCollab tag for local Pipe (matches pull logic)
                try
                {
                    var tag = $"NeoCollab:ElementId={pipe.Id.Value}";
                    var commentParam = pipe.get_Parameter(BuiltInParameter.ALL_MODEL_INSTANCE_COMMENTS);
                    if (commentParam != null && !commentParam.IsReadOnly)
                    {
                        commentParam.Set(tag);
                    }
                }
                catch (Exception)
                {
                    // Tag setting failed - non-critical
                }

                UpdatePipeRelations(pipe, doc);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Pipe Processing Error] {ex.Message}");
            }
        }

        private void ProcessRoom(Element room, Document doc)
        {
            if (room.LevelId == ElementId.InvalidElementId) return;
            try
            {
                // 1. Neo4j Cypher-Query
                string room_Name = room.get_Parameter(BuiltInParameter.ROOM_NAME)?.AsString() ?? "Unbenannt";
                string roomName = string.IsNullOrWhiteSpace(room_Name) ? $"Unnamed_{room.Id}" : room_Name;
                string levelName = doc.GetElement(room.LevelId)?.Name ?? "Unbekannt";
                string levelNameEsc = ParameterUtils.EscapeForCypher(levelName);

                // CRITICAL FIX: Use level Name instead of elementId to prevent duplicates across sessions
                string cy = $"MERGE (r:Room {{elementId: {room.Id.Value}}}) SET r.Name = '{ParameterUtils.EscapeForCypher(roomName)}', r.Level = '{ParameterUtils.EscapeForCypher(levelName)}' " +
                           $"WITH r " +
                           $"MERGE (l:Level {{Name: \"{levelNameEsc}\"}}) " +
                           $"ON CREATE SET l.elementId = {room.LevelId.Value} " +
                           $"ON MATCH SET l.elementId = {room.LevelId.Value} " +
                           $"WITH r, l " +
                           $"MERGE (l)-[:CONTAINS]->(r)";

                _cmdManager.cypherCommands.Enqueue(cy);
                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[Room Processing Error] {ex.Message}");
            }
        }
        // Liest alle relevanten Elemente aus dem Dokument und erzeugt erste Neo4j-Knoten.
        public void CreateInitialGraph(Document doc)
        {
            Logger.LogToFile($"CREATE INITIAL GRAPH: Starting for document with {CommandManager.Instance.SessionId}", "sync.log");

            // create stopwatch to measure the elapsed time
            Stopwatch timer = new Stopwatch();
            timer.Start();
            Debug.WriteLine("#--------#\nTimer started.\n#--------#");

            string buildingName = "Teststra�e 21";
            string buildingNameEsc = ParameterUtils.EscapeForCypher(buildingName);
            // CRITICAL FIX: Use Name as primary identifier to prevent duplicates across sessions
            string cyBuilding = $"MERGE (b:Building {{Name: \"{buildingNameEsc}\"}}) " +
                               $"ON CREATE SET b.elementId = 1 " +
                               $"ON MATCH SET b.elementId = 1";
            _cmdManager.cypherCommands.Enqueue(cyBuilding);
            Debug.WriteLine("[Neo4j] Cypher erzeugt (Building): " + cyBuilding);
            
            // Note: ChangeLog for Building will be created later with all other elements

            // 1. Alle Level einlesen

            // Get all level
            FilteredElementCollector collector = new FilteredElementCollector(doc);
            IList<Level> levels = collector.OfClass(typeof(Level)).Cast<Level>().ToList();
            Logger.LogToFile($"CREATE INITIAL GRAPH: Processing {levels.Count} levels", "sync.log");

            // Iterate over all level
            foreach (Level lvl in levels)
            {
                Debug.WriteLine($"Level: {lvl.Name}, ID: {lvl.Id}");
                string levelName = ParameterUtils.EscapeForCypher(lvl.Name);
                
                // CRITICAL FIX: Use Name as primary identifier to prevent duplicates across sessions
                // Each session may have different elementIds for the same level, but names should be consistent
                string cy = $"MERGE (l:Level {{Name: \"{levelName}\"}}) " +
                           $"ON CREATE SET l.elementId = {lvl.Id} " +
                           $"ON MATCH SET l.elementId = {lvl.Id}";
                _cmdManager.cypherCommands.Enqueue(cy);
                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);

                // CRITICAL FIX: Use MERGE for Building to avoid duplicates and ensure existence by Name
                string cyRel =
            $"MERGE (b:Building {{Name: \"{buildingNameEsc}\"}}) " +
            $"ON CREATE SET b.elementId = 1 " +
            $"WITH b " +
            $"MATCH (l:Level {{Name: \"{levelName}\"}}) " +
            $"MERGE (b)-[:CONTAINS]->(l)";
                _cmdManager.cypherCommands.Enqueue(cyRel);
                Debug.WriteLine("[Neo4j] Cypher erzeugt (Building contains Level): " + cyRel);


                // get all Elements of type Room in the current level

                ElementLevelFilter lvlFilter = new ElementLevelFilter(lvl.Id);

                IList<Element> rooms = new FilteredElementCollector(doc)
                    .OfCategory(BuiltInCategory.OST_Rooms)
                    .WhereElementIsNotElementType()
                    .WherePasses(lvlFilter)
                    .ToElements();

                
                // Iterate over all rooms in that level
                int roomIndex = 0;
                foreach (var element in rooms)
                {
                    roomIndex++;

                    
                    var room = (Room)element;

                    if (room.LevelId == null || room.LevelId.Value == -1)
                    {
                        Debug.WriteLine($"[WARN] Raum {room.Id} hat kein g�ltiges Level � wird �bersprungen.");

                        continue;
                    }
                    string escapedRoomName = ParameterUtils.EscapeForCypher(room.Name);
                    Debug.WriteLine($"Room: {escapedRoomName}, ID: {room.Id}");

                    cy = $"MERGE (r:Room {{elementId: {room.Id.Value}}}) " +
                         $"SET r.Name = '{ParameterUtils.EscapeForCypher(room.Name)}', r.Level = '{ParameterUtils.EscapeForCypher(levelName)}' " +
                         $"WITH r MATCH (l:Level {{elementId: {room.LevelId.Value}}}) " +
                         $"MERGE (l)-[:CONTAINS]->(r)";

                    _cmdManager.cypherCommands.Enqueue(cy);
                    Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);



                    IList<IList<BoundarySegment>> boundaries = room.GetBoundarySegments(new SpatialElementBoundaryOptions());


                    int boundaryIndex = 0;
                    foreach (IList<BoundarySegment> b in boundaries)
                    {
                        boundaryIndex++;

                        
                        int segmentIndex = 0;
                        foreach (BoundarySegment s in b)
                        {
                            segmentIndex++;

                            
                            ElementId neighborId = s.ElementId;
                            if (neighborId.Value == -1)
                            {
                                Debug.WriteLine("Something went wrong when extracting Element ID " + neighborId);

                                continue;
                            }


                            Element neighbor = doc.GetElement(neighborId);

                            if (neighbor is Wall wall)
                            {

                                
                                string wallName = wall.get_Parameter(BuiltInParameter.SYMBOL_NAME_PARAM)?.AsString()
                                                  ?? wall.Name ?? "Unbenannt";
                                string escapedWallName = ParameterUtils.EscapeForCypher(wallName);

                                Debug.WriteLine($"\tNeighbor Type: Wall - ID: {wall.Id}, Name: {escapedWallName}");

                                cy = "MATCH (r:Room{elementId:" + room.Id + "}) " +
     "MATCH (l:Level{elementId:" + neighbor.LevelId + "}) " +
     "MERGE (w:Wall{elementId:" + wall.Id + "}) " +
     "SET w.Name = \"" + escapedWallName + "\" " +
     "MERGE (l)-[:CONTAINS]->(w)-[:BOUNDS]->(r)";
                                _cmdManager.cypherCommands.Enqueue(cy);
                                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);


                            }
                            else
                            {

                                Debug.WriteLine("\tNeighbor Type: Undefined - ID: " + neighbor.Id);
                            }
                        }
                    }
                }
                

                ProcessWalls(doc, lvl);

                

                var stairFilter = new ElementLevelFilter(lvl.Id);
                var stairs = new FilteredElementCollector(doc)
                    .OfCategory(BuiltInCategory.OST_Stairs)
                    .WherePasses(stairFilter)
                    .WhereElementIsNotElementType()
                    .ToElements();

                
                int stairIndex = 0;
                foreach (Element e in stairs)
                {
                    stairIndex++;

                    ProcessStair(e, doc);

                }


                var doorCollector = new FilteredElementCollector(doc)
                    .OfCategory(BuiltInCategory.OST_Doors).OfClass(typeof(FamilyInstance)).WherePasses(lvlFilter);

                var doors = doorCollector.ToElements();


                // Iterate over all doors at current level using the detailed
                // serialization method so that all properties are stored in
                // Neo4j. This ensures a door can be fully reconstructed when
                // pulling the model.
                int doorIndex = 0;
                foreach (var door in doors)
                {
                    doorIndex++;

                    ProcessDoor(door, doc);

                }
             

                ProcessPipes(doc, lvl);

            }
            

            ProcessProvisionalSpaces(doc);

            

            CheckBoundingForAllPipes(doc);

            

            var globalStairs = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_Stairs)
                .WhereElementIsNotElementType()
                .ToElements();

            
            int globalStairIndex = 0;
            foreach (Element stair in globalStairs)
            {
                globalStairIndex++;

                ProcessStair(stair, doc);

            }


            string baseDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "NeoCollab");
            Directory.CreateDirectory(baseDir); // falls noch nicht vorhanden

            // print out the elapsed time and stop the timer
            Debug.WriteLine($"#--------#\nTimer stopped: {timer.ElapsedMilliseconds}ms\n#--------#");
            timer.Stop();
            Logger.LogToFile($"CREATE INITIAL GRAPH: Completed in {timer.ElapsedMilliseconds}ms", "sync.log");
        }
        private void ProcessProvisionalSpaces(Document doc)
        {
            var collector = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_GenericModel)
                                .OfClass(typeof(FamilyInstance));


            foreach (FamilyInstance inst in collector)
            {
                ProcessProvisionalSpace(inst, doc);
            }
        }

        private void ProcessPipes(Document doc, Level level)
        {
            var levelFilter = new ElementLevelFilter(level.Id);
            var catFilter = new LogicalOrFilter(new List<ElementFilter>
            {
                new ElementCategoryFilter(BuiltInCategory.OST_PipeCurves),
                new ElementCategoryFilter(BuiltInCategory.OST_PipeSegments)
            });
            var collector = new FilteredElementCollector(doc)
                .WherePasses(levelFilter)
                .WherePasses(catFilter)
                .OfClass(typeof(MEPCurve));

            foreach (MEPCurve pipe in collector.Cast<MEPCurve>())
            {
                ProcessPipe(pipe, doc);
            }
        }
        private static bool Intersects(BoundingBoxXYZ a, BoundingBoxXYZ b)
        {
            return a.Min.X <= b.Max.X && a.Max.X >= b.Min.X &&
                             a.Min.Y <= b.Max.Y && a.Max.Y >= b.Min.Y &&
                             a.Min.Z <= b.Max.Z && a.Max.Z >= b.Min.Z;
        }

        private void UpdatePipeRelations(MEPCurve pipe, Document doc)
        {
            var bbPipe = pipe.get_BoundingBox(null);
            if (bbPipe == null) return;

            // Update Pipe-ProvisionalSpace relationships
            var psCollector = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_GenericModel)
                .OfClass(typeof(FamilyInstance));

            foreach (FamilyInstance ps in psCollector.Cast<FamilyInstance>())
            {
                if (!ParameterUtils.IsProvisionalSpace(ps))
                    continue;

                var bbPs = ps.get_BoundingBox(null);
                if (bbPs == null) continue;
                bool intersects = Intersects(bbPipe, bbPs);
                string cypher;

                if (intersects)
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}}), (ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) MERGE (pi)-[:CONTAINED_IN]->(ps)";
                }
                else
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}})-[r:CONTAINED_IN]->(ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) DELETE r";
                }
                _cmdManager.cypherCommands.Enqueue(cypher);
                Debug.WriteLine("[Neo4j] Updated Pipe-ProvisionalSpace relation: " + cypher);
            }

            // NEW: Update Pipe-Wall relationships (for pipe intersection detection)
            var wallCollector = new FilteredElementCollector(doc)
                .OfCategory(BuiltInCategory.OST_Walls)
                .OfClass(typeof(Wall));

            foreach (Wall wall in wallCollector.Cast<Wall>())
            {
                var bbWall = wall.get_BoundingBox(null);
                if (bbWall == null) continue;
                
                bool intersects = Intersects(bbPipe, bbWall);
                string cypher;

                if (intersects)
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}}), (w:Wall {{elementId:{wall.Id.Value}}}) MERGE (pi)-[:INTERSECTS]->(w)";
                }
                else
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}})-[r:INTERSECTS]->(w:Wall {{elementId:{wall.Id.Value}}}) DELETE r";
                }
                _cmdManager.cypherCommands.Enqueue(cypher);
                Debug.WriteLine("[Neo4j] Updated Pipe-Wall relation: " + cypher);
            }
        }

        private void UpdateProvisionalSpaceRelations(FamilyInstance ps, Document doc)
        {
            if (!ParameterUtils.IsProvisionalSpace(ps))
                return;

            var bbPs = ps.get_BoundingBox(null);
            if (bbPs == null) return;

            var catFilter = new LogicalOrFilter(new List<ElementFilter>
            {
                new ElementCategoryFilter(BuiltInCategory.OST_PipeCurves),
                new ElementCategoryFilter(BuiltInCategory.OST_PipeSegments)
            });

            var pipes = new FilteredElementCollector(doc)
                .WherePasses(catFilter)
                .OfClass(typeof(MEPCurve));

            foreach (MEPCurve pipe in pipes.Cast<MEPCurve>())
            {
                var bbPipe = pipe.get_BoundingBox(null);
                if (bbPipe == null) continue;

                bool intersects = Intersects(bbPipe, bbPs);
                string cypher;
                if (intersects)
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}}), (ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) MERGE (pi)-[:CONTAINED_IN]->(ps)";
                }
                else
                {
                    cypher =
                        $"MATCH (pi:Pipe {{uid:'{pipe.UniqueId}'}})-[r:CONTAINED_IN]->(ps:ProvisionalSpace {{guid:'{ps.UniqueId}'}}) DELETE r";
                }
                _cmdManager.cypherCommands.Enqueue(cypher);
                Debug.WriteLine("[Neo4j] Updated Pipe-ProvisionalSpace relation: " + cypher);
            }
        }

        public void CheckBoundingForAllPipes(Document doc)
        {
            var catFilter = new LogicalOrFilter(new List<ElementFilter>
            {
                new ElementCategoryFilter(BuiltInCategory.OST_PipeCurves),
                new ElementCategoryFilter(BuiltInCategory.OST_PipeSegments)
            });

            var collector = new FilteredElementCollector(doc)
                .WherePasses(catFilter)
                .OfClass(typeof(MEPCurve));

            foreach (MEPCurve pipe in collector.Cast<MEPCurve>())
            {
                UpdatePipeRelations(pipe, doc);
            }
        }

        /// <summary>
        /// Verarbeitet eine einzelne Treppen-Instanz und verbindet sie mit den Basis- und Ober-Ebenen.
        /// </summary>
        private void ProcessStair(Element stairElem, Document doc)
        {
            var baseParam = stairElem.get_Parameter(BuiltInParameter.STAIRS_BASE_LEVEL_PARAM);
            ElementId baseLevelId = (baseParam != null && baseParam.AsElementId() != ElementId.InvalidElementId)
                  ? baseParam.AsElementId()
                  : stairElem.LevelId;

            var topParam = stairElem.get_Parameter(BuiltInParameter.STAIRS_TOP_LEVEL_PARAM);
            ElementId topLevelId = (topParam != null && topParam.AsElementId() != ElementId.InvalidElementId)
                ? topParam.AsElementId()
                : stairElem.LevelId;

            // 2) Revit-Level-Instanzen
            var baseLevel = doc.GetElement(baseLevelId) as Level;
            var topLevel = doc.GetElement(topLevelId) as Level;
            if (baseLevel == null || topLevel == null)
            {
                Debug.WriteLine($"[Stair Processing] Levels not found for stair {stairElem.Id}; base={baseLevelId}, top={topLevelId}");
                return;  // ohne beide Ebenen keine Relationship
            }
            // 3) Lesbarer Name f�r die Treppe
            string stairName = GenerateStairName(baseLevel.Name, topLevel.Name);
            // 4) Cypher-Statement: Node MERGE + Beziehungen
            string cy =
                $"MERGE (s:Stair {{elementId: {stairElem.Id.Value}}}) " +
  $"SET s.Name = '{ParameterUtils.EscapeForCypher(stairName)}' " +
                $"WITH s " +
                $"MATCH (b:Level {{elementId: {baseLevelId.Value}}}), (t:Level {{elementId: {topLevelId.Value}}}) " +
                $"MERGE (b)-[:CONNECTS_TO]->(s) " +
                $"MERGE (s)-[:CONNECTS_TO]->(t)";

            _cmdManager.cypherCommands.Enqueue(cy);
            Debug.WriteLine("[Neo4j] Cypher erzeugt (Stair-Verbindungen): " + cy);
        }

        // Exportiert nur die angegebenen Elemente als tempor�re IFC-Datei.
        public string ExportIfcSubset(Document doc, List<ElementId> elementsToExport)
        {
            if (doc.IsReadOnly)
            {
                Autodesk.Revit.UI.TaskDialog.Show("IFC Export", "Dokument ist schreibgesch\u00fctzt. Export nicht m\u00f6glich.");
                return string.Empty;
            }
            // 1. Tempor�re 3D-Ansicht erstellen und Elemente isolieren
            View3D view = null;
            using (var tx = new Transaction(doc, "Temp IFC View"))
            {
                tx.Start();
                view = new FilteredElementCollector(doc)
                    .OfClass(typeof(View3D))
                    .Cast<View3D>()
                    .FirstOrDefault(v => !v.IsTemplate && v.CanBePrinted);
                if (view == null)
                    throw new InvalidOperationException("Keine 3D-View gefunden!");

                view.IsolateElementsTemporary(elementsToExport);
                tx.Commit();
            }

            // 2. IFC-Export-Optionen setzen
            var ifcExportOptions = new IFCExportOptions
            {
                FileVersion = IFCVersion.IFC4,
                ExportBaseQuantities = true

            };
            ifcExportOptions.AddOption("UseElementIdAsIfcGUID", "1");

            // 3. Exportieren in ein sitzungsspezifisches Temp-Verzeichnis
            var sessionDir = Path.Combine(Path.GetTempPath(), CommandManager.Instance.SessionId);
            Directory.CreateDirectory(sessionDir);
            var tempIfcPath = Path.Combine(sessionDir, $"change_{Guid.NewGuid()}.ifc");

            // Der Export �ndert das Dokument und muss daher in einer Transaction
            // ausgef�hrt werden.
            using (var txExport = new Transaction(doc, "Export IFC Subset"))
            {
                txExport.Start();
                doc.Export(Path.GetDirectoryName(tempIfcPath), Path.GetFileName(tempIfcPath), ifcExportOptions);
                txExport.Commit();
            }

            // 4. Isolation zur�cksetzen
            using (var tx = new Transaction(doc, "Unisolate IFC View"))
            {
                tx.Start();
                view.DisableTemporaryViewMode(TemporaryViewMode.TemporaryHideIsolate);
                tx.Commit();
            }

            return tempIfcPath;
        }


        /// <summary>
        /// Reads the exported IFC file and maps IFC GlobalIds to the provided
        /// Revit ElementIds. This is a best-effort text based mapping that
        /// assumes the order of occurrences matches the element list.
        /// </summary>
        public Dictionary<string, ElementId> MapIfcGuidsToRevitIds(string ifcFilePath, List<ElementId> elementIds)
        {
            var map = new Dictionary<string, ElementId>();
            if (string.IsNullOrEmpty(ifcFilePath) || !File.Exists(ifcFilePath))
                return map;

            var guidRegex = new Regex(@"GLOBALID\('(?<g>[^']+)'", RegexOptions.IgnoreCase);
            var guids = new List<string>();
            foreach (var line in File.ReadLines(ifcFilePath))
            {
                var m = guidRegex.Match(line);
                if (m.Success)
                {
                    string guid = m.Groups["g"].Value.Trim();
                    if (!string.IsNullOrEmpty(guid))
                        guids.Add(guid);
                }
            }

            for (int i = 0; i < elementIds.Count && i < guids.Count; i++)
            {
                map[guids[i]] = elementIds[i];
            }
            return map;
        }
        // �ltere Methode zur Graphaktualisierung, wird f�r Debugzwecke verwendet.
        public void UpdateGraph(Document doc, List<Element> EnqueuedElements, List<ElementId> deletedElementIds, List<Element> modifiedElements)
        {
            Debug.WriteLine(" Starting to update Graph...\n");
            // Reset stair numbering for each update run
            _stairCounters.Clear();
            string cy;

            // delete nodes
            foreach (ElementId id in deletedElementIds)
            {
                Debug.WriteLine($"Deleting Node with ID: {id}");
                
                int intId = (int)id.Value;
                Element e = doc.GetElement(id);
                
                // CRITICAL FIX: Since element is already deleted, we need to determine type differently
                // Check if this element was previously tracked by checking existing Neo4j nodes
                string cyDel;
                
                if (e != null)
                {
                    // Element still exists, use category to determine type
                    if (e.Category?.Id.Value == (int)BuiltInCategory.OST_Walls)
                    {
                        cyDel = $"MATCH (w:Wall {{elementId: {intId}}}) DETACH DELETE w";
                    }
                    else if (e.Category?.Id.Value == (int)BuiltInCategory.OST_Doors)
                    {
                        cyDel = $"MATCH (d:Door {{elementId: {intId}}}) DETACH DELETE d";
                    }
                    else if (e.Category?.Id.Value == (int)BuiltInCategory.OST_PipeCurves)
                    {
                        cyDel = $"MATCH (p:Pipe {{elementId: {intId}}}) DETACH DELETE p";
                    }
                    else if (e.Category?.Id.Value == (int)BuiltInCategory.OST_GenericModel && e is FamilyInstance fi && ParameterUtils.IsProvisionalSpace(fi))
                    {
                        cyDel = $"MATCH (ps:ProvisionalSpace {{elementId: {intId}}}) DETACH DELETE ps";
                    }
                    else
                    {
                        cyDel = $"MATCH (n {{elementId: {intId}}}) DETACH DELETE n";
                    }
                }
                else
                {
                    // Element is already deleted - use comprehensive deletion that handles all types
                    cyDel = $"OPTIONAL MATCH (w:Wall {{elementId: {intId}}}) OPTIONAL MATCH (d:Door {{elementId: {intId}}}) OPTIONAL MATCH (p:Pipe {{elementId: {intId}}}) OPTIONAL MATCH (ps:ProvisionalSpace {{elementId: {intId}}}) WITH w, d, p, ps FOREACH (wall IN CASE WHEN w IS NOT NULL THEN [w] ELSE [] END | DETACH DELETE wall) FOREACH (door IN CASE WHEN d IS NOT NULL THEN [d] ELSE [] END | DETACH DELETE door) FOREACH (pipe IN CASE WHEN p IS NOT NULL THEN [p] ELSE [] END | DETACH DELETE pipe) FOREACH (space IN CASE WHEN ps IS NOT NULL THEN [ps] ELSE [] END | DETACH DELETE space)";
                }
                
                _cmdManager.cypherCommands.Enqueue(cyDel);
                Debug.WriteLine("[Neo4j] Node deletion Cypher: " + cyDel);

                // Create ChangeLog for deletion (always create, even if element is null)
                Logger.LogToFile($"DELETE CHANGELOG: Creating for element {id}", "sync.log");
                CreateChangeLogForElement(id.Value, "Delete");

                if (e == null)
                {
                    Debug.WriteLine($"[Warning] Gel�schtes Element {id} nicht mehr im Doc vorhanden, SQL �berspringe.");
                    continue;
                }

                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cyDel);

            }
            // Diese Syntax ist perfekt
            foreach (Element e in modifiedElements)
            {
                ElementId id = e.Id;

                // change properties
                int intId = (int)e.Id.Value;
                if (e is FamilyInstance fi && fi.Category.Id.Value == (int)BuiltInCategory.OST_Doors)
                {
                    // CRITICAL FIX: Do not update doors during pull operations to prevent feedback loop
                    if (CommandManager.Instance.IsPullInProgress)
                    {
                        continue; // Skip door updates during pull to prevent overwriting correct Neo4j data
                    }
                    
                    // T�r-Eigenschaften und Host aktualisieren
                    var sym = doc.GetElement(fi.GetTypeId()) as FamilySymbol;
                    string doorType = sym?.Name ?? "Unbekannter Typ";
                    string doorNameMod = fi.get_Parameter(BuiltInParameter.DOOR_NUMBER)?.AsString() ?? fi.Name;
                    var hostWall = fi.Host as Wall;
                    string cyDoor =
                        $"MATCH (d:Door {{elementId: {intId}}}) " +
                        "OPTIONAL MATCH (d)-[r:INSTALLED_IN]->() DELETE r " +
                        "WITH d " +
                        $"MATCH (l:Level {{elementId: {fi.LevelId.Value}}}) ";
                    if (hostWall != null)
                        cyDoor += $"MATCH (w:Wall {{elementId: {hostWall.Id.Value}}}) ";
                    cyDoor +=
                        $"SET d.Name = '{ParameterUtils.EscapeForCypher(doorNameMod)}', " +
                        $"d.Type = '{ParameterUtils.EscapeForCypher(doorType)}', " +
                        $"d.hostId = {(hostWall != null ? hostWall.Id.Value : -1)}, " +
                        $"d.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}' " +
                        "MERGE (l)-[:CONTAINS]->(d) ";
                    if (hostWall != null)
                        cyDoor += "MERGE (d)-[:INSTALLED_IN]->(w)";
                    cy = cyDoor;
                    
                    // Create ChangeLog for Door modification
                    Logger.LogToFile($"MODIFY CHANGELOG: Door {fi.Id}", "sync.log");
                    CreateChangeLogForElement(fi.Id.Value, "Modify");
                }
                else if (e is Room)
                {
                    // Raum-Name aktualisieren
                    cy = $"MATCH (r:Room {{ElementId: {intId}}}) " +
                         $"SET r.Name = '{ParameterUtils.EscapeForCypher(e.Name)}', " +
                         $"r.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}'";
                    
                    // Create ChangeLog for Room modification
                    Logger.LogToFile($"MODIFY CHANGELOG: Room {e.Id}", "sync.log");
                    CreateChangeLogForElement(e.Id.Value, "Modify");
                }
                else if (e is Wall wall)
                {
                    // CRITICAL FIX: Update wall coordinates and properties, not just name
                    // Use WallSerializer to get all current properties including coordinates
                    var wallData = WallSerializer.ToNode(wall);
                    var inv = System.Globalization.CultureInfo.InvariantCulture;
                    
                    Logger.LogToFile($"UPDATEGRPH: Updating Wall {wall.Id} coordinates: Start({((double)wallData["x1"]):F3}, {((double)wallData["y1"]):F3}, {((double)wallData["z1"]):F3}) End({((double)wallData["x2"]):F3}, {((double)wallData["y2"]):F3}, {((double)wallData["z2"]):F3}) (meters)", "sync.log");
                    
                    cy = $"MATCH (w:Wall {{elementId: {intId}}}) " +
                         $"SET w.Name = '{ParameterUtils.EscapeForCypher(e.Name)}', " +
                         $"w.x1 = {((double)wallData["x1"]).ToString(inv)}, " +
                         $"w.y1 = {((double)wallData["y1"]).ToString(inv)}, " +
                         $"w.z1 = {((double)wallData["z1"]).ToString(inv)}, " +
                         $"w.x2 = {((double)wallData["x2"]).ToString(inv)}, " +
                         $"w.y2 = {((double)wallData["y2"]).ToString(inv)}, " +
                         $"w.z2 = {((double)wallData["z2"]).ToString(inv)}, " +
                         $"w.modified = datetime(), " +
                         $"w.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}'";
                    
                    // CRITICAL FIX: Actually enqueue the wall coordinate update command!
                    _cmdManager.cypherCommands.Enqueue(cy);
                    Logger.LogToFile($"UPDATEGRPH: Enqueued Wall coordinate update command for Wall {e.Id}", "sync.log");
                    
                    // Create ChangeLog for Wall modification
                    Logger.LogToFile($"MODIFY CHANGELOG: Wall {e.Id}", "sync.log");
                    CreateChangeLogForElement(e.Id.Value, "Modify");
                }
                else if (e is FamilyInstance psFi && psFi.Category.Id.Value == (int)BuiltInCategory.OST_GenericModel && ParameterUtils.IsProvisionalSpace(psFi))
                {
                    // CRITICAL FIX: Do not update coordinates during pull operations to prevent feedback loop
                    if (CommandManager.Instance.IsPullInProgress)
                    {
                        Logger.LogToFile($"UPDATEGRPH: Skipping ProvisionalSpace coordinate update during pull - {psFi.Id}", "sync.log");
                        continue; // Skip coordinate updates during pull to prevent overwriting correct Neo4j data
                    }
                    
                    try
                    {
                        // CRITICAL FIX: Update ProvisionalSpace coordinates and properties using ProvisionalSpaceSerializer
                        // Use ProvisionalSpaceSerializer to get all current properties including coordinates
                        Logger.LogToFile($"UPDATEGRPH: Starting ProvisionalSpace serialization for {psFi.Id}", "sync.log");
                        var data = ProvisionalSpaceSerializer.ToProvisionalSpaceNode(psFi, out var dictData);
                        
                        // Verify data was serialized correctly
                        if (dictData == null)
                        {
                            Logger.LogToFile($"UPDATEGRPH: ERROR - ProvisionalSpaceSerializer.ToProvisionalSpaceNode returned null dictData for ProvisionalSpace {psFi.Id}", "sync.log");
                            continue;
                        }
                        
                        // Verify required fields exist
                        if (!dictData.ContainsKey("x") || !dictData.ContainsKey("y") || !dictData.ContainsKey("z") || 
                            !dictData.ContainsKey("width") || !dictData.ContainsKey("height") || !dictData.ContainsKey("name"))
                        {
                            Logger.LogToFile($"UPDATEGRPH: ERROR - Missing required fields in serialized ProvisionalSpace {psFi.Id} data", "sync.log");
                            continue;
                        }
                        
                        var inv = System.Globalization.CultureInfo.InvariantCulture;
                        
                        Logger.LogToFile($"UPDATEGRPH: Updating ProvisionalSpace {psFi.Id} coordinates: x={((double)dictData["x"]):F6}, y={((double)dictData["y"]):F6}, z={((double)dictData["z"]):F6} width={((double)dictData["width"]):F6} height={((double)dictData["height"]):F6} (meters)", "sync.log");
                        
                        cy = $"MATCH (ps:ProvisionalSpace {{elementId: {intId}}}) " +
                             $"SET ps.name = '{ParameterUtils.EscapeForCypher(dictData["name"].ToString())}', " +
                             $"ps.x = {((double)dictData["x"]).ToString(inv)}, " +
                             $"ps.y = {((double)dictData["y"]).ToString(inv)}, " +
                             $"ps.z = {((double)dictData["z"]).ToString(inv)}, " +
                             $"ps.width = {((double)dictData["width"]).ToString(inv)}, " +
                             $"ps.height = {((double)dictData["height"]).ToString(inv)}, " +
                             $"ps.modified = datetime('{((DateTime)dictData["modified"]).ToString("o")}'), " +
                             $"ps.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}'";
                        
                        // Log the generated Cypher command for debugging
                        Logger.LogToFile($"UPDATEGRPH: Generated ProvisionalSpace Cypher: {cy}", "sync.log");
                        
                        // CRITICAL FIX: Actually enqueue the ProvisionalSpace coordinate update command!
                        _cmdManager.cypherCommands.Enqueue(cy);
                        Logger.LogToFile($"UPDATEGRPH: Successfully enqueued ProvisionalSpace coordinate update command for ProvisionalSpace {psFi.Id}", "sync.log");
                        
                        // Create ChangeLog for ProvisionalSpace modification
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for modified ProvisionalSpace {psFi.Id}", "sync.log");
                        CreateChangeLogForElement(psFi.Id.Value, "Modify");
                        
                        // CRITICAL FIX: Update relationships when ProvisionalSpace is modified/moved
                        Logger.LogToFile($"UPDATEGRPH: Updating ProvisionalSpace relationships for {psFi.Id}", "sync.log");
                        UpdateProvisionalSpaceRelations(psFi, doc);
                        Logger.LogToFile($"UPDATEGRPH: Completed ProvisionalSpace update workflow for {psFi.Id}", "sync.log");
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"UPDATEGRPH: ERROR during ProvisionalSpace {psFi.Id} update: {ex.Message}", "sync.log");
                        Logger.LogCrash($"ProvisionalSpace update failed for {psFi.Id}", ex);
                        // Continue processing other elements even if this space fails
                        continue;
                    }
                }
                else if (e is MEPCurve pipe && pipe.Category.Id.Value == (int)BuiltInCategory.OST_PipeCurves)
                {
                    // CRITICAL FIX: Do not update pipes during pull operations to prevent feedback loop
                    if (CommandManager.Instance.IsPullInProgress)
                    {
                        Logger.LogToFile($"UPDATEGRPH: Skipping Pipe update during pull - {pipe.Id}", "sync.log");
                        continue; // Skip pipe updates during pull to prevent overwriting correct Neo4j data
                    }
                    
                    try
                    {
                        // CRITICAL FIX: Update pipe coordinates and properties using PipeSerializer
                        // Use PipeSerializer to get all current properties including coordinates
                        Logger.LogToFile($"UPDATEGRPH: Starting Pipe serialization for {pipe.Id}", "sync.log");
                        var data = PipeSerializer.ToNode(pipe);
                        
                        // Verify data was serialized correctly
                        if (data == null)
                        {
                            Logger.LogToFile($"UPDATEGRPH: ERROR - PipeSerializer.ToNode returned null for Pipe {pipe.Id}", "sync.log");
                            continue;
                        }
                        
                        // Verify required fields exist
                        if (!data.ContainsKey("x1") || !data.ContainsKey("y1") || !data.ContainsKey("z1") || 
                            !data.ContainsKey("x2") || !data.ContainsKey("y2") || !data.ContainsKey("z2"))
                        {
                            Logger.LogToFile($"UPDATEGRPH: ERROR - Missing coordinate fields in serialized Pipe {pipe.Id} data", "sync.log");
                            continue;
                        }
                        
                        var inv = System.Globalization.CultureInfo.InvariantCulture;
                        
                        Logger.LogToFile($"UPDATEGRPH: Updating Pipe {pipe.Id} coordinates: Start({((double)data["x1"]):F3}, {((double)data["y1"]):F3}, {((double)data["z1"]):F3}) End({((double)data["x2"]):F3}, {((double)data["y2"]):F3}, {((double)data["z2"]):F3}) diameter={((double)data["diameter"]):F3} (meters)", "sync.log");
                        
                        cy = $"MATCH (p:Pipe {{elementId: {intId}}}) " +
                             $"SET p.x1 = {((double)data["x1"]).ToString(inv)}, " +
                             $"p.y1 = {((double)data["y1"]).ToString(inv)}, " +
                             $"p.z1 = {((double)data["z1"]).ToString(inv)}, " +
                             $"p.x2 = {((double)data["x2"]).ToString(inv)}, " +
                             $"p.y2 = {((double)data["y2"]).ToString(inv)}, " +
                             $"p.z2 = {((double)data["z2"]).ToString(inv)}, " +
                             $"p.diameter = {((double)data["diameter"]).ToString(inv)}, " +
                             $"p.lastModifiedUtc = datetime('{((DateTime)data["modified"]).ToString("o")}'), " +
                             $"p.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}'";
                        
                        // Log the generated Cypher command for debugging
                        Logger.LogToFile($"UPDATEGRPH: Generated Pipe Cypher: {cy}", "sync.log");
                        
                        // CRITICAL FIX: Actually enqueue the pipe coordinate update command!
                        _cmdManager.cypherCommands.Enqueue(cy);
                        Logger.LogToFile($"UPDATEGRPH: Successfully enqueued Pipe coordinate update command for Pipe {pipe.Id}", "sync.log");
                        
                        // Create ChangeLog for Pipe modification
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for modified Pipe {pipe.Id}", "sync.log");
                        CreateChangeLogForElement(pipe.Id.Value, "Modify");
                        
                        // CRITICAL FIX: Update relationships when Pipe is modified/moved
                        Logger.LogToFile($"UPDATEGRPH: Updating Pipe relationships for {pipe.Id}", "sync.log");
                        UpdatePipeRelations(pipe, doc);
                        Logger.LogToFile($"UPDATEGRPH: Completed Pipe update workflow for {pipe.Id}", "sync.log");
                    }
                    catch (Exception ex)
                    {
                        Logger.LogToFile($"UPDATEGRPH: ERROR during Pipe {pipe.Id} update: {ex.Message}", "sync.log");
                        Logger.LogCrash($"Pipe update failed for {pipe.Id}", ex);
                        // Continue processing other elements even if this pipe fails
                        continue;
                    }
                }
                else if (e is Level)
                {
                    cy = $"MATCH (l:Level {{elementId: {intId}}}) " +
                         $"SET l.Name = '{ParameterUtils.EscapeForCypher(e.Name)}', " +
                         $"l.user = '{ParameterUtils.EscapeForCypher(CommandManager.Instance.SessionId)}'";
                }
              
                else
                {
                    // unbekannter Typ �berspringen
                    continue;
                }

                _cmdManager.cypherCommands.Enqueue(cy);
                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);

                // change relationships
                if (typeof(Room).IsAssignableFrom(e.GetType()))
                {
                    Debug.WriteLine($"Modifying Node with ID: {id} and Name: {e.Name}");



                    Room room = e as Room;
                    // get all boundaries
                    IList<IList<BoundarySegment>> boundaries
                    = room.GetBoundarySegments(new SpatialElementBoundaryOptions());

                    foreach (IList<BoundarySegment> b in boundaries)
                    {
                        // Iterate over all elements adjacent to current room
                        foreach (BoundarySegment s in b)
                        {
                            // get neighbor element
                            ElementId neighborId = s.ElementId;
                            if (neighborId.Value == -1)
                            {
                                Debug.WriteLine(" Something went wrong when extracting Element ID " + neighborId);
                                continue;
                            }

                            Element neighbor = doc.GetElement(neighborId);
                            var levelId = neighbor.LevelId;

                            if (neighbor is Wall wall)
                            {

                                if (wall.LevelId == ElementId.InvalidElementId)
                                {
                                    Debug.WriteLine($"[WARN] Wall {wall.Id} has invalid LevelId.");
                                    continue; // �berspringen
                                }
                                string escapedWallName = ParameterUtils.EscapeForCypher(wall.Name);
                                cy = "MATCH (r:Room{ElementId: " + room.Id + "})" +
        " MATCH (w:Wall{ElementId: " + wall.Id + "})" +
        " MATCH (l:Level{ElementId: " + wall.LevelId.Value + "})" +
        " MERGE (l)-[:CONTAINS]->(w)-[:BOUNDS]->(r)";
                                _cmdManager.cypherCommands.Enqueue(cy);
                                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);
                                Debug.WriteLine($"Modified Room with ID: {id} and Name: {e.Name}");
                            }
                        }
                      
                    }
                }
                if (typeof(Wall).IsAssignableFrom(e.GetType()))
                {
                    Debug.WriteLine($"Modifying Node with ID: {id} and Name: {e.Name}");


                    // get the room
                    IList<Element> rooms = getRoomFromWall(doc, e as Wall);


                    foreach (Element element in rooms)
                    {
                        var room = (Room)element;
                        var levelId = room.LevelId;
                        cy = " MATCH (w:Wall{ElementId: " + id + "}) " +
                             " MATCH (r:Room{ElementId: " + room.Id + "})" +
                             " MATCH (l:Level{ElementId: " + levelId + "})" +
                             " MERGE (l)-[:CONTAINS]->(w)-[:BOUNDS]->(r)";
                        _cmdManager.cypherCommands.Enqueue(cy);
                        Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);


                        Debug.WriteLine($"Modified Wall with ID: {id} and Name: {e.Name} ");
                    }
                }

                if (typeof(Level).IsAssignableFrom(e.GetType()))
                {
                    Debug.WriteLine($"Modifying Node with ID: {id} and Name: {e.Name}");


                    ElementLevelFilter lvlFilter = new ElementLevelFilter(id);
                    FilteredElementCollector collector = new FilteredElementCollector(doc);
                    IList<Element> elementsOnLevel = collector.WherePasses(lvlFilter).ToElements();

                    foreach (Element element in elementsOnLevel)
                    {
                        if (typeof(Wall).IsAssignableFrom(element.GetType()))
                        {
                            cy = " MATCH (l:Level{ElementId: " + id + "}) " +
                                 " MATCH (w:Wall{ElementId: " + element.Id + "}) " +
                                 " MERGE (l)-[:CONTAINS]->(w)";
                            _cmdManager.cypherCommands.Enqueue(cy);
                            Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);


                        }
                        else if (typeof(Room).IsAssignableFrom(element.GetType()))
                        {
                            cy = " MATCH (l:Level{ElementId: " + id + "}) " +
                                 " MATCH (r:Room{ElementId: " + element.Id + "}) " +
                                 " MERGE (l)-[:CONTAINS]->(r)";
                            _cmdManager.cypherCommands.Enqueue(cy);
                            Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);


                        }

                        Debug.WriteLine($"Modified Level with ID: {id} and Name: {e.Name}");
                    }
                }
            }

            foreach (var e in EnqueuedElements)
            {
                Logger.LogToFile($"UPDATEGRPH PROCESS ADDED: Processing element {e.Id} of category {e.Category?.Name} ({e.Category?.Id.Value})", "sync.log");
                
                switch (e)
                {
                    case Room room:
                        Logger.LogToFile($"UPDATEGRPH: Processing Room {room.Id}", "sync.log");
                        ProcessRoom(room, doc);
                        break;
                    case Wall wall:
                        Logger.LogToFile($"UPDATEGRPH: Processing Wall {wall.Id}", "sync.log");
                        ProcessWall(wall, doc);
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for Wall {wall.Id}", "sync.log");
                        CreateChangeLogForElement(wall.Id.Value, "Insert");
                        break;
                    case FamilyInstance fi when fi.Category.Id.Value == (int)BuiltInCategory.OST_Doors:
                        Logger.LogToFile($"UPDATEGRPH: Processing Door {fi.Id}", "sync.log");
                        ProcessDoor(fi, doc);
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for Door {fi.Id}", "sync.log");
                        CreateChangeLogForElement(fi.Id.Value, "Insert");
                        break;
                    case FamilyInstance fi when fi.Category.Id.Value == (int)BuiltInCategory.OST_GenericModel && ParameterUtils.IsProvisionalSpace(fi):
                        Logger.LogToFile($"UPDATEGRPH: Processing NEW ProvisionalSpace {fi.Id} (Category: {fi.Category?.Name}, IsProvisionalSpace: {ParameterUtils.IsProvisionalSpace(fi)})", "sync.log");
                        ProcessProvisionalSpace(fi, doc);
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for NEW ProvisionalSpace {fi.Id}", "sync.log");
                        CreateChangeLogForElement(fi.Id.Value, "Insert");
                        Logger.LogToFile($"UPDATEGRPH: ProvisionalSpace {fi.Id} processing COMPLETED", "sync.log");
                        break;
                    case MEPCurve pipe when pipe.Category.Id.Value == (int)BuiltInCategory.OST_PipeCurves:
                        Logger.LogToFile($"UPDATEGRPH: Processing Pipe {pipe.Id}", "sync.log");
                        ProcessPipe(pipe, doc);
                        Logger.LogToFile($"UPDATEGRPH: Creating ChangeLog for Pipe {pipe.Id}", "sync.log");
                        CreateChangeLogForElement(pipe.Id.Value, "Insert");
                        break;
                    case Element st when st.Category.Id.Value == (int)BuiltInCategory.OST_Stairs:
                        Logger.LogToFile($"UPDATEGRPH: Processing Stair {st.Id}", "sync.log");
                        // Directly process the stair element. Level information
                        // will be resolved inside ProcessStair.
                        ProcessStair(st, doc);
                        break;
                    default:
                        Logger.LogToFile($"UPDATEGRPH WARNING: Unhandled element type {e.GetType().Name} with category {e.Category?.Name} ({e.Category?.Id.Value}) for element {e.Id}", "sync.log");
                        break;
                }
            }

            //Enqueue nodes
            var EnqueuedElementIds = EnqueuedElements.Select(e => e.Id).ToList();
            foreach (ElementId id in EnqueuedElementIds)
            {
                Element e = doc.GetElement(id);

                if (typeof(Room).IsAssignableFrom(e.GetType()))
                {
                    var room = (Room)e;

                    // capture result
                    Debug.WriteLine($"Room: {room.Name}, ID: {room.Id}");

                    cy = " MATCH (l:Level{ElementId:" + room.LevelId + "}) " +
                         " MERGE (r:Room{Name: \"" + room.Name + "\", ElementId: " + room.Id + "}) " +
                         " MERGE (l)-[:CONTAINS]->(r)";
                    _cmdManager.cypherCommands.Enqueue(cy);
                    Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);

                    // get all boundaries
                    IList<IList<BoundarySegment>> boundaries
                    = room.GetBoundarySegments(new SpatialElementBoundaryOptions());


                    foreach (IList<BoundarySegment> b in boundaries)
                    {
                        // Iterate over all elements adjacent to current room
                        foreach (BoundarySegment s in b)
                        {

                            // get neighbor element
                            ElementId neighborId = s.ElementId;
                            if (neighborId.Value == -1)
                            {
                                Debug.WriteLine(" Something went wrong when extracting Element ID " + neighborId);
                                continue;
                            }

                            Element neighbor = doc.GetElement(neighborId);

                            if (neighbor is Wall)
                            {
                                Debug.WriteLine($"\tNeighbor Type: Wall - ID: {neighbor.Id}");

                                cy = " MATCH (r:Room{ElementId:" + room.Id + "}) " +
                                     " MATCH (l:Level{ElementId:" + neighbor.LevelId + "}) " +
                                     " MERGE (w:Wall{ElementId: " + neighbor.Id + ", Name: \"" + neighbor.Name + "\"}) " +
                                     " MERGE (l)-[:CONTAINS]->(w)-[:BOUNDS]->(r)";
                                _cmdManager.cypherCommands.Enqueue(cy);
                                Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cy);
                            }
                            else
                            {
                                Debug.WriteLine("\tNeighbor Type: Undefined - ID: " + neighbor.Id);
                            }
                        }
                    }
                }
                if (typeof(Wall).IsAssignableFrom(e.GetType()))
                {
                    var wall = (Wall)e;
                    Debug.WriteLine($"Wall: {wall.Name}, ID: {wall.Id}");
                    if (wall.LevelId == ElementId.InvalidElementId)
                    {
                        Debug.WriteLine($"[WARN] Wall {wall.Id} has invalid LevelId.");
                    }

                    // Create or update wall node with all properties
                    ProcessWall(wall, doc);
                    // Link wall to adjacent rooms
                    IList<Element> rooms = getRoomFromWall(doc, wall);
                    foreach (var roomElement in rooms)
                    {
                        if (roomElement is Room r)
                        {
                            string cyRel =
 $"MATCH (w:Wall {{ElementId: {wall.Id.Value}}}), (r:Room {{ElementId: {r.Id.Value}}}) MERGE (w)-[:BOUNDS]->(r)"; _cmdManager.cypherCommands.Enqueue(cyRel);
                            Debug.WriteLine("[Neo4j] Cypher erzeugt: " + cyRel);
                        }
                    }
                }
            }




        }
        // Hilfsfunktion: findet R�ume, die eine Wand schneiden.
        public static IList<Element> getRoomFromWall(Document doc, Wall wall)
        {
            BoundingBoxXYZ wall_bb = wall.get_BoundingBox(null);
            Outline outl = new Outline(wall_bb.Min, wall_bb.Max);
            ElementFilter bbfilter = new BoundingBoxIntersectsFilter(outl);

            IList<Element> rooms = new FilteredElementCollector(doc).OfCategory(BuiltInCategory.OST_Rooms).WherePasses(bbfilter).ToElements();

            return rooms;
        }

      
        /// <summary>
        /// Creates a ChangeLog entry for an element using the central Neo4j-based multi-session approach
        /// <summary>
        /// Creates ChangeLog entries for all OTHER sessions when this session modifies an element
        /// CRITICAL FIX: Proper session filtering and synchronous completion
        /// </summary>
        private void CreateChangeLogForElement(long elementId, string operation)
        {
            try
            {
                Logger.LogToFile($"CHANGELOG CREATION: Creating ChangeLog for element {elementId} with operation {operation}", "sync.log");
                
                // CRITICAL FIX: Get current session and find ALL other sessions from Neo4j (not local SessionManager)
                string currentSessionId = CommandManager.Instance.SessionId;
                var connector = CommandManager.Instance.Neo4jConnector;
                
                // Get ALL sessions from Neo4j database (includes sessions from other Revit instances)
                var allSessionsTask = connector.GetAllActiveSessionsAsync();
                allSessionsTask.Wait(); // Synchronous wait for simplicity
                var allSessions = allSessionsTask.Result;
                var targetSessions = allSessions.Where(sessionKey => sessionKey != currentSessionId).ToList();
                
                Logger.LogToFile($"CHANGELOG TARGETS: Creating ChangeLog for element {elementId} ({operation}) in {targetSessions.Count} target sessions: [{string.Join(", ", targetSessions)}]", "sync.log");
                Logger.LogToFile($"CHANGELOG DEBUG: All sessions from Neo4j: [{string.Join(", ", allSessions)}], Current (will become original if new element): {currentSessionId}", "sync.log");
                
                if (targetSessions.Count == 0)
                {
                    Logger.LogToFile("CHANGELOG CREATION: No other sessions found in Neo4j, skipping ChangeLog creation", "sync.log");
                    Logger.LogToFile($"CHANGELOG DIAGNOSTIC: Current Session={currentSessionId}, All Sessions from DB=[{string.Join(", ", allSessions)}]", "sync.log");
                    return;
                }
                
                // Create ChangeLog entries for all other sessions SYNCHRONOUSLY to ensure completion
                var tasks = new List<Task>();
                foreach (var targetSession in targetSessions)
                {
                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await connector.CreateChangeLogEntryWithRelationshipsAsync(elementId, operation, targetSession);
                            Logger.LogToFile($"CHANGELOG CREATED: Successfully created ChangeLog for element {elementId} ({operation}) in target session {targetSession}", "sync.log");
                        }
                        catch (Exception ex)
                        {
                            Logger.LogToFile($"CHANGELOG ERROR: Failed to create ChangeLog for element {elementId} in session {targetSession}: {ex.Message}", "sync.log");
                            Logger.LogCrash($"CreateChangeLogForElement failed for {elementId} in session {targetSession}", ex);
                        }
                    });
                    tasks.Add(task);
                }
                
                // CRITICAL: Wait for all ChangeLog creations to complete
                Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(10)); // 10 second timeout
                
                Logger.LogToFile($"CHANGELOG CREATION COMPLETED: ChangeLog creation finished for element {elementId} in {targetSessions.Count} sessions", "sync.log");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("CreateChangeLogForElement", ex);
            }
        }
    }
}
