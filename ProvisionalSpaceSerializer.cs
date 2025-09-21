using Autodesk.Revit.DB;
using System;
using System.Collections.Generic;
using System.Runtime.Versioning;
using System.Diagnostics;
using static NeoCollab.ParameterUtils;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public static class ProvisionalSpaceSerializer
{
    private const string LogFile = "provisional_spaces.log";

    // Wandelt einen ProvisionalSpace in ein Dictionary zur Ablage in Neo4j um.

    public static Dictionary<string, object> ToNode(FamilyInstance inst)
    {
        Logger.LogToFile($"[Serializer] Start {inst.UniqueId}", LogFile);

        BoundingBoxXYZ bb = inst.get_BoundingBox(null);
        double width = 0, height = 0, thickness = 0;
        if (bb != null)
        {
            // CRITICAL FIX: Calculate dimensions but store in METERS for ChangeLog compatibility
            width = Math.Abs(bb.Max.X - bb.Min.X);
            height = Math.Abs(bb.Max.Z - bb.Min.Z);
            thickness = Math.Abs(bb.Max.Y - bb.Min.Y);
        }
        bool isProv = ParameterUtils.IsProvisionalSpace(inst);
        Logger.LogToFile($"[Serializer] isProv {isProv}", LogFile);
        BoundingBoxXYZ? bbView = null;
        XYZ bbMin = XYZ.Zero, bbMax = XYZ.Zero;
        if (isProv)
        {
            bbView = inst.get_BoundingBox(inst.Document.ActiveView) ?? inst.get_BoundingBox(null);
            if (bbView != null)
            {
                bbMin = bbView.Min;
                bbMax = bbView.Max;
            }
        }

        var level = inst.Document.GetElement(inst.LevelId) as Level;
        string levelName = level?.Name ?? string.Empty;
        string name = inst.Name ?? inst.Symbol?.FamilyName ?? string.Empty;
        var loc = inst.Location as LocationPoint;

        var dict = new Dictionary<string, object>
        {
            ["rvtClass"] = "ProvisionalSpace",
            ["guid"] = inst.UniqueId,
            ["elementId"] = inst.Id.Value,  // FIXED: Single elementId field instead of separate revitId
            ["typeId"] = inst.GetTypeId().Value,
            ["familyName"] = inst.Symbol?.FamilyName ?? string.Empty,
            ["symbolName"] = inst.Symbol?.Name ?? string.Empty,
            ["name"] = name,
            // CRITICAL FIX: Store dimensions in METERS for ChangeLog compatibility (same as WallSerializer)
            ["width"] = Math.Round(UnitConversion.ToMeters(width), 6),
            ["height"] = Math.Round(UnitConversion.ToMeters(height), 6),
            ["thickness"] = Math.Round(UnitConversion.ToMeters(thickness), 6),
            ["level"] = levelName,
            ["levelId"] = inst.LevelId.Value,
            // CRITICAL FIX: Store coordinates in METERS for ChangeLog compatibility (same as WallSerializer)
            ["x"] = Math.Round(UnitConversion.ToMeters(loc?.Point.X ?? 0), 6),
            ["y"] = Math.Round(UnitConversion.ToMeters(loc?.Point.Y ?? 0), 6),
            ["z"] = Math.Round(UnitConversion.ToMeters(loc?.Point.Z ?? 0), 6),
            ["rotation"] = Math.Round(loc?.Rotation ?? 0, 6),
            ["hostId"] = inst.Host?.Id.Value ?? -1,
            ["ifcType"] = inst.get_Parameter(BuiltInParameter.IFC_EXPORT_ELEMENT)?.AsString() ?? string.Empty,
            ["created"] = DateTime.UtcNow,
            ["modified"] = DateTime.UtcNow,
            ["user"] = CommandManager.Instance.SessionId
        };
        if (isProv)
        {
            dict["category"] = inst.Category?.Name ?? string.Empty;
            dict["familyName"] = inst.Symbol?.FamilyName ?? string.Empty;
            dict["phaseCreated"] = inst.get_Parameter(BuiltInParameter.PHASE_CREATED)?.AsInteger() ?? -1;
            dict["phaseDemolished"] = inst.get_Parameter(BuiltInParameter.PHASE_DEMOLISHED)?.AsInteger() ?? -1;
            // CRITICAL FIX: Store bounding box in METERS for ChangeLog compatibility
            dict["bbMinX"] = Math.Round(UnitConversion.ToMeters(bbMin.X), 6);
            dict["bbMinY"] = Math.Round(UnitConversion.ToMeters(bbMin.Y), 6);
            dict["bbMinZ"] = Math.Round(UnitConversion.ToMeters(bbMin.Z), 6);
            dict["bbMaxX"] = Math.Round(UnitConversion.ToMeters(bbMax.X), 6);
            dict["bbMaxY"] = Math.Round(UnitConversion.ToMeters(bbMax.Y), 6);
            dict["bbMaxZ"] = Math.Round(UnitConversion.ToMeters(bbMax.Z), 6);
        }

        SerializeParameters(inst, dict);
        Logger.LogToFile($"[Serializer] Dictionary ready for {inst.UniqueId}", LogFile);
        return dict;
    }

    // Erstellt einen ProvisionalSpaceNode und liefert zugleich das Dictionary
    // mit allen serialisierten Eigenschaften zur weiteren Verwendung.
    public static ProvisionalSpaceNode ToProvisionalSpaceNode(
        FamilyInstance inst,
        out Dictionary<string, object> dict)
    {
        Logger.LogToFile($"[Serializer] ToProvisionalSpaceNode {inst.UniqueId}", LogFile);
        dict = ToNode(inst);
        var node = new ProvisionalSpaceNode(
            dict.TryGetValue("guid", out var gObj) ? gObj.ToString() ?? string.Empty : string.Empty,
            dict.GetValueOrDefault("name", string.Empty).ToString() ?? string.Empty,
            dict.GetValueOrDefault("familyName", string.Empty).ToString() ?? string.Empty,
            dict.GetValueOrDefault("symbolName", string.Empty).ToString() ?? string.Empty,
            Convert.ToDouble(dict.GetValueOrDefault("width", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("height", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("thickness", 0.0)),
            dict.GetValueOrDefault("level", string.Empty).ToString() ?? string.Empty,
            Convert.ToInt64(dict["levelId"]),
            Convert.ToDouble(dict.GetValueOrDefault("x", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("y", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("z", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("rotation", 0.0)),
            Convert.ToInt64(dict.GetValueOrDefault("hostId", -1L)),
            Convert.ToInt32(dict.GetValueOrDefault("elementId", -1)),  // FIXED: Use elementId instead of revitId
            dict.GetValueOrDefault("ifcType", string.Empty).ToString() ?? string.Empty,
            dict.ContainsKey("category") ? dict["category"].ToString() : null,
            Convert.ToInt32(dict.GetValueOrDefault("phaseCreated", -1)),
            Convert.ToInt32(dict.GetValueOrDefault("phaseDemolished", -1)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMinX", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMinY", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMinZ", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMaxX", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMaxY", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("bbMaxZ", 0.0))
        );
                Debug.WriteLine($"[ProvisionalSpaceSerializer] Created node for {inst.UniqueId}");
        Logger.LogToFile($"[Serializer] Node created for {inst.UniqueId}", LogFile);
        return node;

    }
}
