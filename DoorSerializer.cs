using Autodesk.Revit.DB;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Versioning;

using static NeoCollab.ParameterUtils;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public static class DoorSerializer
{
    // Erstellt ein Dictionary mit allen relevanten Eigenschaften einer Tür,
    // welches später nach Neo4j geschrieben werden kann.
    public static Dictionary<string, object> ToNode(FamilyInstance door)
    {
        var loc = door.Location as LocationPoint;
        BoundingBoxXYZ bb = door.get_BoundingBox(null);
        double width = 0, height = 0, thickness = 0;
        if (bb != null)
        {
            // CRITICAL FIX: Calculate dimensions but store in METERS for ChangeLog compatibility
            width = Math.Abs(bb.Max.X - bb.Min.X);
            height = Math.Abs(bb.Max.Z - bb.Min.Z);
            thickness = Math.Abs(bb.Max.Y - bb.Min.Y);
        }

        var dict = new Dictionary<string, object>
        {
            ["rvtClass"] = "Door",
            ["uid"] = door.UniqueId,
            ["name"] = door.get_Parameter(BuiltInParameter.DOOR_NUMBER)?.AsString() ?? string.Empty,
            ["elementId"] = door.Id.Value,
            ["typeId"] = door.GetTypeId().Value,
            ["familyName"] = door.Symbol?.FamilyName ?? string.Empty,
            ["symbolName"] = door.Symbol?.Name ?? string.Empty,
            ["levelId"] = door.LevelId.Value,
            ["hostId"] = door.Host?.Id.Value ?? -1,
            ["hostUid"] = door.Host?.UniqueId ?? string.Empty,
            // CRITICAL FIX: Store coordinates in METERS for ChangeLog compatibility (same as WallSerializer)
            ["x"] = Math.Round(UnitConversion.ToMeters(loc?.Point.X ?? 0), 6),
            ["y"] = Math.Round(UnitConversion.ToMeters(loc?.Point.Y ?? 0), 6),
            ["z"] = Math.Round(UnitConversion.ToMeters(loc?.Point.Z ?? 0), 6),
            ["rotation"] = Math.Round(loc?.Rotation ?? 0, 6),
            // CRITICAL FIX: Store dimensions in METERS for ChangeLog compatibility
            ["width"] = Math.Round(UnitConversion.ToMeters(width), 6),
            ["height"] = Math.Round(UnitConversion.ToMeters(height), 6),
            ["thickness"] = Math.Round(UnitConversion.ToMeters(thickness), 6),
            ["created"] = DateTime.UtcNow,
            ["modified"] = DateTime.UtcNow,
            ["user"] = CommandManager.Instance.SessionId
        };

        SerializeParameters(door, dict);
        return dict;
    }

    // Erstellt einen DoorNode aus den übertragenen Attributen.
    public static DoorNode ToDoorNode(FamilyInstance door)
    {
        var dict = ToNode(door);
        var node = new DoorNode(
                        dict.GetValueOrDefault("name", string.Empty).ToString() ?? string.Empty,
            dict.TryGetValue("uid", out var uidObj) ? uidObj.ToString() ?? string.Empty : string.Empty,
            Convert.ToInt64(dict["elementId"]),
            Convert.ToInt64(dict["typeId"]),
            dict.GetValueOrDefault("familyName", string.Empty).ToString() ?? string.Empty,
            dict.GetValueOrDefault("symbolName", string.Empty).ToString() ?? string.Empty,
            Convert.ToInt64(dict["levelId"]),
            Convert.ToInt64(dict["hostId"]),
            dict.GetValueOrDefault("hostUid", string.Empty).ToString() ?? string.Empty,
            Convert.ToDouble(dict.GetValueOrDefault("x", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("y", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("z", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("rotation", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("width", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("height", 0.0)),
            Convert.ToDouble(dict.GetValueOrDefault("thickness", 0.0))
        );
        Debug.WriteLine($"[DoorSerializer] Created node for {door.UniqueId}");
        return node;
    }
}