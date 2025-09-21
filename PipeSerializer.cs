using Autodesk.Revit.DB;
using System;
using System.Collections.Generic;
using System.Runtime.Versioning;
using System.Diagnostics;
using static NeoCollab.ParameterUtils;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public static class PipeSerializer
{
    // Serialisiert ein Rohr in ein Dictionary für Neo4j.
    public static Dictionary<string, object> ToNode(MEPCurve pipe)
    {
        var lc = pipe.Location as LocationCurve;
        var line = lc?.Curve as Line;
        XYZ s = line?.GetEndPoint(0) ?? XYZ.Zero;
        XYZ e = line?.GetEndPoint(1) ?? XYZ.Zero;
        double diameter = pipe.get_Parameter(BuiltInParameter.RBS_PIPE_DIAMETER_PARAM)?.AsDouble() ?? 0;
        Level level = pipe.ReferenceLevel ?? pipe.Document.GetElement(pipe.LevelId) as Level;
        var dict = new Dictionary<string, object>
        {
            ["rvtClass"] = "Pipe",
            ["uid"] = pipe.UniqueId,
            ["elementId"] = pipe.Id.Value,
            ["typeId"] = pipe.GetTypeId().Value,
            ["systemTypeId"] = pipe.get_Parameter(BuiltInParameter.RBS_PIPING_SYSTEM_TYPE_PARAM)?.AsInteger() ?? -1,
            ["levelId"] = level?.Id.Value ?? -1,
            // CRITICAL FIX: Store coordinates in METERS for ChangeLog compatibility (same as WallSerializer)
            ["x1"] = Math.Round(UnitConversion.ToMeters(s.X), 6),
            ["y1"] = Math.Round(UnitConversion.ToMeters(s.Y), 6),
            ["z1"] = Math.Round(UnitConversion.ToMeters(s.Z), 6),
            ["x2"] = Math.Round(UnitConversion.ToMeters(e.X), 6),
            ["y2"] = Math.Round(UnitConversion.ToMeters(e.Y), 6),
            ["z2"] = Math.Round(UnitConversion.ToMeters(e.Z), 6),
            // CRITICAL FIX: Store diameter in METERS for ChangeLog compatibility
            ["diameter"] = Math.Round(UnitConversion.ToMeters(diameter), 6),
            ["created"] = DateTime.UtcNow,
            ["modified"] = DateTime.UtcNow,
            ["user"] = CommandManager.Instance.SessionId
        };

        SerializeParameters(pipe, dict);
        return dict;
    }

    // Erstellt einen PipeNode aus dem übergebenen Element.
    public static PipeNode ToPipeNode(MEPCurve pipe)
    {
        var dict = ToNode(pipe);
        var node = new PipeNode(
            dict.TryGetValue("uid", out var uidObj) ? uidObj.ToString() ?? string.Empty : string.Empty,
            Convert.ToInt64(dict["elementId"]),
            Convert.ToInt64(dict["typeId"]),
            Convert.ToInt64(dict.GetValueOrDefault("systemTypeId", -1L)),
            Convert.ToInt64(dict["levelId"]),
            Convert.ToDouble(dict["x1"]),
            Convert.ToDouble(dict["y1"]),
            Convert.ToDouble(dict["z1"]),
            Convert.ToDouble(dict["x2"]),
            Convert.ToDouble(dict["y2"]),
            Convert.ToDouble(dict["z2"]),
            Convert.ToDouble(dict.GetValueOrDefault("diameter", 0.0))
        );
            Debug.WriteLine($"[PipeSerializer] Created node for {pipe.UniqueId}");
        return node;
    }
}