using System;
using System.Runtime.Versioning;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public record WallNode(
    string Uid,
    long ElementId,
    long TypeId,
    string TypeName,
    string FamilyName,
    long LevelId,
    double X1,
    double Y1,
    double Z1,
    double X2,
    double Y2,
    double Z2,
    double HeightMm,
    double ThicknessMm,
    bool Structural,
    bool Flipped,
    double BaseOffsetMm,
  int LocationLine,
    string RvtClass = "Wall")
{
    // Gibt true zurück, wenn die Wand als tragend markiert ist.
    public static bool IsStructural(Autodesk.Revit.DB.Wall wall)
    {
        var param = wall.get_Parameter(Autodesk.Revit.DB.BuiltInParameter.WALL_STRUCTURAL_USAGE_PARAM);
        if (param != null)
        {
            return param.AsInteger() != 0;
        }
        param = wall.get_Parameter(Autodesk.Revit.DB.BuiltInParameter.WALL_STRUCTURAL_SIGNIFICANT);
        return param?.AsInteger() != 0;
    }
}