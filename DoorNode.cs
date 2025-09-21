using System.Runtime.Versioning;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public record DoorNode(
     string Name,
    string Uid,
    long ElementId,
    long TypeId,
    string FamilyName,
    string SymbolName,
    long LevelId,
    long HostId,
    string HostUid,
    double X,
    double Y,
    double Z,
    double Rotation,
    double Width,
    double Height,
    double Thickness,
    string RvtClass = "Door");