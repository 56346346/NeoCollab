using System.Runtime.Versioning;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public record ProvisionalSpaceNode(
    string Guid,
    string Name,
    string FamilyName,
    string SymbolName,
    double Width,
    double Height,
    double Thickness,
    string Level,
    long LevelId,
    double X,
    double Y,
    double Z,
    double Rotation,
    long HostId,
    int ElementId,  // FIXED: Changed from RevitId to ElementId for consistency
    string IfcType,
    string? Category,
    int PhaseCreated,
    int PhaseDemolished,
    double BbMinX,
    double BbMinY,
    double BbMinZ,
    double BbMaxX,
    double BbMaxY,
double BbMaxZ,
    string RvtClass = "ProvisionalSpace");