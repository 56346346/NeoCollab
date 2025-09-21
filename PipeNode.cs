using System.Runtime.Versioning;

namespace NeoCollab;

[SupportedOSPlatform("windows")]
public record PipeNode(
    string Uid,
    long ElementId,
    long TypeId,
    long SystemTypeId,
    long LevelId,
    double X1,
    double Y1,
    double Z1,
    double X2,
    double Y2,
    double Z2,
  double Diameter,
    string RvtClass = "Pipe");