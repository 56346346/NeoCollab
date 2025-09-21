using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using System.Runtime.Versioning;

namespace NeoCollab
{
    #region Enums and Data Classes

    public enum ChangeType
    {
        Add,
        Modify,
        Delete
    }

    public class ClashResult
    {
        public string Severity { get; set; }
        public string ComponentGuid { get; set; }
        public string Message { get; set; }
    }

    public class ElementMetadata
    {
        public ElementId Id { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
        public string Uid { get; set; } = string.Empty;
        public string IfcGuid { get; set; } = string.Empty; // For Solibri deletion
    }

    #endregion

    #region Unit Conversion Utilities

    [SupportedOSPlatform("windows")]
    public static class UnitConversion
    {
        // Wandelt Fuß in Millimeter um.
        public static double ToMm(double feet) => UnitUtils.ConvertFromInternalUnits(feet, UnitTypeId.Millimeters);
        
        // Wandelt Millimeter in Revit-Fuß um mit Precision-Rundung.
        public static double ToFt(double mm) => 
            Math.Round(UnitUtils.ConvertToInternalUnits(mm, UnitTypeId.Millimeters), 10);
        
        // Wandelt Fuß in Meter um.
        public static double ToMeters(double feet) => UnitUtils.ConvertFromInternalUnits(feet, UnitTypeId.Meters);
        
        // Wandelt Meter in Revit-Fuß um mit Precision-Rundung.
        public static double FromMeters(double meters) => 
            Math.Round(UnitUtils.ConvertToInternalUnits(meters, UnitTypeId.Meters), 10);
        
        // Präzise Conversion mit Logging
        public static double FromMetersWithLogging(double meters, string context)
        {
            double feetRaw = UnitUtils.ConvertToInternalUnits(meters, UnitTypeId.Meters);
            double feetRounded = Math.Round(feetRaw, 10);
            
            if (Math.Abs(feetRaw - feetRounded) > 1e-12)
            {
                // Logger.LogToFile($"UNIT PRECISION FIX {context}: {meters}m → {feetRaw}ft → {feetRounded}ft (rounded)", "sync.log");
            }
            
            return feetRounded;
        }
    }

    #endregion

    #region Parameter Utilities

    [SupportedOSPlatform("windows")]
    public static class ParameterUtils
    {
        /// <summary>
        /// Name of the shared parameter used to store the Neo4j UID on elements.
        /// </summary>
        public const string Neo4jUidParam = "Neo4jUid";

        /// <summary>
        /// Returns the Neo4j UID for the given element. If the custom parameter is
        /// not present, falls back to <see cref="Element.UniqueId"/>.
        /// </summary>
        public static string GetNeo4jUid(Element elem)
        {
            return elem.LookupParameter(Neo4jUidParam)?.AsString() ?? elem.UniqueId;
        }

        /// <summary>
        /// Writes the Neo4j UID to the element parameter if available.
        /// </summary>
        public static void SetNeo4jUid(Element elem, string uid)
        {
            var p = elem.LookupParameter(Neo4jUidParam);
            if (p != null && !p.IsReadOnly)
                p.Set(uid);
        }

        /// <summary>
        /// Escapes a string for safe usage inside Cypher queries.
        /// Removes backslashes and doubles single quotes.
        /// </summary>
        public static string EscapeForCypher(string input)
        {
            if (string.IsNullOrEmpty(input))
                return string.Empty;
            return input.Replace("\\", string.Empty)
                        .Replace("'", "''")
                        .Replace("\"", "'");
        }

        // Ersetzt problematische Zeichen in Parameternamen durch Unterstriche.
        private static string Sanitize(string name)
        {
            return name
                      .Replace(" ", "_")
                      .Replace("-", "_")
                      .Replace(".", "_")
                      .Replace(":", "_");
        }

        // Schreibt alle Parameter eines Elements in das Dictionary.
        public static void SerializeParameters(Element elem, Dictionary<string, object> dict)
        {
            try
            {
                foreach (Parameter p in elem.GetOrderedParameters())
                {
                    if (p.Definition == null) continue;
                    string key = "param_" + Sanitize(p.Definition.Name);
                    object value = p.StorageType switch
                    {
                        StorageType.Double => p.AsDouble(),
                        StorageType.Integer => p.AsInteger(),
                        StorageType.String => p.AsString() ?? string.Empty,
                        StorageType.ElementId => p.AsElementId().Value,
                        _ => p.AsValueString() ?? string.Empty
                    };
                    dict[key] = value;
                }
            }
            catch
            {
                // ignore serialization errors
            }
        }

        // Überträgt Werte aus dem Dictionary zurück auf die Revit-Parameter.
        public static void ApplyParameters(Element elem, Dictionary<string, object> dict)
        {
            try
            {
                foreach (Parameter p in elem.GetOrderedParameters())
                {
                    if (p.IsReadOnly || p.Definition == null) continue;
                    string key = "param_" + Sanitize(p.Definition.Name);
                    if (!dict.TryGetValue(key, out object? value)) continue;
                    try
                    {
                        switch (p.StorageType)
                        {
                            case StorageType.Double:
                                if (value is double d || double.TryParse(value.ToString(), out d))
                                    p.Set(d);
                                break;
                            case StorageType.Integer:
                                if (value is int i || int.TryParse(value.ToString(), out i))
                                    p.Set(i);
                                break;
                            case StorageType.String:
                                p.Set(value?.ToString() ?? string.Empty);
                                break;
                            case StorageType.ElementId:
                                if (value is long l)
                                    p.Set(new ElementId((int)l));
                                else if (int.TryParse(value.ToString(), out int id))
                                    p.Set(new ElementId(id));
                                break;
                            default:
                                break;
                        }
                    }
                    catch
                    {
                        // ignore if setting fails
                    }
                }
            }
            catch
            {
                // ignore errors
            }
        }

        // Liefert die eingestellte IFC-Entität des Elements, falls vorhanden.
        public static string GetIfcEntity(Element elem)
        {
            try
            {
                var p = elem.get_Parameter(BuiltInParameter.IFC_EXPORT_ELEMENT);
                return p?.AsString() ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        // Prüft anhand des Namens auf einen ProvisionalSpace.
        public static bool IsProvisionalSpaceName(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            string[] patterns =
            {
                "ProvSpace",
                "ProvisionalSpace",
                "Provisional Space",
                "ProvSpaceVoid"
            };

            foreach (var pat in patterns)
            {
                if (name.Contains(pat, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        // Erkennt ProvisionalSpaces anhand mehrerer Kriterien.
        /// <summary>
        /// Determines whether the given element represents a provisional space.
        /// The main indicator is the IFC export type (parameter
        /// <see cref="BuiltInParameter.IFC_EXPORT_ELEMENT"/>), which must contain
        /// "ProvSpace". In addition the name and family are checked for the same
        /// keyword and the category must be GenericModel.
        /// </summary>
        public static bool IsProvisionalSpace(Element elem)
        {
            bool categoryMatch = elem.Category != null &&
    (elem.Category.Id.Value == (int)BuiltInCategory.OST_GenericModel ||
                                  elem.Category.Parent?.Id.Value == (int)BuiltInCategory.OST_GenericModel); 
            bool nameMatch = IsProvisionalSpaceName(elem.Name);
            bool familyMatch = false;
            if (elem is FamilyInstance fi)
            {
                familyMatch = IsProvisionalSpaceName(fi.Symbol?.Name) ||
                              IsProvisionalSpaceName(fi.Symbol?.FamilyName);
            }
            string ifc = GetIfcEntity(elem);
            bool ifcMatch = !string.IsNullOrEmpty(ifc) &&
    (ifc.Contains("provspace", StringComparison.OrdinalIgnoreCase) ||
                 ifc.Equals("IfcBuildingElementProxyType", StringComparison.OrdinalIgnoreCase));
            bool paramMatch = false;
            Parameter? flag = elem.LookupParameter("IsProvisionalSpace") ?? elem.LookupParameter("ProvisionalSpace");
            if (flag != null)
            {
                paramMatch = flag.StorageType switch
                {
                    StorageType.Integer => flag.AsInteger() == 1,
                    StorageType.String => flag.AsString()?.Equals("Yes", StringComparison.OrdinalIgnoreCase) == true,
                    _ => false
                };
            }
            return categoryMatch && (ifcMatch || nameMatch || familyMatch || paramMatch);
        }

        /// <summary>
        /// Variant of <see cref="IsProvisionalSpace(Element)"/> that works on a
        /// property dictionary as retrieved from Neo4j. The dictionary must contain
        /// the property <c>ifcType</c> used to flag provisional spaces.
        /// </summary>
        public static bool IsProvisionalSpace(IDictionary<string, object> props)
        {
            if (props == null)
                return false;

            bool categoryMatch = props.TryGetValue("category", out var catObj) &&
                                catObj is string cat &&
    (cat.Equals("Generic Models", StringComparison.OrdinalIgnoreCase) ||
                                 cat.Equals("Allgemeines Modell", StringComparison.OrdinalIgnoreCase));
            bool ifcMatch = props.TryGetValue("ifcType", out var ifcObj) &&
                           ifcObj is string ifcStr &&
     (ifcStr.Contains("provspace", StringComparison.OrdinalIgnoreCase) ||
                            ifcStr.Equals("IfcBuildingElementProxyType", StringComparison.OrdinalIgnoreCase));
            bool nameMatch = props.TryGetValue("name", out var nameObj) &&
                                     nameObj is string name &&
                                     IsProvisionalSpaceName(name);

            return categoryMatch && (ifcMatch || nameMatch);
        }
    }

    #endregion

    #region Node Extensions

    public static class NodeExtensions
    {
        public static Dictionary<string, object> ToDictionary(this WallNode w) => new()
        {
            ["rvtClass"] = w.RvtClass,
            ["uid"] = w.Uid,
            ["elementId"] = w.ElementId,
            ["typeId"] = w.TypeId,
            ["typeName"] = w.TypeName,
            ["familyName"] = w.FamilyName,
            ["levelId"] = w.LevelId,
            ["x1"] = w.X1,
            ["y1"] = w.Y1,
            ["z1"] = w.Z1,
            ["x2"] = w.X2,
            ["y2"] = w.Y2,
            ["z2"] = w.Z2,
            ["height_mm"] = w.HeightMm,
            ["thickness_mm"] = w.ThicknessMm,
            ["structural"] = w.Structural,
            ["flipped"] = w.Flipped,
            ["base_offset_mm"] = w.BaseOffsetMm,
            ["location_line"] = w.LocationLine
        };

        public static Dictionary<string, object> ToDictionary(this DoorNode d) => new()
        {
            ["name"] = d.Name,
            ["rvtClass"] = d.RvtClass,
            ["uid"] = d.Uid,
            ["elementId"] = d.ElementId,
            ["typeId"] = d.TypeId,
            ["familyName"] = d.FamilyName,
            ["symbolName"] = d.SymbolName,
            ["levelId"] = d.LevelId,
            ["hostId"] = d.HostId,
            ["hostUid"] = d.HostUid,
            ["x"] = d.X,
            ["y"] = d.Y,
            ["z"] = d.Z,
            ["rotation"] = d.Rotation,
            ["width"] = d.Width,
            ["height"] = d.Height,
            ["thickness"] = d.Thickness
        };

        public static Dictionary<string, object> ToDictionary(this PipeNode p) => new()
        {
            ["rvtClass"] = p.RvtClass,
            ["uid"] = p.Uid,
            ["elementId"] = p.ElementId,
            ["typeId"] = p.TypeId,
            ["systemTypeId"] = p.SystemTypeId,
            ["levelId"] = p.LevelId,
            ["x1"] = p.X1,
            ["y1"] = p.Y1,
            ["z1"] = p.Z1,
            ["x2"] = p.X2,
            ["y2"] = p.Y2,
            ["z2"] = p.Z2,
            ["diameter"] = p.Diameter
        };

        public static Dictionary<string, object> ToDictionary(this ProvisionalSpaceNode ps) => new()
        {
            ["rvtClass"] = ps.RvtClass,
            ["guid"] = ps.Guid,
            ["name"] = ps.Name,
            ["familyName"] = ps.FamilyName,
            ["symbolName"] = ps.SymbolName,
            ["width"] = ps.Width,
            ["height"] = ps.Height,
            ["thickness"] = ps.Thickness,
            ["level"] = ps.Level,
            ["levelId"] = ps.LevelId,
            ["x"] = ps.X,
            ["y"] = ps.Y,
            ["z"] = ps.Z,
            ["rotation"] = ps.Rotation,
            ["hostId"] = ps.HostId,
            ["elementId"] = ps.ElementId,
            ["ifcType"] = ps.IfcType,
            ["category"] = ps.Category,
            ["phaseCreated"] = ps.PhaseCreated,
            ["phaseDemolished"] = ps.PhaseDemolished,
            ["bbMinX"] = ps.BbMinX,
            ["bbMinY"] = ps.BbMinY,
            ["bbMinZ"] = ps.BbMinZ,
            ["bbMaxX"] = ps.BbMaxX,
            ["bbMaxY"] = ps.BbMaxY,
            ["bbMaxZ"] = ps.BbMaxZ
        };
    }

    #endregion

    #region Utility Scripts

    /// <summary>
    /// Utility script to clean up invalid ChangeLog entries
    /// </summary>
    public static class CleanupScript
    {
        public static async Task CleanupInvalidChangeLogEntries()
        {
            try
            {
                // Create a simple logger for the connector
                using var loggerFactory = LoggerFactory.Create(builder => builder.AddDebug());
                var logger = loggerFactory.CreateLogger<Neo4jConnector>();
                
                var connector = new Neo4jConnector(logger);
                await connector.CleanupInvalidChangeLogEntriesAsync();
                
                Console.WriteLine("Cleanup completed successfully. Check sync.log for details.");
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Failed to cleanup invalid ChangeLog entries", ex);
                Console.WriteLine($"Cleanup failed: {ex.Message}");
                throw;
            }
        }
    }

    #endregion

    #region IFC Export Handler

    public class IfcExportHandler : IExternalEventHandler
    {
        public Document Document { get; set; }
        public List<ElementId> ElementIds { get; set; }
        public string ExportedPath { get; private set; }

        public void Execute(UIApplication app)
        {
            try
            {
                ExportedPath = new SpaceExtractor(CommandManager.Instance)
                    .ExportIfcSubset(Document, ElementIds);
            }
            catch (Exception ex)
            {
                Autodesk.Revit.UI.TaskDialog.Show("IFC Export Fehler", ex.Message);
                Logger.LogCrash("IFC Export", ex);
            }
        }

        public string GetName() => "IFC Export Handler";
    }

    #endregion
}
