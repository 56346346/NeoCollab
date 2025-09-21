using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Autodesk.Revit.DB;
using Autodesk.Revit.UI;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Net;
using System.Text.Json;

namespace NeoCollab
{
    /// <summary>
    /// Complete Solibri integration service handling validation and REST API communication
    /// Consolidates functionality from SolibriValidationService and SolibriApiClient
    /// </summary>
    public class SolibriValidationService
    {
        private readonly string _tempDirectory;
        private readonly string _baseUrl;
        private string _currentModelId = string.Empty;
        private bool _isModelLoaded = false;
        
        private static readonly HttpClient Http = new HttpClient
        {
            BaseAddress = new Uri("http://localhost:10876/solibri/v1/"),
            Timeout = TimeSpan.FromMinutes(5)
        };

        // Timing configuration for ChangeLog synchronization
        public static readonly TimeSpan ChangeLogProcessingDelay = TimeSpan.FromSeconds(3);
        public static readonly TimeSpan ValidationTimeout = TimeSpan.FromMinutes(5);

        public SolibriValidationService(int port = 10876)
        {
            _baseUrl = $"http://localhost:{port}/solibri/v1/";
            if (Http.BaseAddress?.ToString() != _baseUrl)
            {
                Http.BaseAddress = new Uri(_baseUrl);
            }
            _tempDirectory = Path.Combine(Path.GetTempPath(), "NeoCollab", "Solibri");
            Directory.CreateDirectory(_tempDirectory);
        }

        /// <summary>
        /// Validates elements from ChangeLog entries against Solibri ruleset
        /// </summary>
        public async Task<bool> ValidateChangeLogElementsAsync(Document doc, List<ElementId> changedElementIds, List<ElementId> deletedElementIds, List<string> deletedIfcGuids = null)
        {
            try
            {
                Logger.LogToFile($"SOLIBRI VALIDATION: Starting - {changedElementIds.Count} changed, {deletedElementIds.Count} deleted", "solibri.log");
                if (deletedIfcGuids != null && deletedIfcGuids.Count > 0)
                {
                    Logger.LogToFile($"SOLIBRI DELETE: {deletedIfcGuids.Count} IFC-GUIDs found", "solibri.log");
                }

                // Check if Solibri is running
                if (!await PingAsync())
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: Solibri REST API not reachable", "solibri.log");
                    Autodesk.Revit.UI.TaskDialog.Show("Solibri Validation", "Solibri REST API ist nicht erreichbar. Bitte starten Sie Solibri mit --rest-api-server-port=10876 --rest-api-server-http");
                    return false;
                }

                // CRITICAL FIX: Get current model ID from Solibri before operations
                await RetrieveCurrentModelIdAsync();
                Logger.LogToFile($"SOLIBRI MODEL ID: Retrieved current model ID: '{_currentModelId}'", "solibri.log");

                // Export changed elements to IFC (only if there are changed elements)
                string ifcPath = null;
                if (changedElementIds.Any())
                {
                    ifcPath = await ExportElementsToIfcAsync(doc, changedElementIds);
                    if (string.IsNullOrEmpty(ifcPath))
                    {
                        Logger.LogToFile("SOLIBRI VALIDATION: IFC export failed", "solibri.log");
                        return false;
                    }
                    
                    // Upload to Solibri (new model or partial update)
                    string modelId = await UploadIfcAsync(ifcPath, _isModelLoaded);
                    _isModelLoaded = true;
                }
                else
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: No changed elements to export, skipping IFC export", "solibri.log");
                }

                // Delete components if needed
                if (deletedElementIds.Any())
                {
                    List<string> guidsToDelete = new List<string>();
                    
                    if (deletedIfcGuids != null && deletedIfcGuids.Count > 0)
                    {
                        // Use provided IFC-GUIDs
                        guidsToDelete = deletedIfcGuids;
                        Logger.LogToFile($"SOLIBRI DELETE: Using {guidsToDelete.Count} provided IFC-GUIDs", "solibri.log");
                    }
                    else
                    {
                        // Fallback: try to extract from elements (likely to fail for deleted elements)
                        guidsToDelete = GetIfcGuidsForElements(doc, deletedElementIds);
                        Logger.LogToFile($"SOLIBRI DELETE: Extracted {guidsToDelete.Count} IFC-GUIDs from elements", "solibri.log");
                    }
                    
                    if (guidsToDelete.Any())
                    {
                        // Ensure we have a model ID for deletion
                        if (string.IsNullOrEmpty(_currentModelId))
                        {
                            Logger.LogToFile("SOLIBRI DELETE: No current model ID for deletion operation", "solibri.log");
                            Logger.LogToFile("SOLIBRI DELETE: Cannot delete components without model ID", "solibri.log");
                        }
                        else
                        {
                            Logger.LogToFile($"SOLIBRI DELETE: Deleting {guidsToDelete.Count} components from Solibri", "solibri.log");
                            await DeleteComponentsAsync(guidsToDelete);
                        }
                    }
                    else
                    {
                        Logger.LogToFile("SOLIBRI DELETE: No IFC GUIDs found for deleted elements - cannot delete from Solibri", "solibri.log");
                        Logger.LogToFile("SOLIBRI DELETE: This is expected for elements deleted in the same session they were created", "solibri.log");
                    }
                }

                // Ensure we have at least some operation to perform
                bool hasChangedElements = changedElementIds.Any();
                bool hasDeletedElements = deletedElementIds.Any();
                
                if (!hasChangedElements && !hasDeletedElements)
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: No changes to validate", "solibri.log");
                    return true;
                }

                // CRITICAL FIX: Handle Insert/Modify with Partial Update + Selection Basket
                if (hasChangedElements)
                {
                    Logger.LogToFile($"SOLIBRI PARTIAL UPDATE: Processing {changedElementIds.Count} Insert/Modify elements", "solibri.log");
                    
                    // Export partial IFC for changed elements only
                    string partialIfcPath = await ExportPartialIfcAsync(doc, changedElementIds);
                    if (!string.IsNullOrEmpty(partialIfcPath) && File.Exists(partialIfcPath))
                    {
                        Logger.LogToFile($"SOLIBRI PARTIAL UPDATE: Partial IFC exported to {partialIfcPath}", "solibri.log");
                        
                        // Upload partial update to Solibri
                        string partialModelId = await UploadPartialUpdateAsync(partialIfcPath);
                        if (!string.IsNullOrEmpty(partialModelId))
                        {
                            Logger.LogToFile($"SOLIBRI PARTIAL UPDATE: Partial update successful, model ID: {partialModelId}", "solibri.log");
                            
                            // Add changed elements to Selection Basket
                            var changedGuids = ExtractIfcGuidsFromElements(doc, changedElementIds);
                            if (changedGuids.Any())
                            {
                                await SetSelectionBasketAsync(changedGuids);
                                Logger.LogToFile($"SOLIBRI SELECTION BASKET: Added {changedGuids.Count} elements to selection basket", "solibri.log");
                            }
                        }
                    }
                }

                // Start validation only if we have a model to validate
                if (hasChangedElements || !string.IsNullOrEmpty(_currentModelId))
                {
                    // Wait for Solibri to process the uploaded model
                    Logger.LogToFile($"SOLIBRI VALIDATION: Waiting 2 seconds for model processing...", "solibri.log");
                    await Task.Delay(2000);
                    
                    Logger.LogToFile("SOLIBRI VALIDATION: Starting validation run", "solibri.log");
                    await StartCheckingAsync();

                    // Wait for completion
                    bool completed = await WaitForCheckingCompleteAsync(TimeSpan.FromMinutes(5));
                    if (!completed)
                    {
                        Logger.LogToFile("SOLIBRI VALIDATION: Validation timeout", "solibri.log");
                        Autodesk.Revit.UI.TaskDialog.Show("Solibri Validation", "Validierung hat das Zeitlimit überschritten");
                        return false;
                    }

                    // Export BCF results
                    string bcfPath = await ExportBcfXmlAsync(_tempDirectory);

                    // Parse BCF and display in Revit
                    var issues = ParseBcfXml(bcfPath);
                    await DisplayValidationResultsInRevitAsync(doc, issues);

                    Logger.LogToFile($"SOLIBRI VALIDATION: Completed successfully with {issues.Count} issues", "solibri.log");
                }
                else
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: Only deletions performed, no validation needed", "solibri.log");
                }
                
                return true;
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Solibri Validation", ex);
                Autodesk.Revit.UI.TaskDialog.Show("Solibri Validation Error", $"Fehler bei der Validierung: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Exports specified elements to IFC file
        /// </summary>
        private async Task<string> ExportElementsToIfcAsync(Document doc, List<ElementId> elementIds)
        {
            try
            {
                if (!elementIds.Any())
                {
                    // Export entire model if no specific elements
                    elementIds = new FilteredElementCollector(doc)
                        .WhereElementIsNotElementType()
                        .ToElementIds()
                        .ToList();
                }

                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var ifcPath = Path.Combine(_tempDirectory, $"validation_{timestamp}.ifc");

                // Use existing IFC export functionality
                NeoCollabClass.RequestIfcExport(doc, elementIds);
                
                // Wait a moment for export to complete
                await Task.Delay(1000);
                
                var exportedPath = NeoCollabClass.ExportHandler?.ExportedPath;
                if (!string.IsNullOrEmpty(exportedPath) && File.Exists(exportedPath))
                {
                    // Copy to our temp directory
                    File.Copy(exportedPath, ifcPath, true);
                    return ifcPath;
                }

                Logger.LogToFile("SOLIBRI VALIDATION: IFC export failed - no exported file found", "solibri.log");
                return null;
            }
            catch (Exception ex)
            {
                Logger.LogCrash("IFC Export for Solibri", ex);
                return null;
            }
        }

        /// <summary>
        /// Extracts IFC GUIDs for deleted elements (if available)
        /// </summary>
        private List<string> GetIfcGuidsForElements(Document doc, List<ElementId> elementIds)
        {
            var guids = new List<string>();
            
            foreach (var id in elementIds)
            {
                try
                {
                    var element = doc.GetElement(id);
                    if (element != null)
                    {
                        // Try to get IFC GUID parameter
                        var ifcGuidParam = element.get_Parameter(BuiltInParameter.IFC_GUID);
                        if (ifcGuidParam != null && !string.IsNullOrEmpty(ifcGuidParam.AsString()))
                        {
                            guids.Add(ifcGuidParam.AsString());
                        }
                        else
                        {
                            // Fallback: use UniqueId
                            guids.Add(element.UniqueId);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogToFile($"Error getting GUID for element {id}: {ex.Message}", "solibri.log");
                }
            }
            
            return guids;
        }

        /// <summary>
        /// Parses BCF-XML file and extracts validation issues
        /// </summary>
        private List<ValidationIssue> ParseBcfXml(string bcfZipPath)
        {
            var issues = new List<ValidationIssue>();
            
            try
            {
                using var archive = ZipFile.OpenRead(bcfZipPath);
                
                foreach (var entry in archive.Entries.Where(e => e.Name.Equals("markup.bcf", StringComparison.OrdinalIgnoreCase)))
                {
                    using var stream = entry.Open();
                    var xdoc = XDocument.Load(stream);

                    var issue = new ValidationIssue();
                    
                    // Extract basic information
                    issue.Title = xdoc.Descendants("Title").FirstOrDefault()?.Value ?? "Validation Issue";
                    issue.Description = xdoc.Descendants("Description").FirstOrDefault()?.Value ?? "";
                    
                    // Extract severity
                    var severityText = xdoc.Descendants("Priority").FirstOrDefault()?.Value ?? 
                                      xdoc.Descendants("Severity").FirstOrDefault()?.Value ?? "Info";
                    issue.Severity = ParseSeverity(severityText);
                    
                    // Extract component GUIDs
                    var components = xdoc.Descendants("Component")
                        .Select(c => c.Attribute("IfcGuid")?.Value)
                        .Where(guid => !string.IsNullOrEmpty(guid))
                        .ToList();
                    issue.ComponentGuids = components;
                    
                    // Extract viewpoint information if available
                    var viewpoint = xdoc.Descendants("Viewpoints").FirstOrDefault();
                    if (viewpoint != null)
                    {
                        issue.ViewpointFile = viewpoint.Attribute("Viewpoint")?.Value;
                    }
                    
                    issues.Add(issue);
                }
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Parse BCF XML", ex);
            }
            
            return issues;
        }

        /// <summary>
        /// Displays validation results as text fields in both Revit sessions
        /// </summary>
        private Task DisplayValidationResultsInRevitAsync(Document doc, List<ValidationIssue> issues)
        {
            return Task.Run(() =>
            {
                try
                {
                if (!issues.Any())
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: No issues found - displaying success message", "solibri.log");
                    
                    // Create success text field
                    using (var trans = new Transaction(doc, "Solibri Validation - Success"))
                    {
                        trans.Start();
                        CreateValidationTextField(doc, "✓ Solibri Validation Passed", "No issues found in validation", ValidationSeverity.Success);
                        trans.Commit();
                    }
                    return;
                }

                Logger.LogToFile($"SOLIBRI VALIDATION: Displaying {issues.Count} issues in Revit", "solibri.log");

                using (var trans = new Transaction(doc, "Solibri Validation Results"))
                {
                    trans.Start();
                    
                    int issueNumber = 1;
                    foreach (var issue in issues.Take(10)) // Limit to first 10 issues to avoid clutter
                    {
                        var title = $"Issue {issueNumber}: {issue.Title}";
                        var description = $"{issue.Severity}: {issue.Description}";
                        
                        if (issue.ComponentGuids.Any())
                        {
                            description += $"\nAffected Components: {string.Join(", ", issue.ComponentGuids.Take(3))}";
                            if (issue.ComponentGuids.Count > 3)
                                description += $" (and {issue.ComponentGuids.Count - 3} more)";
                        }
                        
                        CreateValidationTextField(doc, title, description, issue.Severity);
                        issueNumber++;
                    }
                    
                    if (issues.Count > 10)
                    {
                        CreateValidationTextField(doc, $"Additional Issues", $"{issues.Count - 10} more issues found. Check BCF export for details.", ValidationSeverity.Warning);
                    }
                    
                    trans.Commit();
                }
                
                // Show summary dialog
                var summary = $"Solibri Validation completed with {issues.Count} issues:\n";
                var errorCount = issues.Count(i => i.Severity == ValidationSeverity.Error);
                var warningCount = issues.Count(i => i.Severity == ValidationSeverity.Warning);
                var infoCount = issues.Count(i => i.Severity == ValidationSeverity.Info);
                
                summary += $"• {errorCount} Errors\n• {warningCount} Warnings\n• {infoCount} Info";
                
                Autodesk.Revit.UI.TaskDialog.Show("Solibri Validation Results", summary);
                }
                catch (Exception ex)
                {
                    Logger.LogCrash("Display Validation Results", ex);
                }
            });
        }

        /// <summary>
        /// Creates a text field in Revit to display validation issue
        /// </summary>
        private void CreateValidationTextField(Document doc, string title, string description, ValidationSeverity severity)
        {
            try
            {
                // Find a text note type
                var textNoteType = new FilteredElementCollector(doc)
                    .OfClass(typeof(TextNoteType))
                    .FirstOrDefault() as TextNoteType;
                
                if (textNoteType == null)
                {
                    Logger.LogToFile("SOLIBRI VALIDATION: No TextNoteType found", "solibri.log");
                    return;
                }

                // Create text content
                var content = $"{title}\n{description}";
                
                // Position text notes in a column
                var basePoint = new XYZ(0, 0, 0);
                var existingTextNotes = new FilteredElementCollector(doc)
                    .OfClass(typeof(TextNote))
                    .Cast<TextNote>()
                    .Where(tn => tn.Text.StartsWith("Issue ") || tn.Text.StartsWith("✓ Solibri") || tn.Text.StartsWith("Additional Issues"))
                    .Count();
                
                var offset = new XYZ(0, -existingTextNotes * 10, 0); // 10 feet between text notes
                var position = basePoint + offset;
                
                // Create text note
                var textNote = TextNote.Create(doc, doc.ActiveView.Id, position, content, textNoteType.Id);
                
                Logger.LogToFile($"SOLIBRI VALIDATION: Created text field '{title}' at position {position}", "solibri.log");
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"Error creating text field: {ex.Message}", "solibri.log");
            }
        }

        private ValidationSeverity ParseSeverity(string severityText)
        {
            if (string.IsNullOrEmpty(severityText))
                return ValidationSeverity.Info;
                
            severityText = severityText.ToUpperInvariant();
            
            if (severityText.Contains("ERROR") || severityText.Contains("CRITICAL") || severityText.Contains("HIGH"))
                return ValidationSeverity.Error;
            else if (severityText.Contains("WARNING") || severityText.Contains("MEDIUM") || severityText.Contains("WARN"))
                return ValidationSeverity.Warning;
            else
                return ValidationSeverity.Info;
        }

        #region Solibri REST API Methods (consolidated from SolibriApiClient)

        /// <summary>
        /// Tests if Solibri REST API is reachable
        /// </summary>
        public async Task<bool> PingAsync()
        {
            try
            {
                var response = await Http.GetAsync("ping");
                return response.IsSuccessStatusCode;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Uploads IFC file as new model or partial update
        /// </summary>
        public async Task<string> UploadIfcAsync(string ifcFilePath, bool isPartialUpdate = false)
        {
            if (string.IsNullOrWhiteSpace(ifcFilePath) || !File.Exists(ifcFilePath))
                throw new ArgumentException("IFC file path is invalid", nameof(ifcFilePath));

            Logger.LogToFile($"SOLIBRI UPLOAD: {(isPartialUpdate ? "Partial update" : "New model")} - {ifcFilePath}", "solibri.log");

            using var fs = File.OpenRead(ifcFilePath);
            var content = new StreamContent(fs);
            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            HttpResponseMessage response;
            
            if (isPartialUpdate && !string.IsNullOrEmpty(_currentModelId))
            {
                // Partial update for existing model
                response = await Http.PutAsync($"models/{_currentModelId}/partialUpdate", content);
                
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    // Model doesn't exist anymore, create new one
                    Logger.LogToFile($"SOLIBRI UPLOAD: Model {_currentModelId} not found, creating new model", "solibri.log");
                    var modelName = Path.GetFileNameWithoutExtension(ifcFilePath);
                    response = await Http.PostAsync($"models?name={WebUtility.UrlEncode(modelName)}", content);
                    _currentModelId = await ExtractModelIdFromResponse(response);
                }
            }
            else
            {
                // New model
                var modelName = Path.GetFileNameWithoutExtension(ifcFilePath);
                response = await Http.PostAsync($"models?name={WebUtility.UrlEncode(modelName)}", content);
                _currentModelId = await ExtractModelIdFromResponse(response);
            }

            response.EnsureSuccessStatusCode();
            Logger.LogToFile($"SOLIBRI UPLOAD: Success - Model ID: {_currentModelId}", "solibri.log");
            return _currentModelId;
        }

        /// <summary>
        /// Starts validation/checking for the current model
        /// </summary>
        public async Task StartCheckingAsync()
        {
            Logger.LogToFile("SOLIBRI CHECKING: Starting validation", "solibri.log");
            
            var response = await Http.PostAsync("checking?checkSelected=false", null);
            response.EnsureSuccessStatusCode();
            
            Logger.LogToFile("SOLIBRI CHECKING: Validation started", "solibri.log");
        }

        /// <summary>
        /// Waits for checking to complete by polling status
        /// </summary>
        public async Task<bool> WaitForCheckingCompleteAsync(TimeSpan timeout)
        {
            Logger.LogToFile("SOLIBRI CHECKING: Waiting for completion", "solibri.log");
            
            var startTime = DateTime.Now;
            while (DateTime.Now - startTime < timeout)
            {
                var response = await Http.GetAsync("status");
                response.EnsureSuccessStatusCode();
                
                var statusJson = await response.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(statusJson);
                
                if (doc.RootElement.TryGetProperty("busy", out var busyElement) && !busyElement.GetBoolean())
                {
                    Logger.LogToFile("SOLIBRI CHECKING: Validation completed", "solibri.log");
                    return true;
                }
                
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
            
            Logger.LogToFile("SOLIBRI CHECKING: Timeout waiting for completion", "solibri.log");
            return false;
        }

        /// <summary>
        /// Exports BCF-XML with validation results
        /// </summary>
        public async Task<string> ExportBcfXmlAsync(string outputDirectory)
        {
            if (string.IsNullOrWhiteSpace(outputDirectory))
                throw new ArgumentException("Output directory is required", nameof(outputDirectory));

            if (!Directory.Exists(outputDirectory))
                Directory.CreateDirectory(outputDirectory);

            Logger.LogToFile("SOLIBRI BCF: Exporting BCF-XML", "solibri.log");
            
            var response = await Http.GetAsync("bcfxml/two_one?scope=all");
            response.EnsureSuccessStatusCode();

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var fileName = $"solibri_validation_{timestamp}.bcfzip";
            var filePath = Path.Combine(outputDirectory, fileName);

            using (var fs = File.Create(filePath))
            {
                await response.Content.CopyToAsync(fs);
            }

            Logger.LogToFile($"SOLIBRI BCF: Exported to {filePath}", "solibri.log");
            return filePath;
        }

        /// <summary>
        /// Deletes components from the model (for deleted elements)
        /// </summary>
        public async Task DeleteComponentsAsync(IEnumerable<string> ifcGuids)
        {
            Logger.LogToFile($"=== SOLIBRI DELETE START ===", "solibri.log");
            Logger.LogToFile($"SOLIBRI DELETE: Current model ID: '{_currentModelId}'", "solibri.log");
            
            if (string.IsNullOrEmpty(_currentModelId))
            {
                Logger.LogToFile("SOLIBRI DELETE: No model loaded, cannot delete components", "solibri.log");
                throw new InvalidOperationException("No model loaded");
            }

            var guidList = ifcGuids.ToList();
            Logger.LogToFile($"SOLIBRI DELETE: Attempting to delete {guidList.Count} components", "solibri.log");
            Logger.LogToFile($"SOLIBRI DELETE: Component GUIDs: [{string.Join(", ", guidList)}]", "solibri.log");
            
            if (!guidList.Any())
            {
                Logger.LogToFile("SOLIBRI DELETE: No components to delete", "solibri.log");
                return;
            }

            try
            {
                var json = JsonSerializer.Serialize(guidList);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                
                var url = $"models/{_currentModelId}/deleteComponents";
                Logger.LogToFile($"SOLIBRI DELETE: Sending POST to: {_baseUrl}{url}", "solibri.log");
                Logger.LogToFile($"SOLIBRI DELETE: Request payload: {json}", "solibri.log");
                
                var response = await Http.PostAsync(url, content);
                
                Logger.LogToFile($"SOLIBRI DELETE: Response status: {response.StatusCode} ({(int)response.StatusCode})", "solibri.log");
                
                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    Logger.LogToFile($"SOLIBRI DELETE: Success! Response: '{responseContent}'", "solibri.log");
                }
                else
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Logger.LogToFile($"SOLIBRI DELETE: Failed with status {response.StatusCode}: {error}", "solibri.log");
                    throw new HttpRequestException($"Failed to delete components: {response.StatusCode} - {error}");
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SOLIBRI DELETE: Exception during deletion: {ex.Message}", "solibri.log");
                throw;
            }
        }

        private async Task<string> ExtractModelIdFromResponse(HttpResponseMessage response)
        {
            // Try to get model ID from response body first
            try
            {
                var responseJson = await response.Content.ReadAsStringAsync();
                var modelInfo = JsonSerializer.Deserialize<ModelInfo>(responseJson);
                if (!string.IsNullOrEmpty(modelInfo?.Uuid))
                    return modelInfo.Uuid;
            }
            catch
            {
                // Ignore JSON parsing errors
            }

            // Fallback: extract from Location header
            var locationHeader = response.Headers.Location?.ToString();
            if (!string.IsNullOrEmpty(locationHeader))
            {
                var parts = locationHeader.Split('/');
                return parts[^1];
            }

            throw new Exception("Could not extract model ID from response");
        }

        private Task<string> ExportPartialIfcAsync(Document doc, List<ElementId> elementIds)
        {
            try
            {
                Logger.LogToFile($"SOLIBRI PARTIAL IFC: Exporting {elementIds.Count} elements for partial update", "solibri.log");
                
                // Use SpaceExtractor to export IFC subset
                var extractor = new SpaceExtractor(CommandManager.Instance);
                string ifcPath = extractor.ExportIfcSubset(doc, elementIds);
                
                if (!string.IsNullOrEmpty(ifcPath) && File.Exists(ifcPath))
                {
                    Logger.LogToFile($"SOLIBRI PARTIAL IFC: Successfully exported to {ifcPath}", "solibri.log");
                    return Task.FromResult(ifcPath);
                }
                else
                {
                    Logger.LogToFile("SOLIBRI PARTIAL IFC: Export failed - no file created", "solibri.log");
                    return Task.FromResult<string>(null);
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SOLIBRI PARTIAL IFC: Export failed: {ex.Message}", "solibri.log");
                return Task.FromResult<string>(null);
            }
        }

        private async Task<string> UploadPartialUpdateAsync(string ifcPath)
        {
            try
            {
                Logger.LogToFile($"SOLIBRI PARTIAL UPLOAD: Uploading partial update from {ifcPath}", "solibri.log");
                
                // Use existing upload but as partial update if model already exists
                if (!string.IsNullOrEmpty(_currentModelId))
                {
                    return await UploadIfcAsync(ifcPath, true); // true = partial update
                }
                else
                {
                    return await UploadIfcAsync(ifcPath, false); // false = new model
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SOLIBRI PARTIAL UPLOAD: Upload failed: {ex.Message}", "solibri.log");
                return null;
            }
        }

        private List<string> ExtractIfcGuidsFromElements(Document doc, List<ElementId> elementIds)
        {
            var guids = new List<string>();
            try
            {
                foreach (var elementId in elementIds)
                {
                    var element = doc.GetElement(elementId);
                    if (element != null)
                    {
                        // Get IFC GUID from element
                        var ifcGuidParam = element.get_Parameter(BuiltInParameter.IFC_GUID);
                        if (ifcGuidParam != null && !string.IsNullOrEmpty(ifcGuidParam.AsString()))
                        {
                            guids.Add(ifcGuidParam.AsString());
                        }
                    }
                }
                Logger.LogToFile($"SOLIBRI GUID EXTRACTION: Extracted {guids.Count} IFC GUIDs from {elementIds.Count} elements", "solibri.log");
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SOLIBRI GUID EXTRACTION: Failed: {ex.Message}", "solibri.log");
            }
            return guids;
        }

        private async Task SetSelectionBasketAsync(List<string> ifcGuids)
        {
            try
            {
                Logger.LogToFile($"SOLIBRI SELECTION BASKET: Setting selection basket with {ifcGuids.Count} GUIDs", "solibri.log");
                
                var json = JsonSerializer.Serialize(ifcGuids);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                
                var response = await Http.PostAsync("selectionBasket", content);
                
                if (response.IsSuccessStatusCode)
                {
                    Logger.LogToFile($"SOLIBRI SELECTION BASKET: Successfully set selection basket", "solibri.log");
                }
                else
                {
                    var error = await response.Content.ReadAsStringAsync();
                    Logger.LogToFile($"SOLIBRI SELECTION BASKET: Failed to set selection basket: {response.StatusCode} - {error}", "solibri.log");
                }
            }
            catch (Exception ex)
            {
                Logger.LogToFile($"SOLIBRI SELECTION BASKET: Exception: {ex.Message}", "solibri.log");
            }
        }

        private async Task RetrieveCurrentModelIdAsync()
        {
            try
            {
                Logger.LogToFile("SOLIBRI MODEL ID: Retrieving current model ID from Solibri", "solibri.log");
                
                var response = await Http.GetAsync("models");
                
                if (response.IsSuccessStatusCode)
                {
                    var jsonContent = await response.Content.ReadAsStringAsync();
                    var models = JsonSerializer.Deserialize<ModelInfo[]>(jsonContent);
                    
                    if (models != null && models.Length > 0)
                    {
                        _currentModelId = models[0].Uuid;
                        Logger.LogToFile($"SOLIBRI MODEL ID: Found model ID: {_currentModelId}", "solibri.log");
                    }
                    else
                    {
                        _currentModelId = string.Empty;
                        Logger.LogToFile("SOLIBRI MODEL ID: No models found in Solibri", "solibri.log");
                    }
                }
                else
                {
                    _currentModelId = string.Empty;
                    Logger.LogToFile($"SOLIBRI MODEL ID: Failed to retrieve models: {response.StatusCode}", "solibri.log");
                }
            }
            catch (Exception ex)
            {
                _currentModelId = string.Empty;
                Logger.LogToFile($"SOLIBRI MODEL ID: Exception retrieving model ID: {ex.Message}", "solibri.log");
            }
        }

        public class ModelInfo
        {
            public string Uuid { get; set; } = string.Empty;
        }

        #endregion
    }

    public enum ValidationSeverity
    {
        Success,
        Info,
        Warning,
        Error
    }

    public class ValidationIssue
    {
        public string Title { get; set; } = "";
        public string Description { get; set; } = "";
        public ValidationSeverity Severity { get; set; } = ValidationSeverity.Info;
        public List<string> ComponentGuids { get; set; } = new List<string>();
        public string ViewpointFile { get; set; } = "";
    }
}
