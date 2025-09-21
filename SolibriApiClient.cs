using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;
using System.Net;
using NeoCollab;
using System.Text.Json;
using Autodesk.Revit.UI;
using System.Linq;

namespace NeoCollab
{
    /// <summary>
    /// Simplified Solibri REST API client for ChangeLog-based validation
    /// </summary>
    public class SolibriApiClient
    {
        private readonly string _baseUrl;
        private string _currentModelId = string.Empty;
        
        private static readonly HttpClient Http = new HttpClient
        {
            BaseAddress = new Uri("http://localhost:10876/solibri/v1/"),
            Timeout = TimeSpan.FromMinutes(5)
        };

        public SolibriApiClient(int port = 10876)
        {
            _baseUrl = $"http://localhost:{port}/solibri/v1/";
            if (Http.BaseAddress?.ToString() != _baseUrl)
            {
                Http.BaseAddress = new Uri(_baseUrl);
            }
        }

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
        /// funktioniert nicht
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
        ///         /// funktioniert nicht

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
                /// funktioniert nicht

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
            if (string.IsNullOrEmpty(_currentModelId))
                throw new InvalidOperationException("No model loaded");

            var guidList = ifcGuids.ToList();
            if (!guidList.Any())
                return;

            Logger.LogToFile($"SOLIBRI DELETE: Deleting {guidList.Count} components", "solibri.log");
            
            var json = JsonSerializer.Serialize(guidList);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = await Http.PostAsync($"models/{_currentModelId}/deleteComponents", content);
            response.EnsureSuccessStatusCode();
            
            Logger.LogToFile("SOLIBRI DELETE: Components deleted", "solibri.log");
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

        public class ModelInfo
        {
            public string Uuid { get; set; } = string.Empty;
        }
    }
}