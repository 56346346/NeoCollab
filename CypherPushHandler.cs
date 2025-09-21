using System;
using Autodesk.Revit.UI;
using System.Linq;

#nullable enable



namespace NeoCollab
{
    public class CypherPushHandler : IExternalEventHandler
    {
        public async void Execute(UIApplication app)
        {
            try
            {
                var doc = app.ActiveUIDocument?.Document;
                await CommandManager.Instance.ProcessCypherQueueAsync(doc);


                if (doc != null)
                {
                    // SKIP SOLIBRI VALIDATION for initial session to prevent UI hanging
                    // Initial sessions with large models can cause IFC export to hang
                    var isInitialSession = CommandManager.Instance.IsInitialSession;
                    
                    if (!isInitialSession)
                    {
                        Logger.LogToFile("SOLIBRI VALIDATION: Starting for subsequent session", "solibri.log");
                        // BACKGROUND SOLIBRI VALIDATION: Run Solibri validation in background to prevent UI blocking
                        _ = Task.Run(async () =>
                        {
                            Logger.LogToFile("SOLIBRI VALIDATION: Starting background validation", "solibri.log");
                            await NeoCollabClass.SolibriLock.WaitAsync();
                            try
                            {
                                // NOTE: SolibriRulesetValidator disabled - replaced by SolibriValidationService
                                // var errs = await SolibriRulesetValidator.Validate(doc);
                                // var sev = errs.Count == 0 ? Severity.Info : errs.Max(e => e.Severity);
                                
                                Logger.LogToFile("SOLIBRI VALIDATION: Skipping deprecated SolibriRulesetValidator - validation now handled by SolibriValidationService", "solibri.log");
                                
                                // Note: UI update would need to be done via ExternalEvent for thread safety
                                // For now, just log the result
                            }
                            catch (Exception ex)
                            {
                                Logger.LogCrash("Background Solibri Validation", ex);
                            }
                            finally
                            {
                                NeoCollabClass.SolibriLock.Release();
                            }
                        });
                    }
                    else
                    {
                        Logger.LogToFile("SOLIBRI VALIDATION: Skipped for initial session to prevent UI hanging", "solibri.log");
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.LogCrash("Neo4j Push", ex);
            }
        }

        public string GetName() => "Process Cypher Queue";
    }
}