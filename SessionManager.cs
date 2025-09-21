using System;
using System.Collections.Generic;
using Autodesk.Revit.DB;
using System.Linq;

namespace NeoCollab
{
    public class Session
    {
        public Document Document { get; }
        public DateTime LastSyncTime { get; set; }

        public Session(Document doc)
        {
            Document = doc;
            LastSyncTime = CommandManager.Instance.LastSyncTime;
        }
    }

    public static class SessionManager
    {
        private static readonly Dictionary<string, Session> _sessions = new();
        public static IReadOnlyDictionary<string, Session> OpenSessions => _sessions;
        public static string CurrentUserId => CommandManager.Instance.SessionId;

        public static void AddSession(string id, Session session)
        {
            _sessions[id] = session;
        }

        public static void RemoveSession(string id)
        {
            _sessions.Remove(id);
        }
    }
}