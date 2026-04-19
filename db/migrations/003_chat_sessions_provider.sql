-- TAILOR — Per-session provider/model selection for the native chat UI.
-- Applied to db/chat_sessions.sqlite3.
--
-- Sessions created before this migration will have NULL provider/model,
-- which is treated at runtime as "use the default brain (llm.* in config)".
-- New sessions may opt into a specific provider/model at creation time.
-- The choice is immutable for the lifetime of the session.

ALTER TABLE chat_sessions ADD COLUMN provider TEXT;
ALTER TABLE chat_sessions ADD COLUMN model TEXT;
