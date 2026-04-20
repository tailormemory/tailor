-- TAILOR — Encrypted provider API-key storage.
-- Applied to db/secrets.sqlite3.
--
-- ciphertext is the opaque blob produced by scripts.lib.secrets_crypto.encrypt
-- (nonce || AES-GCM ciphertext || tag). last4 is the tail of the plaintext
-- key in clear, used only to render sk-***abcd in the UI without decrypting.
-- updated_at is a Unix timestamp (seconds) for the "last rotated N days ago"
-- badge. One row per provider — rotation replaces the row in place.

CREATE TABLE IF NOT EXISTS secrets (
    provider   TEXT    PRIMARY KEY,
    ciphertext BLOB    NOT NULL,
    last4      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);
