PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_id INTEGER UNIQUE NOT NULL,
    tg_username TEXT,
    first_name TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0,
    registered_at TEXT NOT NULL,
    premium_until TEXT,
    state TEXT,
    state_payload TEXT
);

CREATE TABLE IF NOT EXISTS men (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT,
    tg_username TEXT,
    tg_id TEXT,
    name TEXT NOT NULL,
    description TEXT,
    photo_file_id TEXT,
    is_closed INTEGER NOT NULL DEFAULT 0,
    created_by INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(phone),
    UNIQUE(tg_username),
    UNIQUE(tg_id)
);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    man_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    rating INTEGER NOT NULL,
    text TEXT,
    status TEXT NOT NULL DEFAULT 'APPROVED',
    created_at TEXT NOT NULL,
    updated_at TEXT,
    FOREIGN KEY(man_id) REFERENCES men(id) ON DELETE CASCADE,
    FOREIGN KEY(author_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS access (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    man_id INTEGER NOT NULL,
    granted_at TEXT NOT NULL,
    expires_at TEXT,
    UNIQUE(user_id, man_id),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(man_id) REFERENCES men(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    man_id INTEGER,
    type TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(man_id) REFERENCES men(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO settings(key, value) VALUES
    ('price_week', '200'),
    ('price_month', '500'),
    ('price_single', '99');
