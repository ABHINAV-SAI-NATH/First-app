/*
 * Milk Ledger sync backend.
 *
 * A tiny, keyless sync server: no user accounts, no login. Anyone who
 * has a "space" ID (the Sync Code shown in the app) can push/pull that
 * space's data. That's the same trust model the app used with jsonblob
 * before — the security comes entirely from the ID being an unguessable
 * random string, not from authentication. If you want real access
 * control, add an auth check to each route below.
 *
 * Storage: SQLite via better-sqlite3, a single file (data.db) on disk.
 * SQLite comfortably handles multiple GB — there is no 10KB-style size
 * cap here, unlike jsonblob's free tier.
 *
 * Only records CHANGED since a device's last sync are ever transmitted
 * (delta sync), keyed by each store's own key field and an `updatedAt`
 * timestamp already present on every record the app produces. Pull
 * endpoints are paginated so a brand-new device joining a space with a
 * lot of history doesn't get an enormous single response.
 */
const express = require('express');
const Database = require('better-sqlite3');
const crypto = require('crypto');
const path = require('path');

const app = express();
app.use(express.json({ limit: '25mb' }));

// Basic CORS so the app (served from a different origin, e.g. a static
// host or file://) can call this API. Tighten this if you want to lock
// it down to a specific origin.
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const db = new Database(path.join(__dirname, 'data.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS spaces (
    id TEXT PRIMARY KEY,
    buy_price REAL DEFAULT 0,
    sell_price REAL DEFAULT 0,
    created_at INTEGER,
    last_active INTEGER
  );
  CREATE TABLE IF NOT EXISTS records (
    space_id TEXT NOT NULL,
    store TEXT NOT NULL,
    rkey TEXT NOT NULL,
    data TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (space_id, store, rkey)
  );
  CREATE INDEX IF NOT EXISTS idx_records_lookup ON records(space_id, store, updated_at);
  CREATE TABLE IF NOT EXISTS tombstones (
    space_id TEXT NOT NULL,
    store TEXT NOT NULL,
    rkey TEXT NOT NULL,
    ts INTEGER NOT NULL,
    PRIMARY KEY (space_id, store, rkey)
  );
  CREATE INDEX IF NOT EXISTS idx_tomb_lookup ON tombstones(space_id, ts);
`);

const STORES = ['farmers', 'entries', 'customers', 'sales', 'saleDays'];
const KEY_FIELD = { farmers: 'udNumber', entries: 'id', customers: 'id', sales: 'id', saleDays: 'date' };
const PAGE_LIMIT = 2000;

function genId() {
  return crypto.randomBytes(12).toString('hex');
}

function getSpace(id) {
  return db.prepare('SELECT * FROM spaces WHERE id = ?').get(id);
}

// ---- Create a new sync space ----
app.post('/api/spaces', (req, res) => {
  const id = genId();
  const now = Date.now();
  const buyPrice = Number(req.body?.buyPrice) || 0;
  const sellPrice = Number(req.body?.sellPrice) || 0;
  db.prepare('INSERT INTO spaces (id, buy_price, sell_price, created_at, last_active) VALUES (?,?,?,?,?)')
    .run(id, buyPrice, sellPrice, now, now);
  res.json({ id, ts: now });
});

// ---- Push changed records + deletions + settings ----
app.post('/api/spaces/:id/push', (req, res) => {
  const spaceId = req.params.id;
  if (!getSpace(spaceId)) return res.status(404).json({ error: 'space not found' });

  const now = Date.now();
  const body = req.body || {};

  const upsertRec = db.prepare(`
    INSERT INTO records (space_id, store, rkey, data, updated_at) VALUES (?,?,?,?,?)
    ON CONFLICT(space_id, store, rkey) DO UPDATE SET
      data = excluded.data, updated_at = excluded.updated_at
    WHERE excluded.updated_at > records.updated_at
  `);
  const upsertTomb = db.prepare(`
    INSERT INTO tombstones (space_id, store, rkey, ts) VALUES (?,?,?,?)
    ON CONFLICT(space_id, store, rkey) DO UPDATE SET ts = excluded.ts
    WHERE excluded.ts > tombstones.ts
  `);
  const delRec = db.prepare('DELETE FROM records WHERE space_id=? AND store=? AND rkey=? AND updated_at<=?');

  const txn = db.transaction(() => {
    for (const store of STORES) {
      const recs = Array.isArray(body.stores?.[store]) ? body.stores[store] : [];
      const keyField = KEY_FIELD[store];
      for (const rec of recs) {
        if (!rec || rec[keyField] == null) continue;
        const key = String(rec[keyField]);
        const updatedAt = Number(rec.updatedAt) || now;
        upsertRec.run(spaceId, store, key, JSON.stringify(rec), updatedAt);
      }
    }
    const tombs = Array.isArray(body.tombstones) ? body.tombstones : [];
    for (const t of tombs) {
      if (!t || !STORES.includes(t.s)) continue;
      const ts = Number(t.t) || now;
      upsertTomb.run(spaceId, t.s, String(t.k), ts);
      delRec.run(spaceId, t.s, String(t.k), ts);
    }
    if (typeof body.buyPrice === 'number' || typeof body.sellPrice === 'number') {
      db.prepare('UPDATE spaces SET buy_price=COALESCE(?,buy_price), sell_price=COALESCE(?,sell_price), last_active=? WHERE id=?')
        .run(
          typeof body.buyPrice === 'number' ? body.buyPrice : null,
          typeof body.sellPrice === 'number' ? body.sellPrice : null,
          now, spaceId
        );
    } else {
      db.prepare('UPDATE spaces SET last_active=? WHERE id=?').run(now, spaceId);
    }
  });
  txn();

  res.json({ ok: true, serverTime: now });
});

// ---- Pull one store's changes since `since`, paginated ----
app.get('/api/spaces/:id/store/:store/pull', (req, res) => {
  const spaceId = req.params.id;
  const store = req.params.store;
  if (!STORES.includes(store)) return res.status(400).json({ error: 'unknown store' });
  if (!getSpace(spaceId)) return res.status(404).json({ error: 'space not found' });

  const since = Number(req.query.since) || 0;
  const limit = Math.min(Number(req.query.limit) || PAGE_LIMIT, PAGE_LIMIT);

  const rows = db.prepare(
    'SELECT data FROM records WHERE space_id=? AND store=? AND updated_at>? ORDER BY updated_at ASC LIMIT ?'
  ).all(spaceId, store, since, limit + 1);

  let more = false;
  if (rows.length > limit) { more = true; rows.length = limit; }

  db.prepare('UPDATE spaces SET last_active=? WHERE id=?').run(Date.now(), spaceId);
  res.json({ rows: rows.map(r => JSON.parse(r.data)), more });
});

// ---- Pull tombstones (deletions) since `since`, paginated ----
app.get('/api/spaces/:id/tombstones', (req, res) => {
  const spaceId = req.params.id;
  if (!getSpace(spaceId)) return res.status(404).json({ error: 'space not found' });

  const since = Number(req.query.since) || 0;
  const limit = Math.min(Number(req.query.limit) || PAGE_LIMIT, PAGE_LIMIT);

  const rows = db.prepare(
    'SELECT store, rkey, ts FROM tombstones WHERE space_id=? AND ts>? ORDER BY ts ASC LIMIT ?'
  ).all(spaceId, since, limit + 1);

  let more = false;
  if (rows.length > limit) { more = true; rows.length = limit; }

  res.json({ rows: rows.map(r => ({ s: r.store, k: r.rkey, t: r.ts })), more });
});

// ---- Current default prices for a space ----
app.get('/api/spaces/:id/settings', (req, res) => {
  const space = getSpace(req.params.id);
  if (!space) return res.status(404).json({ error: 'space not found' });
  res.json({ buyPrice: space.buy_price, sellPrice: space.sell_price, serverTime: Date.now() });
});

app.get('/health', (req, res) => res.json({ ok: true, time: Date.now() }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Milk Ledger sync backend listening on port ' + PORT));
