// ═══════════════════════════════════════════════════════
//  JalRakshak — Real Backend Server
//  Node.js + Express + SQLite + WebSocket
// ═══════════════════════════════════════════════════════

const express = require('express');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const Database = require('better-sqlite3');
const cron = require('node-cron');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../frontend')));

// ─────────────────────────────────────────
//  DATABASE SETUP (SQLite)
// ─────────────────────────────────────────
const db = new Database('./jalrakshak.db');

db.exec(`
  CREATE TABLE IF NOT EXISTS readings (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    hour       INTEGER NOT NULL,
    usage      REAL    NOT NULL,
    baseline   REAL    NOT NULL,
    deviation  REAL    NOT NULL,
    recorded_at TEXT   DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS anomalies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    hour        INTEGER NOT NULL,
    slot_label  TEXT    NOT NULL,
    current_val REAL    NOT NULL,
    baseline    REAL    NOT NULL,
    deviation   REAL    NOT NULL,
    severity    TEXT    NOT NULL,
    detected_at TEXT    DEFAULT (datetime('now','localtime'))
  );

  CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );

  INSERT OR IGNORE INTO settings VALUES ('anomaly_threshold', '20');
  INSERT OR IGNORE INTO settings VALUES ('high_alert_threshold', '50');
  INSERT OR IGNORE INTO settings VALUES ('morning_baseline', '50');
`);

// ─────────────────────────────────────────
//  BASELINE DATA (24-hour pattern in L/hr)
// ─────────────────────────────────────────
const BASELINE = [8,6,5,4,5,12,35,50,45,30,20,18,
                  15,16,18,20,25,35,42,38,30,25,18,12];

// Current live usage (in-memory, updated by sensor simulation)
let liveUsage = [...BASELINE].map(b => +(b + (Math.random()-0.5)*b*0.1).toFixed(1));

function getSettings() {
  const rows = db.prepare('SELECT key, value FROM settings').all();
  const s = {};
  rows.forEach(r => s[r.key] = parseFloat(r.value));
  return s;
}

function getSlotLabel(h) {
  if (h >= 5 && h < 9)   return `Morning (${h}:00)`;
  if (h >= 9 && h < 12)  return `Mid-Morning (${h}:00)`;
  if (h >= 12 && h < 14) return `Noon (${h}:00)`;
  if (h >= 14 && h < 18) return `Afternoon (${h}:00)`;
  if (h >= 18 && h < 22) return `Evening (${h}:00)`;
  return `Night (${h}:00)`;
}

// ─────────────────────────────────────────
//  ANOMALY DETECTION ENGINE
// ─────────────────────────────────────────
function runAnomalyDetection() {
  const now = new Date();
  const h = now.getHours();
  const current = liveUsage[h];
  const baseline = BASELINE[h];
  const settings = getSettings();

  const deviation = ((current - baseline) / baseline) * 100;
  const absDeviation = Math.abs(deviation);

  // Save reading to DB
  db.prepare(`INSERT INTO readings (hour, usage, baseline, deviation) VALUES (?,?,?,?)`)
    .run(h, current, baseline, +deviation.toFixed(2));

  let anomalyDetected = null;

  if (absDeviation >= settings.anomaly_threshold) {
    const severity = absDeviation >= settings.high_alert_threshold ? 'high' : 'medium';

    // Check if same anomaly already logged in last 5 minutes
    const recent = db.prepare(`
      SELECT * FROM anomalies
      WHERE hour = ? AND severity = ?
        AND detected_at > datetime('now', '-5 minutes', 'localtime')
    `).get(h, severity);

    if (!recent) {
      const result = db.prepare(`
        INSERT INTO anomalies (hour, slot_label, current_val, baseline, deviation, severity)
        VALUES (?,?,?,?,?,?)
      `).run(h, getSlotLabel(h), current, baseline, +deviation.toFixed(2), severity);

      anomalyDetected = {
        id: result.lastInsertRowid,
        hour: h,
        slot: getSlotLabel(h),
        current: current,
        baseline: baseline,
        deviation: +deviation.toFixed(2),
        severity,
        time: now.toLocaleTimeString()
      };
    }
  }

  // Broadcast to all WebSocket clients
  const payload = JSON.stringify({
    type: 'UPDATE',
    data: {
      hour: h,
      current,
      baseline,
      deviation: +deviation.toFixed(2),
      liveUsage: [...liveUsage],
      anomaly: anomalyDetected,
      timestamp: now.toISOString()
    }
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });

  if (anomalyDetected) {
    console.log(`[ANOMALY] ${anomalyDetected.severity.toUpperCase()} — ${anomalyDetected.slot} | ${current} L/hr (+${deviation.toFixed(1)}%)`);
  }
}

// Simulate live sensor fluctuation every 3 seconds
function simulateSensor() {
  const h = new Date().getHours();
  const noise = (Math.random() - 0.5) * BASELINE[h] * 0.08;
  liveUsage[h] = Math.max(1, +(liveUsage[h] + noise).toFixed(1));
  runAnomalyDetection();
}

// Run sensor every 3 seconds
setInterval(simulateSensor, 3000);

// Daily reset at midnight
cron.schedule('0 0 * * *', () => {
  liveUsage = [...BASELINE].map(b => +(b + (Math.random()-0.5)*b*0.1).toFixed(1));
  console.log('[CRON] Daily usage data reset');
});

// ═══════════════════════════════════════════════════════
//  REST API ROUTES
// ═══════════════════════════════════════════════════════

// GET /api/status — health check
app.get('/api/status', (req, res) => {
  res.json({ status: 'ok', server: 'JalRakshak Backend v1.0', uptime: process.uptime() });
});

// GET /api/live — current live readings
app.get('/api/live', (req, res) => {
  const h = new Date().getHours();
  const settings = getSettings();
  const current = liveUsage[h];
  const baseline = BASELINE[h];
  const deviation = ((current - baseline) / baseline) * 100;

  res.json({
    hour: h,
    current,
    baseline,
    deviation: +deviation.toFixed(2),
    liveUsage,
    baseline24h: BASELINE,
    settings,
    timestamp: new Date().toISOString()
  });
});

// GET /api/readings?limit=100 — historical readings
app.get('/api/readings', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const rows = db.prepare(`
    SELECT * FROM readings ORDER BY id DESC LIMIT ?
  `).all(limit);
  res.json({ count: rows.length, data: rows });
});

// GET /api/anomalies?limit=50 — anomaly history
app.get('/api/anomalies', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const rows = db.prepare(`
    SELECT * FROM anomalies ORDER BY id DESC LIMIT ?
  `).all(limit);
  res.json({ count: rows.length, data: rows });
});

// GET /api/anomalies/today — today's anomalies
app.get('/api/anomalies/today', (req, res) => {
  const rows = db.prepare(`
    SELECT * FROM anomalies
    WHERE date(detected_at) = date('now','localtime')
    ORDER BY id DESC
  `).all();
  res.json({ count: rows.length, data: rows });
});

// GET /api/stats — summary statistics
app.get('/api/stats', (req, res) => {
  const today = db.prepare(`
    SELECT COUNT(*) as total_readings,
           AVG(usage) as avg_usage,
           MAX(usage) as peak_usage,
           MIN(usage) as min_usage,
           SUM(usage) as total_liters
    FROM readings
    WHERE date(recorded_at) = date('now','localtime')
  `).get();

  const anomaliesToday = db.prepare(`
    SELECT COUNT(*) as count FROM anomalies
    WHERE date(detected_at) = date('now','localtime')
  `).get();

  const highAlerts = db.prepare(`
    SELECT COUNT(*) as count FROM anomalies
    WHERE severity = 'high' AND date(detected_at) = date('now','localtime')
  `).get();

  res.json({
    today: {
      ...today,
      anomalies: anomaliesToday.count,
      high_alerts: highAlerts.count
    },
    liveHour: new Date().getHours(),
    currentUsage: liveUsage[new Date().getHours()]
  });
});

// POST /api/readings/manual — manually add a reading (for real sensor)
app.post('/api/readings/manual', (req, res) => {
  const { hour, usage } = req.body;
  if (hour === undefined || usage === undefined) {
    return res.status(400).json({ error: 'hour and usage are required' });
  }
  if (hour < 0 || hour > 23) {
    return res.status(400).json({ error: 'hour must be 0-23' });
  }

  // Update live reading
  liveUsage[hour] = parseFloat(usage);
  runAnomalyDetection();

  res.json({ success: true, message: 'Reading recorded', hour, usage });
});

// POST /api/simulate/spike — simulate a usage spike
app.post('/api/simulate/spike', (req, res) => {
  const h = new Date().getHours();
  const spikePct = req.body.percent || 60;

  [h, (h+1)%24, (h+2)%24].forEach(hour => {
    liveUsage[hour] = +(BASELINE[hour] * (1 + spikePct/100)).toFixed(1);
  });

  runAnomalyDetection();
  res.json({ success: true, message: `Spike of ${spikePct}% simulated at hour ${h}` });
});

// POST /api/simulate/normalize — normalize usage
app.post('/api/simulate/normalize', (req, res) => {
  liveUsage = [...BASELINE].map(b => +(b + (Math.random()-0.5)*b*0.1).toFixed(1));
  res.json({ success: true, message: 'Usage normalized to baseline' });
});

// GET /api/settings — get settings
app.get('/api/settings', (req, res) => {
  res.json(getSettings());
});

// PUT /api/settings — update settings
app.put('/api/settings', (req, res) => {
  const { anomaly_threshold, high_alert_threshold, morning_baseline } = req.body;
  const updateStmt = db.prepare('INSERT OR REPLACE INTO settings VALUES (?,?)');

  if (anomaly_threshold !== undefined)
    updateStmt.run('anomaly_threshold', String(anomaly_threshold));
  if (high_alert_threshold !== undefined)
    updateStmt.run('high_alert_threshold', String(high_alert_threshold));
  if (morning_baseline !== undefined) {
    updateStmt.run('morning_baseline', String(morning_baseline));
    BASELINE[6] = parseFloat(morning_baseline);
  }

  res.json({ success: true, settings: getSettings() });
});

// ─────────────────────────────────────────
//  WEBSOCKET — Real-time connection
// ─────────────────────────────────────────
wss.on('connection', (ws) => {
  console.log('[WS] Client connected');

  // Send initial state immediately
  ws.send(JSON.stringify({
    type: 'INIT',
    data: {
      liveUsage,
      baseline24h: BASELINE,
      settings: getSettings(),
      anomalies: db.prepare('SELECT * FROM anomalies ORDER BY id DESC LIMIT 20').all()
    }
  }));

  ws.on('close', () => console.log('[WS] Client disconnected'));
});

// ─────────────────────────────────────────
//  START SERVER
// ─────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`
  ╔═══════════════════════════════════════╗
  ║   💧 JalRakshak Backend Running       ║
  ║   http://localhost:${PORT}               ║
  ║   WebSocket: ws://localhost:${PORT}      ║
  ╚═══════════════════════════════════════╝
  `);
});
