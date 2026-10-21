const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const SECRET = process.env.SECRET || 'change-me';   // نفس القيمة اللي في سكربت روبلوكس
const PORT = process.env.PORT || 3000;
const NEAR_RADIUS = 70;                              // أبعد مسافة نوصّل فيها الأشخاص ببعض (ستود)

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const sessions = new Map(); // userId -> session
const codes = new Map();    // code -> userId
const ALPHA = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const genCode = () => { let c; do { c = Array.from({ length: 5 }, () => ALPHA[Math.random() * ALPHA.length | 0]).join(''); } while (codes.has(c)); return c; };
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const getSession = (id, name) => {
  let s = sessions.get(id);
  if (!s) { s = { id, name: name || id, code: null, jobId: '', pos: [0, 0, 0], mic: false, ws: null, ready: false, level: 0, seen: Date.now() }; sessions.set(id, s); }
  if (name) s.name = name;
  return s;
};
const auth = (req, res, next) => req.get('X-Secret') === SECRET ? next() : res.status(401).json({ error: 'bad secret' });

// اللعبة تطلب كود/رابط ربط للاعب
app.post('/api/register', auth, (req, res) => {
  const { id, name } = req.body || {};
  if (!id) return res.status(400).end();
  const s = getSession(String(id), name);
  if (!s.code) { s.code = genCode(); codes.set(s.code, s.id); }
  const base = process.env.PUBLIC_URL || `https://${req.get('host')}`;
  res.json({ code: s.code, url: `${base}/?c=${s.code}`, site: base });
});

// اللعبة ترسل المواقع وحالة المايك كل ~0.4 ثانية، وترجع لها حالة الربط ومستوى الصوت
app.post('/api/sync', auth, (req, res) => {
  const { jobId = '', players = [] } = req.body || {};
  const now = Date.now();
  for (const p of players) {
    const s = getSession(String(p.id), p.name);
    s.jobId = jobId; s.pos = [p.x, p.y, p.z]; s.seen = now;
    if (s.mic !== !!p.mic) { s.mic = !!p.mic; send(s.ws, { type: 'state', micOn: s.mic }); }
    if (!s.mic) s.level = 0;
  }
  const out = {};
  for (const p of players) {
    const s = sessions.get(String(p.id));
    out[s.id] = { linked: !!s.ws, ready: !!s.ws && s.ready, level: s.mic ? s.level : 0 };
    if (s.ws) {
      const peers = [];
      for (const o of sessions.values()) {
        if (o === s || !o.ws || o.jobId !== s.jobId || now - o.seen > 5000) continue;
        const d = Math.hypot(o.pos[0] - s.pos[0], o.pos[1] - s.pos[1], o.pos[2] - s.pos[2]);
        if (d <= NEAR_RADIUS) peers.push({ id: o.id, name: o.name, dist: d });
      }
      send(s.ws, { type: 'near', peers });
    }
  }
  res.json({ players: out });
});

wss.on('connection', (ws) => {
  let me = null;
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'join') {
      const id = codes.get(String(m.code || '').toUpperCase());
      const s = id && sessions.get(id);
      if (!s) return send(ws, { type: 'error', fatal: true, msg: 'الكود غلط أو انتهى. اطلب رابط جديد من اللعبة.' });
      if (s.ws && s.ws !== ws) { send(s.ws, { type: 'error', fatal: true, msg: 'انفتح الموقع من مكان ثاني.' }); s.ws.close(); }
      s.ws = ws; me = s; s.ready = false;
      send(ws, { type: 'joined', id: s.id, name: s.name, micOn: s.mic });
    } else if (!me) { return; }
    else if (m.type === 'ready') me.ready = !!m.v;
    else if (m.type === 'level') me.level = Math.max(0, Math.min(1, +m.v || 0));
    else if (m.type === 'signal') {
      const o = sessions.get(String(m.to));
      if (o && o.ws && o.jobId === me.jobId) send(o.ws, { type: 'signal', from: me.id, data: m.data });
    }
  });
  ws.on('close', () => { if (me && me.ws === ws) { me.ws = null; me.ready = false; me.level = 0; } });
});

setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) if (now - s.seen > 30000) { if (s.ws) s.ws.close(); codes.delete(s.code); sessions.delete(id); }
  wss.clients.forEach((c) => c.ping());
}, 10000);

server.listen(PORT, () => console.log('listening on', PORT));
