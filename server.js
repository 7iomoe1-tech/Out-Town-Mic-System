'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const NEAR_RADIUS = 70;          // أبعد مسافة نوصّل فيها لاعبين ببعض (ستود)
const SESSION_TTL = 30000;       // نحذف اللاعب لو اللعبة ما أرسلت عنه شي
const CODE_LEN = 8;
const GRACE = 8000;              // مهلة قبل ما نعتبر اللاعب انقطع (تمنع قفل المايك لو الاتصال تذبذب)
const MAX_FAILS = 10;            // محاولات كود غلط مسموحة لكل IP
const FAIL_WINDOW = 10 * 60 * 1000;

// التراخيص: LICENSES = key:gameId,key2:gameId2  (بدون gameId = يشتغل في أي لعبة)
const LICENSES = new Map(
  (process.env.LICENSES || process.env.SECRET || 'change-me').split(',')
    .map((x) => x.trim()).filter(Boolean)
    .map((x) => { const [k, g] = x.split(':'); return [k, g || null]; })
);

const sessions = new Map();   // "license|userId" -> session
const codes = new Map();      // code -> session key
const fails = new Map();      // ip -> { n, t }
const ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const sKey = (ns, id) => `${ns}|${id}`;

const genCode = () => {
  let c;
  do { c = Array.from({ length: CODE_LEN }, () => ALPHABET[Math.random() * ALPHABET.length | 0]).join(''); }
  while (codes.has(c));
  return c;
};
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const clean = (v, n) => String(v == null ? '' : v).slice(0, n);

function getSession(ns, id, name) {
  const k = sKey(ns, id);
  let s = sessions.get(k);
  if (!s) {
    s = { key: k, ns, id, name: id, code: null, jobId: '', pos: [0, 0, 0], mic: false, ptt: false, ch: 0, chName: '',
          ws: null, ready: false, wasReady: false, dropAt: 0, level: 0, seen: Date.now() };
    sessions.set(k, s);
  }
  if (name) s.name = clean(name, 40);
  return s;
}

function ipOf(req) {
  const f = req.headers['x-forwarded-for'];
  return (f ? f.split(',')[0].trim() : req.socket.remoteAddress) || '?';
}
function blocked(ip) {
  const f = fails.get(ip);
  return !!f && Date.now() - f.t < FAIL_WINDOW && f.n >= MAX_FAILS;
}
function addFail(ip) {
  const f = fails.get(ip);
  if (!f || Date.now() - f.t > FAIL_WINDOW) fails.set(ip, { n: 1, t: Date.now() });
  else f.n++;
}

process.on('unhandledRejection', (e) => console.warn('unhandled:', e && e.message));

// سيرفرات ICE: STUN مجاني، و TURN اختياري (يحل مشكلة الشبكات اللي ما يمشي فيها الاتصال المباشر)
let iceCache = { t: 0, ttl: 0, v: null };
async function getIce() {
  if (iceCache.v && Date.now() - iceCache.t < iceCache.ttl) return iceCache.v;
  let v = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  let ttl = 3600e3;
  try {
    if (process.env.ICE_SERVERS) {
      v = JSON.parse(process.env.ICE_SERVERS);
    } else if (process.env.TURN_KEY_ID && process.env.TURN_KEY_API_TOKEN) {
      const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${process.env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${process.env.TURN_KEY_API_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ ttl: 86400 }),
      });
      if (r.ok) {
        const j = await r.json();
        v = j.iceServers
          .map((s) => ({ ...s, urls: [].concat(s.urls).filter((u) => !/:53(\?|$)/.test(u)) }))  // المنفذ 53 ما يشتغل في المتصفحات
          .filter((s) => s.urls.length);
      } else { console.warn('TURN request failed:', r.status); ttl = 60e3; }
    }
  } catch (e) { console.warn('ICE error:', e.message); ttl = 60e3; }
  iceCache = { t: Date.now(), ttl, v };
  return v;
}

// نبلغ اللاعبين القريبين إن اتصال لاعب انقطع أو تجدد، عشان يسكرون الاتصال القديم
function notifyGone(s) {
  for (const o of sessions.values()) {
    if (o !== s && o.ns === s.ns && o.jobId === s.jobId && o.ws) send(o.ws, { type: 'gone', id: s.id });
  }
}

const app = express();
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.set({
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY',
    'Permissions-Policy': 'microphone=(self), camera=(), geolocation=()',
  });
  next();
});
app.use(express.json({ limit: '200kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.type('text').send('ok'));

function auth(req, res, next) {
  const key = req.get('X-Secret');
  const game = LICENSES.get(key);
  if (game === undefined) return res.status(401).json({ error: 'bad key' });
  if (game && clean((req.body || {}).gameId, 32) !== game) return res.status(403).json({ error: 'wrong game' });
  req.license = key;
  next();
}

// اللعبة تطلب رابط ربط للاعب
app.post('/api/register', auth, (req, res) => {
  const { id, name } = req.body || {};
  if (!id) return res.status(400).json({ error: 'missing id' });
  const s = getSession(req.license, clean(id, 20), name);
  if (!s.code) { s.code = genCode(); codes.set(s.code, s.key); }
  const base = process.env.PUBLIC_URL || `https://${req.get('host')}`;
  res.json({ code: s.code, url: `${base}/?c=${s.code}`, site: base });
});

// اللعبة ترسل المواقع وحالة المايك، وترجع لها حالة الربط ومستوى الصوت
app.post('/api/sync', auth, (req, res) => {
  const jobId = clean(req.body.jobId, 64);
  const players = Array.isArray(req.body.players) ? req.body.players.slice(0, 100) : [];
  const now = Date.now();
  const active = [];

  for (const p of players) {
    if (!p || p.id == null) continue;
    const s = getSession(req.license, clean(p.id, 20), p.name);
    s.jobId = jobId;
    s.pos = [+p.x || 0, +p.y || 0, +p.z || 0];
    s.seen = now;
    const mic = !!p.mic, ptt = !!p.ptt, ch = Number(p.ch) || 0, chName = clean(p.chName, 40);
    if (s.mic !== mic || s.ptt !== ptt || s.ch !== ch || s.chName !== chName) {
      s.mic = mic; s.ptt = ptt; s.ch = ch; s.chName = chName;
      send(s.ws, { type: 'state', micOn: mic, ptt, ch, chName });
    }
    if (!s.mic && !s.ptt) s.level = 0;
    active.push(s);
  }

  const out = {};
  for (const s of active) {
    const grace = !s.ws && s.dropAt && now - s.dropAt < GRACE;
    out[s.id] = {
      linked: !!s.ws || !!grace,
      ready: (!!s.ws && s.ready) || (!!grace && s.wasReady),
      level: (s.mic || s.ptt) ? s.level : 0,
    };
    if (!s.ws) continue;
    const peers = [];
    for (const o of sessions.values()) {
      if (o === s || o.ns !== s.ns || o.jobId !== s.jobId || !o.ws || now - o.seen > 5000) continue;
      const d = Math.hypot(o.pos[0] - s.pos[0], o.pos[1] - s.pos[1], o.pos[2] - s.pos[2]);
      const radio = s.ch > 0 && o.ch === s.ch;
      if (d <= NEAR_RADIUS || radio) peers.push({ id: o.id, name: o.name, dist: d, mic: o.mic, ptt: o.ptt, radio });
    }
    send(s.ws, { type: 'near', peers });
  }
  res.json({ players: out });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });

wss.on('connection', (ws, req) => {
  const ip = ipOf(req);
  let me = null;
  let bucket = 100, last = Date.now();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (raw) => {
    const n = Date.now();
    bucket = Math.min(100, bucket + (n - last) / 10); last = n;
    if (bucket-- < 1) return;

    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      if (blocked(ip)) return send(ws, { type: 'error', fatal: true, msg: 'محاولات كثيرة. انتظر شوي وجرّب مرة ثانية.' });
      const k = codes.get(clean(m.code, 16).toUpperCase());
      const s = k && sessions.get(k);
      if (!s) {
        addFail(ip);
        return send(ws, { type: 'error', fatal: true, msg: 'الكود غلط أو انتهى. اطلب رابط جديد من اللعبة.' });
      }
      if (s.ws && s.ws !== ws) { send(s.ws, { type: 'error', fatal: true, msg: 'انفتح الموقع من مكان ثاني.' }); s.ws.close(); }
      s.ws = ws; s.ready = false; s.dropAt = 0; me = s;
      notifyGone(s);
      return send(ws, { type: 'joined', id: s.id, name: s.name, micOn: s.mic, ptt: s.ptt, ch: s.ch, chName: s.chName, ice: await getIce() });
    }

    if (!me) return;
    if (m.type === 'ready') me.ready = !!m.v;
    else if (m.type === 'level') me.level = Math.max(0, Math.min(1, +m.v || 0));
    else if (m.type === 'signal') {
      const o = sessions.get(sKey(me.ns, clean(m.to, 20)));
      if (o && o.ws && o.jobId === me.jobId) send(o.ws, { type: 'signal', from: me.id, data: m.data });
    }
  });

  ws.on('close', () => {
    if (me && me.ws === ws) { me.wasReady = me.ready; me.dropAt = Date.now(); me.ws = null; me.ready = false; me.level = 0; notifyGone(me); }
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [k, s] of sessions) {
    if (now - s.seen > SESSION_TTL) {
      if (s.ws) s.ws.close();
      codes.delete(s.code);
      sessions.delete(k);
    }
  }
  for (const [ip, f] of fails) if (now - f.t > FAIL_WINDOW) fails.delete(ip);
  wss.clients.forEach((c) => {
    if (!c.isAlive) return c.terminate();
    c.isAlive = false; c.ping();
  });
}, 10000);

server.listen(PORT, () => console.log(`mic server on :${PORT}`));
