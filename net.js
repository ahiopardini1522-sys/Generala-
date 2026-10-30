/* Capa de red de la Generala.
   Las salas online viajan por un servidor público y gratuito de mensajes (MQTT sobre WebSocket),
   así que nadie necesita una cuenta. Este archivo imita la parte de base de datos que usa el juego
   (doc / collection, get / set / update / delete / onSnapshot): cada documento es un mensaje retenido
   en el servidor, y quien entra a una sala recibe enseguida el último estado. */
(function () {
  'use strict';

  const NS = window.__GNRL_NS || 'gnrl-4k7q2';
  const BROKERS = window.__GNRL_BROKERS || [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://mqtt.eclipseprojects.io:443/mqtt'
  ];
  const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const CONNECT_TIMEOUT = window.__GNRL_CONNECT_TIMEOUT || 7000;

  const te = new TextEncoder();
  const td = new TextDecoder();
  const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
  const clone = (x) => JSON.parse(JSON.stringify(x));
  const lastSeg = (p) => p.slice(p.lastIndexOf('/') + 1);
  const parentOf = (p) => p.slice(0, p.lastIndexOf('/'));
  // Igual que update en la base de datos: los objetos se fusionan, lo demás (arrays incluidos) se reemplaza.
  function merge(a, b) {
    const out = Object.assign({}, a);
    for (const k of Object.keys(b)) {
      const bv = b[k];
      out[k] = isObj(bv) ? merge(isObj(out[k]) ? out[k] : {}, bv) : bv;
    }
    return out;
  }
  function pick(alpha) {
    const a = new Uint32Array(1);
    crypto.getRandomValues(a);
    return alpha[a[0] % alpha.length];
  }
  const randStr = (n, alpha) => Array.from({ length: n }, () => pick(alpha)).join('');

  /* ---------- paquetes MQTT 3.1.1 ---------- */
  function cat(parts) {
    let n = 0;
    for (const p of parts) n += p.length;
    const out = new Uint8Array(n);
    let o = 0;
    for (const p of parts) { out.set(p, o); o += p.length; }
    return out;
  }
  const u16 = (n) => Uint8Array.of((n >> 8) & 255, n & 255);
  function mstr(s) { const b = te.encode(s); return cat([u16(b.length), b]); }
  function varint(n) {
    const o = [];
    do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 128; o.push(b); } while (n > 0);
    return Uint8Array.from(o);
  }
  const pkt = (first, body) => cat([Uint8Array.of(first), varint(body.length), body]);
  const encConnect = (cid, keep) => pkt(0x10, cat([Uint8Array.of(0, 4, 77, 81, 84, 84, 4, 2), u16(keep), mstr(cid)]));
  function encPublish(topic, payload, qos, retain, id) {
    const parts = [mstr(topic)];
    if (qos > 0) parts.push(u16(id));
    parts.push(payload);
    return pkt(0x30 | (qos << 1) | (retain ? 1 : 0), cat(parts));
  }
  function encSubscribe(id, filters, qos) {
    const parts = [u16(id)];
    for (const f of filters) { parts.push(mstr(f)); parts.push(Uint8Array.of(qos)); }
    return pkt(0x82, cat(parts));
  }
  function encUnsubscribe(id, filters) {
    const parts = [u16(id)];
    for (const f of filters) parts.push(mstr(f));
    return pkt(0xA2, cat(parts));
  }
  const encPuback = (id) => Uint8Array.of(0x40, 2, (id >> 8) & 255, id & 255);
  const PINGREQ = Uint8Array.of(0xC0, 0);

  /* ---------- conexión ---------- */
  class Mqtt {
    constructor(url) {
      this.url = url;
      this.cid = 'g' + randStr(12, 'abcdefghijklmnopqrstuvwxyz0123456789');
      this.ws = null;
      this.up = false;
      this.everUp = false;
      this.opening = null;
      this.keep = true;
      this.buf = new Uint8Array(0);
      this.nextId = 0;
      this.acks = new Map();
      this.filters = new Set();
      this.subscribed = new Set();
      this.pendingSub = new Map();
      this.lastRx = 0;
      this.pinger = 0;
      this.retry = 0;
      this.retryT = 0;
      this.onmsg = () => {};
      this.onstate = () => {};
    }
    newId() { this.nextId = (this.nextId % 65535) + 1; return this.nextId; }
    send(bytes) {
      if (!this.up || !this.ws) throw { code: 'unavailable', message: 'sin conexión' };
      this.ws.send(bytes);
    }
    open() {
      if (this.up) return Promise.resolve();
      if (this.opening) return this.opening;
      this.keep = true;
      this.opening = new Promise((resolve, reject) => {
        let settled = false;
        let ws = null;
        const settle = (err) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          this.opening = null;
          if (err) reject(err); else resolve();
        };
        const timer = setTimeout(() => {
          try { if (ws) ws.close(); } catch (e) {}
          settle({ code: 'unavailable', message: 'tiempo agotado' });
        }, CONNECT_TIMEOUT);
        try { ws = new WebSocket(this.url, 'mqtt'); } catch (e) { settle({ code: 'unavailable', message: String(e) }); return; }
        ws.binaryType = 'arraybuffer';
        this.ws = ws;
        this.buf = new Uint8Array(0);
        ws.onopen = () => { try { ws.send(encConnect(this.cid, 30)); } catch (e) {} };
        ws.onmessage = (ev) => {
          if (this.ws !== ws) return;
          this.lastRx = Date.now();
          this.feed(new Uint8Array(ev.data), (rc) => {
            if (rc === 0) {
              this.up = true;
              this.everUp = true;
              this.retry = 0;
              this.subscribed.clear();
              this.startPing(ws);
              settle();
              this.afterUp();
            } else settle({ code: 'unavailable', message: 'rechazado ' + rc });
          });
        };
        ws.onerror = () => {};
        ws.onclose = () => {
          const wasUp = this.up && this.ws === ws;
          if (this.ws === ws) { this.ws = null; this.up = false; clearInterval(this.pinger); }
          if (wasUp) this.failAcks();
          settle({ code: 'unavailable', message: 'cerrado' });
          if (wasUp) this.onstate(false);
          this.scheduleReconnect();
        };
      });
      return this.opening;
    }
    afterUp() {
      const fs = Array.from(this.filters).filter((f) => !this.subscribed.has(f) && !this.pendingSub.has(f));
      if (fs.length) this.subRaw(fs).catch(() => {});
      this.onstate(true);
    }
    scheduleReconnect() {
      if (!this.keep || this.retryT || this.up) return;
      const wait = Math.min(15000, 800 * Math.pow(2, this.retry++)) + Math.random() * 400;
      this.retryT = setTimeout(() => {
        this.retryT = 0;
        if (!this.keep || this.up) return;
        this.open().catch(() => this.scheduleReconnect());
      }, wait);
    }
    startPing(ws) {
      clearInterval(this.pinger);
      this.lastRx = Date.now();
      this.pinger = setInterval(() => {
        if (this.ws !== ws) return;
        if (Date.now() - this.lastRx > 45000) { try { ws.close(); } catch (e) {} return; }
        try { ws.send(PINGREQ); } catch (e) {}
      }, 15000);
    }
    // Al volver a la app o recuperar internet: comprueba que la conexión siga viva o la rehace.
    probe() {
      if (!this.keep || !this.everUp) return;
      if (!this.up) {
        clearTimeout(this.retryT); this.retryT = 0;
        if (!this.opening) this.open().catch(() => this.scheduleReconnect());
        return;
      }
      const ws = this.ws, mark = this.lastRx;
      try { ws.send(PINGREQ); } catch (e) {}
      setTimeout(() => { if (this.ws === ws && this.lastRx === mark) { try { ws.close(); } catch (e) {} } }, 4000);
    }
    close() {
      this.keep = false;
      clearTimeout(this.retryT); this.retryT = 0;
      clearInterval(this.pinger);
      try { if (this.ws) this.ws.close(); } catch (e) {}
    }
    feed(chunk, onConnack) {
      this.buf = this.buf.length ? cat([this.buf, chunk]) : chunk;
      let o = 0;
      for (;;) {
        const b = this.buf;
        if (b.length - o < 2) break;
        let mul = 1, len = 0, i = o + 1, byte, ok = true;
        do {
          if (i >= b.length) { ok = false; break; }
          byte = b[i++];
          len += (byte & 127) * mul;
          mul *= 128;
        } while (byte & 128);
        if (!ok || b.length - i < len) break;
        this.handle(b[o], b.subarray(i, i + len), onConnack);
        o = i + len;
      }
      this.buf = o ? this.buf.slice(o) : this.buf;
    }
    handle(first, body, onConnack) {
      const type = first >> 4;
      if (type === 2) { onConnack(body[1]); return; }
      if (type === 3) {
        const qos = (first >> 1) & 3;
        const tl = (body[0] << 8) | body[1];
        const topic = td.decode(body.subarray(2, 2 + tl));
        let p = 2 + tl, id = 0;
        if (qos > 0) { id = (body[p] << 8) | body[p + 1]; p += 2; }
        const payload = body.slice(p);
        if (qos === 1) { try { this.ws.send(encPuback(id)); } catch (e) {} }
        try { this.onmsg(topic, payload); } catch (e) { console.error(e); }
        return;
      }
      if (type === 4 || type === 9) {
        const id = (body[0] << 8) | body[1];
        const a = this.acks.get(id);
        if (a) {
          this.acks.delete(id);
          clearTimeout(a.timer);
          if (type === 9 && body[2] === 0x80) a.rej({ code: 'invalid_argument', message: 'suscripción rechazada' });
          else a.res();
        }
      }
    }
    waitAck(id, ms) {
      return new Promise((res, rej) => {
        const timer = setTimeout(() => { this.acks.delete(id); rej({ code: 'unavailable', message: 'sin confirmación' }); }, ms || 8000);
        this.acks.set(id, { res, rej, timer });
      });
    }
    failAcks() {
      for (const a of this.acks.values()) { clearTimeout(a.timer); a.rej({ code: 'unavailable', message: 'conexión perdida' }); }
      this.acks.clear();
    }
    subRaw(filters) {
      const id = this.newId();
      const w = this.waitAck(id);
      const p = (async () => {
        this.send(encSubscribe(id, filters, 1));
        await w;
        filters.forEach((f) => this.subscribed.add(f));
      })();
      filters.forEach((f) => this.pendingSub.set(f, p));
      const done = () => filters.forEach((f) => { if (this.pendingSub.get(f) === p) this.pendingSub.delete(f); });
      p.then(done, done);
      return p;
    }
    async subscribe(f) {
      this.filters.add(f);
      await this.open();
      if (this.subscribed.has(f)) return;
      const pend = this.pendingSub.get(f);
      if (pend) return pend;
      await this.subRaw([f]);
    }
    unsubscribe(f) {
      this.filters.delete(f);
      this.subscribed.delete(f);
      if (this.up) { try { this.send(encUnsubscribe(this.newId(), [f])); } catch (e) {} }
    }
    async publish(topic, payload, opts) {
      await this.open();
      const retain = !!(opts && opts.retain);
      if (opts && opts.qos === 0) { this.send(encPublish(topic, payload, 0, retain, 0)); return; }
      const id = this.newId();
      const w = this.waitAck(id);
      this.send(encPublish(topic, payload, 1, retain, id));
      await w;
    }
  }

  /* ---------- documentos sobre mensajes retenidos ---------- */
  let lastNotice = 0;
  function notice(msg) {
    const now = Date.now();
    if (now - lastNotice < 6000) return;
    lastNotice = now;
    if (window.__toast) window.__toast(msg, 3200);
  }

  class Store {
    constructor(idx) {
      this.idx = idx;
      this.mq = new Mqtt(BROKERS[idx]);
      this.cache = new Map();
      this.wantDoc = new Map();
      this.wantCol = new Map();
      this.docL = new Map();
      this.colL = new Map();
      this.ring = new Map();
      this.barriers = new Map();
      this.bn = 0;
      this.queue = Promise.resolve();
      this.bfilter = NS + '/_b/' + this.mq.cid + '/+';
      this.dropped = false;
      this._ready = null;
      this.mq.onmsg = (t, p) => this.onMsg(t, p);
      this.mq.onstate = (up) => this.onState(up);
    }
    top(path) { return NS + '/' + path; }
    ready() {
      if (!this._ready) {
        this._ready = this.mq.open().then(() => this.mq.subscribe(this.bfilter)).catch((e) => {
          this._ready = null;
          // un servidor al que nunca pudimos conectar no se reintenta en segundo plano
          if (!this.mq.everUp) { this.mq.close(); if (stores.get(this.idx) === this) stores.delete(this.idx); }
          throw e;
        });
      }
      return this._ready;
    }
    wanted(path) { return this.wantDoc.has(path) || this.wantCol.has(parentOf(path)); }
    wantAny() { return this.wantDoc.size > 0 || this.wantCol.size > 0; }
    onState(up) {
      if (!up) { this.dropped = true; if (this.wantAny()) notice('Sin conexión. Reconectando…'); }
      else if (this.dropped) { this.dropped = false; if (this.wantAny()) notice('Conexión recuperada.'); }
    }
    onMsg(topic, payload) {
      const bw = this.barriers.get(topic);
      if (bw) { this.barriers.delete(topic); bw(); return; }
      if (topic.indexOf(NS + '/') !== 0) return;
      const path = topic.slice(NS.length + 1);
      if (!this.wanted(path)) return;
      const text = td.decode(payload);
      const cur = this.cache.get(path);
      const ring = this.ring.get(path);
      if (ring && ring.length) {
        const k = ring.indexOf(text);
        if (k >= 0) {
          ring.splice(0, k + 1);
          if (ring.length > 0) return;                 // eco viejo de algo que ya reemplazamos
          if (cur && cur.text === text) return;        // eco de lo último que escribimos
        }
      }
      if (cur && cur.text === text) return;            // mismo contenido (por ejemplo al reconectar)
      let ent;
      if (text === '') ent = { exists: false, data: undefined, text };
      else { try { ent = { exists: true, data: JSON.parse(text), text }; } catch (e) { return; } }
      if (!cur && !ent.exists) { this.cache.set(path, ent); return; }
      this.cache.set(path, ent);
      this.notify(path, false);
    }
    notify(path, pending) {
      const dl = this.docL.get(path);
      if (dl) for (const l of Array.from(dl)) l(this.snap(path, pending));
      const col = parentOf(path);
      const cl = this.colL.get(col);
      if (cl) for (const l of Array.from(cl)) l(this.qsnap(col));
    }
    snap(path, pending) {
      const e = this.cache.get(path);
      const exists = !!(e && e.exists);
      const data = exists ? e.data : undefined;
      return { id: lastSeg(path), exists, data: () => (exists ? clone(data) : undefined), metadata: { fromCache: false, hasPendingWrites: !!pending } };
    }
    qsnap(col) {
      const docs = [];
      for (const [p, e] of this.cache) if (e.exists && parentOf(p) === col) docs.push(this.snap(p, false));
      docs.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
      return { docs, size: docs.length, empty: docs.length === 0, metadata: { fromCache: false, hasPendingWrites: false } };
    }
    async hold(kind, path) {
      await this.ready();
      const m = kind === 'doc' ? this.wantDoc : this.wantCol;
      const n = m.get(path) || 0;
      m.set(path, n + 1);
      const filter = kind === 'doc' ? this.top(path) : this.top(path) + '/+';
      let released = false;
      const h = {
        fresh: n === 0,
        release: () => {
          if (released) return;
          released = true;
          const k = m.get(path) || 0;
          if (k > 1) { m.set(path, k - 1); return; }
          m.delete(path);
          this.mq.unsubscribe(filter);
          if (kind === 'doc') this.cache.delete(path);
          else for (const p of Array.from(this.cache.keys())) if (parentOf(p) === path && !this.wantDoc.has(p)) this.cache.delete(p);
        }
      };
      try { await this.mq.subscribe(filter); } catch (e) { h.release(); throw e; }
      return h;
    }
    // Espera a que el servidor termine de mandar los mensajes retenidos de lo que acabamos de suscribir.
    async settle() {
      await this.mq.open();
      const t = NS + '/_b/' + this.mq.cid + '/' + (++this.bn);
      const wait = new Promise((res) => {
        const to = setTimeout(() => { this.barriers.delete(t); res(); }, 2500);
        this.barriers.set(t, () => { clearTimeout(to); setTimeout(res, 120); });
      });
      await this.mq.publish(t, Uint8Array.of(1), { qos: 0, retain: false });
      await wait;
    }
    async readOnce(path) {
      const h = await this.hold('doc', path);
      try {
        if (h.fresh) await this.settle();
        return this.cache.get(path) || { exists: false };
      } finally { h.release(); }
    }
    write(path, data, isUpdate) {
      const job = this.queue.then(() => this._write(path, data, isUpdate));
      this.queue = job.catch(() => {});
      return job;
    }
    async _write(path, data, isUpdate) {
      await this.ready();
      let next = data;
      if (isUpdate) {
        let e = this.wanted(path) ? this.cache.get(path) : null;
        if (!e) e = await this.readOnce(path);
        if (!e || !e.exists) throw { code: 'invalid_argument', message: 'el documento no existe' };
        next = merge(e.data, data);
      }
      await this.publishDoc(path, { exists: true, data: next, text: JSON.stringify(next) });
    }
    remove(path) {
      const job = this.queue.then(async () => {
        await this.ready();
        await this.publishDoc(path, { exists: false, data: undefined, text: '' });
      });
      this.queue = job.catch(() => {});
      return job;
    }
    async publishDoc(path, ent) {
      await this.mq.open();
      const prev = this.cache.get(path);
      let ring = this.ring.get(path);
      if (!ring) this.ring.set(path, (ring = []));
      ring.push(ent.text);
      if (ring.length > 16) ring.shift();
      if (this.wanted(path)) { this.cache.set(path, ent); this.notify(path, true); }
      try {
        await this.mq.publish(this.top(path), te.encode(ent.text), { retain: true, qos: 1 });
      } catch (e) {
        const i = ring.lastIndexOf(ent.text);
        if (i >= 0) ring.splice(i, 1);
        if (this.wanted(path)) {
          if (prev) this.cache.set(path, prev); else this.cache.delete(path);
          this.notify(path, false);
        }
        throw e;
      }
    }
  }

  /* ---------- elección de servidor ---------- */
  const stores = new Map();
  let preferred = null;
  let probing = null;
  const getStore = (i) => { let s = stores.get(i); if (!s) { s = new Store(i); stores.set(i, s); } return s; };
  function dispose(i) { const s = stores.get(i); if (s) { s.mq.close(); stores.delete(i); } }
  // El primer carácter del código dice en qué servidor está la sala, así todos se conectan al mismo.
  function brokerOfChar(ch) {
    const i = ALPHA.indexOf(String(ch).toUpperCase());
    if (i < 0) return 0;
    return Math.min(BROKERS.length - 1, Math.floor((i * BROKERS.length) / ALPHA.length));
  }
  function idxForPath(path) {
    const m = /^games\/([A-Za-z0-9]{4})(?:\/|$)/.exec(path);
    if (m) return brokerOfChar(m[1][0]);
    return preferred === null ? 0 : preferred;
  }
  function pickBroker() {
    if (preferred !== null) return Promise.resolve(preferred);
    if (probing) return probing;
    probing = new Promise((resolve) => {
      let left = BROKERS.length, won = false;
      BROKERS.forEach((_, i) => {
        getStore(i).ready().then(() => {
          if (won) return;
          won = true;
          preferred = i;
          BROKERS.forEach((__, j) => { if (j !== i) dispose(j); });
          resolve(i);
        }, () => {
          if (--left === 0 && !won) { BROKERS.forEach((__, j) => dispose(j)); resolve(null); }
        });
      });
    }).then((i) => { probing = null; return i; });
    return probing;
  }
  window.__genCode = function () {
    const b = preferred === null ? 0 : preferred;
    const firsts = Array.from(ALPHA).filter((c) => brokerOfChar(c) === b);
    return pick(firsts.length ? firsts.join('') : ALPHA) + randStr(3, ALPHA);
  };

  /* ---------- referencias a documentos y colecciones ---------- */
  function watch(kind, path, getS, next, error) {
    let alive = true, started = false, h = null, s = null;
    const emit = (snap) => { if (alive && started) { try { next(snap); } catch (e) { console.error(e); } } };
    const listeners = () => (kind === 'doc' ? s.docL : s.colL);
    (async () => {
      try {
        s = getS();
        const L = listeners();
        let set = L.get(path);
        if (!set) L.set(path, (set = new Set()));
        set.add(emit);
        h = await s.hold(kind, path);
        if (h.fresh) await s.settle();
        if (!alive) { h.release(); return; }
        started = true;
        emit(kind === 'doc' ? s.snap(path, false) : s.qsnap(path));
      } catch (e) {
        if (alive && error) { try { error(e && e.code ? e : { code: 'unavailable', message: String(e) }); } catch (x) {} }
      }
    })();
    return () => {
      alive = false;
      if (s) { const set = listeners().get(path); if (set) { set.delete(emit); if (!set.size) listeners().delete(path); } }
      if (h) h.release();
    };
  }
  function docRef(path) {
    const store = () => getStore(idxForPath(path));
    return {
      id: lastSeg(path),
      path,
      get: async () => {
        const s = store();
        const h = await s.hold('doc', path);
        try { if (h.fresh) await s.settle(); return s.snap(path, false); } finally { h.release(); }
      },
      set: (data) => store().write(path, clone(data), false),
      update: (data) => store().write(path, clone(data), true),
      delete: () => store().remove(path),
      onSnapshot: (next, error) => watch('doc', path, store, next, error),
      collection: (sub) => colRef(path + '/' + sub)
    };
  }
  function colRef(path) {
    const store = () => getStore(idxForPath(path));
    return {
      path,
      doc: (id) => docRef(path + '/' + id),
      get: async () => {
        const s = store();
        const h = await s.hold('col', path);
        try { if (h.fresh) await s.settle(); return s.qsnap(path); } finally { h.release(); }
      },
      onSnapshot: (next, error) => watch('col', path, store, next, error)
    };
  }
  const db = { doc: docRef, collection: colRef };

  /* ---------- identidad: un id por celular ---------- */
  let memUid = null;
  function uid() {
    if (memUid) return memUid;
    try { memUid = localStorage.getItem('generala:uid'); } catch (e) {}
    if (!memUid) {
      memUid = 'u_' + randStr(14, 'abcdefghijklmnopqrstuvwxyz0123456789');
      try { localStorage.setItem('generala:uid', memUid); } catch (e) {}
    }
    return memUid;
  }
  const user = {
    id: async () => uid(),
    can: async () => true,
    isOwner: async () => false,
    canEdit: async () => false,
    me: async () => ({ id: uid(), name: '', isOwner: false, canEdit: false })
  };

  // Misma puerta que usa el juego dentro de Claude: window.claude.use('db' | 'user').
  window.claude = {
    use: async (name) => {
      if (name === 'user') return user;
      if (name === 'db') { const i = await pickBroker(); return i === null ? null : db; }
      return null;
    }
  };

  document.addEventListener('visibilitychange', () => { if (!document.hidden) stores.forEach((s) => s.mq.probe()); });
  window.addEventListener('online', () => stores.forEach((s) => s.mq.probe()));

  /* ---------- instalación y modo sin conexión ---------- */
  let deferred = null;
  const isStandalone = () => {
    try { return matchMedia('(display-mode: standalone)').matches || navigator.standalone === true; } catch (e) { return false; }
  };
  window.__renderInstall = function () {
    const box = document.getElementById('installBox');
    if (!box) return;
    if (isStandalone()) { box.innerHTML = ''; return; }
    box.innerHTML = deferred
      ? '<button class="btn small" data-act="install">Instalar la app en este celular</button>'
      : '<button class="link" data-act="install-help">Cómo instalarla en el celular</button>';
  };
  window.__install = async function () {
    if (!deferred) return false;
    const ev = deferred;
    deferred = null;
    try { ev.prompt(); await ev.userChoice; } catch (e) {}
    window.__renderInstall();
    return true;
  };
  window.addEventListener('beforeinstallprompt', (e) => { e.preventDefault(); deferred = e; window.__renderInstall(); });
  window.addEventListener('appinstalled', () => { deferred = null; window.__renderInstall(); });
  if ('serviceWorker' in navigator && location.protocol !== 'file:' && !window.__GNRL_NO_SW) {
    // Cuando se activa una versión nueva de la app, el juego decide si recarga o avisa.
    let hadController = !!navigator.serviceWorker.controller;
    navigator.serviceWorker.addEventListener('controllerchange', () => {
      if (!hadController) { hadController = true; return; }
      if (window.__onNewVersion) window.__onNewVersion();
    });
    window.addEventListener('load', () => { navigator.serviceWorker.register('sw.js').catch(() => {}); });
  }

  // ganchos para pruebas
  window.__gnrl = {
    NS, brokerOfChar, stores, merge,
    codec: { pkt, mstr, varint, encConnect, encPublish, encSubscribe, encUnsubscribe, encPuback, Mqtt },
    preferred: () => preferred
  };
})();
