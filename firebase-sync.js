// Live sync between the ME hub and Firebase — v2, built for hundreds of people at once.
//
// What changed from v1 (one document per data type, whole list rewritten on every save):
//  • Every record (policy, account, notification, message…) is stored on its own, inside a
//    small set of "shard" documents per data type. Two people saving different records never
//    touch each other's data.
//  • Saves only send what actually changed, inside a transaction. If someone else changed the
//    same record a moment earlier, both edits are merged field by field (votes, messages,
//    reactions and signatures from different people all survive).
//  • Large uploaded files are moved out of the records into their own chunked documents and
//    cached in the browser, so no document ever hits Firestore's 1 MB limit.
//  • Remote updates are batched (at most a few re-renders a second, however busy it gets).
//  • Data written by v1 is migrated automatically the first time v2 connects. The old
//    documents are left in place as a backup.
//
// Everything lives in the same collection as before, so the Firestore rule in the publishing
// guide (match /medright-policy-hub/{doc}) still covers it — no rule change needed.

import "./firebase-config.js";

export const VERSION = 4;

const CFG = () => window.MRHUB_FIREBASE || {};
const ROOT = () => window.MRHUB_ROOT || "medright-policy-hub";
const KEYS = () => window.MRHUB_KEYS || ["policies", "accounts", "depts", "cats", "inbox", "notes", "userdata",
  "theme", "ann", "quotes", "chars", "forms", "responses", "svc", "handlers"];
const SDK = "https://www.gstatic.com/firebasejs/10.12.2/";

// Shards per data type. NEVER change a number once live — records are placed by it.
const SHARDS = { typing: 4, notes: 24, userdata: 8, chat: 8, responses: 8, attempts: 8, checkins: 6, accounts: 6,
  inbox: 4, recog: 4, know: 4, policies: 4, cands: 4, subs: 4, certs: 4, compDecls: 4, compReports: 4,
  compRounds: 4, events: 4, forms: 4, asks: 4, cases: 4, svc: 4, exams: 4, ann: 2, benefits: 2,
  presence: 16, theme: 1, video: 1, depts: 1, cats: 1, titles: 1, evKinds: 1, knowCats: 1, benCats: 1, quotes: 1, chars: 1, handlers: 1 };
const shardsOf = k => SHARDS[k] || 2;
const BLOB_MIN = 6000;       // strings longer than this that start with "data:" become blobs
const SHARD_MAX = 850000;    // a shard document that would grow past this spills its biggest records into their own documents
const BLOB_CHUNK = 700000;   // characters per blob document
const EMIT_DELAY = 350;      // ms to batch remote changes before re-rendering

export function enabled() {
  const c = CFG();
  return !!(c && c.apiKey && (c.projectId || c.databaseURL));
}
function mode() {
  const m = window.MRHUB_MODE || "auto";
  if (m === "firestore" || m === "rtdb") return m;
  return CFG().databaseURL ? "rtdb" : "firestore";
}

// ---------- small utilities ----------
const J = v => v === undefined ? undefined : JSON.stringify(v);
const same = (a, b) => a === b || J(a) === J(b);
const isObj = v => !!v && typeof v === "object" && !Array.isArray(v);
function hash(str) {
  let h1 = 0xdeadbeef, h2 = 0x41c6ce57;
  for (let i = 0; i < str.length; i++) { const c = str.charCodeAt(i); h1 = Math.imul(h1 ^ c, 2654435761); h2 = Math.imul(h2 ^ c, 1597334677); }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (h2 >>> 0).toString(36) + (h1 >>> 0).toString(36);
}
const shardFor = (key, id) => parseInt(hash(id).slice(0, 6), 36) % shardsOf(key);
const kindOf = v => Array.isArray(v) ? "arr" : isObj(v) ? "obj" : "val";

// A stable id for each record.
function entries(key, value) {
  const out = new Map();
  if (value === undefined || value === null) return out;
  const k = kindOf(value);
  if (k === "obj") { Object.keys(value).forEach(n => { if (value[n] !== undefined) out.set("k:" + n, value[n]); }); return out; }
  if (k === "val") { out.set("_", value); return out; }
  const seen = {};
  value.forEach(item => {
    let id;
    if (item === null || typeof item !== "object") id = "s:" + J(item);
    else if (item.id !== undefined && item.id !== null && item.id !== "") id = "i:" + item.id;
    else if (key === "accounts" && item.email) id = "e:" + String(item.email).toLowerCase();
    else id = "h:" + hash(J(item));
    if (seen[id]) { seen[id]++; id = id + "#" + seen[id]; } else seen[id] = 1;
    out.set(id, item);
  });
  return out;
}

// Three-way merge: base = what I started from, mine = what I want, theirs = what is on the server now.
function merge3(b, m, t) {
  if (same(m, b)) return t;
  if (same(t, b)) return m;
  if (same(m, t)) return m;
  if (isObj(m) && isObj(t)) {
    const bb = isObj(b) ? b : {}, out = {};
    new Set(Object.keys(m).concat(Object.keys(t), Object.keys(bb))).forEach(k => {
      const v = merge3(bb[k], m[k], t[k]);
      if (v !== undefined) out[k] = v;
    });
    return out;
  }
  if (Array.isArray(m) && Array.isArray(t)) return mergeArr(Array.isArray(b) ? b : [], m, t);
  return m;
}
function mergeArr(b, m, t) {
  const keyOf = x => isObj(x) ? (x.id !== undefined ? "i:" + x.id : x.email !== undefined ? "e:" + x.email : null) : null;
  const all = b.concat(m, t);
  if (all.length && all.every(x => keyOf(x))) {
    const B = new Map(b.map(x => [keyOf(x), x])), M = new Map(m.map(x => [keyOf(x), x])), T = new Map(t.map(x => [keyOf(x), x]));
    const order = t.map(keyOf);
    m.forEach((x, i) => {
      const id = keyOf(x);
      if (order.indexOf(id) > -1) return;
      let at = -1;
      for (let j = i - 1; j >= 0 && at < 0; j--) at = order.indexOf(keyOf(m[j]));
      order.splice(at + 1, 0, id);
    });
    return order.map(id => merge3(B.get(id), M.get(id), T.get(id))).filter(v => v !== undefined);
  }
  if (all.every(x => x === null || typeof x !== "object")) {
    const has = (arr, x) => arr.some(y => same(y, x));
    return t.filter(x => !(has(b, x) && !has(m, x))).concat(m.filter(x => !has(b, x) && !has(t, x)));
  }
  const prefix = (p, a) => p.length <= a.length && p.every((x, i) => same(x, a[i]));
  if (prefix(b, m) && prefix(b, t)) return t.concat(m.slice(b.length));
  return m;
}

// ---------- blobs (large uploaded files) ----------
const blobMem = new Map();
const blobUp = new Set();
let idbP = null;
function idb() {
  if (idbP) return idbP;
  idbP = new Promise(res => {
    try {
      const r = indexedDB.open("mrhub-blobs", 1);
      r.onupgradeneeded = () => r.result.createObjectStore("b");
      r.onsuccess = () => res(r.result);
      r.onerror = () => res(null);
    } catch (e) { res(null); }
  });
  return idbP;
}
async function idbGet(k) { const d = await idb(); if (!d) return null; return new Promise(res => { try { const q = d.transaction("b").objectStore("b").get(k); q.onsuccess = () => res(q.result || null); q.onerror = () => res(null); } catch (e) { res(null); } }); }
async function idbPut(k, v) { const d = await idb(); if (!d) return; try { d.transaction("b", "readwrite").objectStore("b").put(v, k); } catch (e) {} }

function extract(v, found) {
  if (typeof v === "string") {
    if (v.length > BLOB_MIN && v.slice(0, 5) === "data:") {
      const id = hash(v) + v.length.toString(36);
      if (found) found.set(id, v);
      blobMem.set(id, v);
      return "mrblob:" + id;
    }
    return v;
  }
  if (Array.isArray(v)) return v.map(x => extract(x, found));
  if (isObj(v)) { const o = {}; Object.keys(v).forEach(k => { if (v[k] !== undefined) o[k] = extract(v[k], found); }); return o; }
  return v;
}
function refsIn(j, out) { const re = /"mrblob:([a-z0-9]+)"/g; let m; while ((m = re.exec(j))) out.add(m[1]); }
function inject(v) {
  if (typeof v === "string") return v.slice(0, 7) === "mrblob:" ? (blobMem.get(v.slice(7)) || "") : v;
  if (Array.isArray(v)) return v.map(inject);
  if (isObj(v)) { const o = {}; Object.keys(v).forEach(k => { o[k] = inject(v[k]); }); return o; }
  return v;
}

// ---------- shared state ----------
let api = null;
const shards = {};        // docId -> { k, kind, m }
const parsed = {};        // key -> Map(id -> { j, v })
const timers = {};
const queues = {};
let handlers = { onStatus() {}, onData() {} };

function assemble(key) {
  const docs = Object.keys(shards).filter(d => shards[d].k === key).map(d => shards[d]);
  if (!docs.length) return undefined;
  const kind = docs[0].kind || "arr";
  const cache = parsed[key] || (parsed[key] = new Map());
  const rows = [];
  docs.forEach(d => Object.keys(d.m || {}).forEach(id => rows.push([id, d.m[id]])));
  const live = new Set();
  const items = rows.map(([id, r]) => {
    live.add(id);
    let c = cache.get(id);
    if (!c || c.j !== r.j) { const raw = JSON.parse(r.j); c = { j: r.j, v: r.j.indexOf('"mrblob:') > -1 ? inject(raw) : raw }; cache.set(id, c); }
    return { id: id, o: r.o || 0, v: c.v };
  });
  Array.from(cache.keys()).forEach(id => { if (!live.has(id)) cache.delete(id); });
  if (kind === "obj") { const o = {}; items.forEach(x => { o[x.id.slice(2)] = x.v; }); return o; }
  if (kind === "val") return items.length ? items[0].v : undefined;
  items.sort((a, b) => a.o - b.o || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return items.map(x => x.v);
}
async function resolveBlobs(key, getBlob) {
  const need = new Set();
  Object.keys(shards).forEach(d => { if (shards[d].k !== key) return; const m = shards[d].m || {}; Object.keys(m).forEach(id => refsIn(m[id].j, need)); });
  const miss = Array.from(need).filter(id => !blobMem.has(id));
  if (!miss.length) return false;
  await Promise.all(miss.map(async id => {
    const c = await idbGet(id);
    if (c) { blobMem.set(id, c); return; }
    const v = await getBlob(id).catch(() => null);
    if (v) { blobMem.set(id, v); idbPut(id, v); }
  }));
  if (parsed[key]) parsed[key].clear();
  return true;
}
function schedule(key, getBlob) {
  clearTimeout(timers[key]);
  timers[key] = setTimeout(async () => {
    await resolveBlobs(key, getBlob);
    const v = assemble(key);
    if (v !== undefined) handlers.onData(key, v);
  }, EMIT_DELAY);
}

// Order value for new array records: slot them between their neighbours.
function orderFor(key, nextIds) {
  const known = {};
  Object.keys(shards).forEach(d => { if (shards[d].k !== key) return; const m = shards[d].m || {}; Object.keys(m).forEach(id => { known[id] = m[id].o || 0; }); });
  const out = {};
  nextIds.forEach((id, i) => {
    if (known[id] !== undefined) { out[id] = known[id]; return; }
    let lo = null, hi = null;
    for (let j = i - 1; j >= 0 && lo === null; j--) { const x = out[nextIds[j]]; if (x !== undefined) lo = x; }
    for (let j = i + 1; j < nextIds.length && hi === null; j++) { const x = known[nextIds[j]]; if (x !== undefined) hi = x; }
    out[id] = lo === null && hi === null ? Date.now() : lo === null ? hi - 1 : hi === null || hi <= lo ? lo + 1 : (lo + hi) / 2;
  });
  return out;
}

// ---------- start ----------
export async function start(h) {
  handlers = Object.assign({ onStatus() {}, onData() {} }, h || {});
  const onStatus = handlers.onStatus;
  if (!enabled()) { onStatus("off", "No Firebase config yet."); return null; }
  try {
    const appMod = await import(SDK + "firebase-app.js");
    const app = appMod.getApps && appMod.getApps().length ? appMod.getApps()[0] : appMod.initializeApp(CFG());
    api = mode() === "rtdb" ? await startRtdb(app) : await startFirestore(app);
    return api;
  } catch (e) {
    onStatus("error", e && e.message ? e.message : "Could not reach Firebase.");
    return null;
  }
}

async function startFirestore(app) {
  const fs = await import(SDK + "firebase-firestore.js");
  let db;
  try { db = fs.initializeFirestore(app, { localCache: fs.persistentLocalCache({ tabManager: fs.persistentMultipleTabManager() }) }); }
  catch (e) { db = fs.getFirestore(app); }
  const R = ROOT();
  const ref = id => fs.doc(db, R, id);
  const getBlob = async id => {
    const first = await fs.getDoc(ref("_b~" + id + "~0"));
    if (!first.exists()) return null;
    const n = first.data().n || 1, parts = [first.data().d];
    for (let i = 1; i < n; i++) { const s = await fs.getDoc(ref("_b~" + id + "~" + i)); parts.push(s.exists() ? s.data().d : ""); }
    return parts.join("");
  };
  const putBlobs = async found => {
    for (const [id, v] of found) {
      if (blobUp.has(id)) continue;
      const n = Math.ceil(v.length / BLOB_CHUNK);
      const ex = await fs.getDoc(ref("_b~" + id + "~0")).catch(() => null);
      if (!ex || !ex.exists()) for (let i = 0; i < n; i++) await fs.setDoc(ref("_b~" + id + "~" + i), { t: "b", n: n, d: v.slice(i * BLOB_CHUNK, (i + 1) * BLOB_CHUNK) });
      blobUp.add(id); idbPut(id, v);
    }
  };

  let first = true;
  await new Promise(resolve => {
    fs.onSnapshot(fs.query(fs.collection(db, R), fs.where("t", "==", "s")), snap => {
      const touched = new Set();
      snap.docChanges().forEach(ch => {
        if (ch.type === "removed") { const k = (shards[ch.doc.id] || {}).k; delete shards[ch.doc.id]; if (k) touched.add(k); return; }
        const d = ch.doc.data();
        shards[ch.doc.id] = { k: d.k, kind: d.kind, m: d.m || {} };
        touched.add(d.k);
      });
      touched.forEach(k => schedule(k, getBlob));
      if (first) {
        first = false;
        migrate(fs, db, ref).catch(() => {}).then(() => {});
        handlers.onStatus("live", "Cloud Firestore connected \u2014 per-record sync.");
        resolve();
      }
    }, err => { handlers.onStatus("error", friendly(err)); resolve(); });
  });

  const write = async (key, next, prev) => {
    const kind = kindOf(next);
    const N = entries(key, next), P = prev === undefined ? null : entries(key, prev);
    const changed = [], removed = [];
    N.forEach((v, id) => { if (!P || !P.has(id) || !same(P.get(id), v)) changed.push(id); });
    if (P) P.forEach((v, id) => { if (!N.has(id)) removed.push(id); });
    if (!changed.length && !removed.length) return;
    const found = new Map();
    const mine = {}, base = {};
    changed.forEach(id => { mine[id] = extract(N.get(id), found); });
    if (P) changed.concat(removed).forEach(id => { if (P.has(id)) base[id] = extract(P.get(id), null); });
    const extra = new Set(); refsIn(J(mine), extra);
    for (const id of extra) {
      if (found.has(id) || blobUp.has(id)) continue;
      const v = blobMem.get(id) || await idbGet(id);
      if (v) { blobMem.set(id, v); found.set(id, v); }
    }
    if (found.size) await putBlobs(found);
    const ord = kind === "arr" ? orderFor(key, Array.from(N.keys())) : {};
    const where = {};
    Object.keys(shards).forEach(d => { if (shards[d].k === key) Object.keys(shards[d].m || {}).forEach(id => { where[id] = d; }); });
    const byShard = {};
    changed.concat(removed).forEach(id => { const s = where[id] || (key + "~" + shardFor(key, id)); (byShard[s] = byShard[s] || []).push(id); });
    const spills = [];
    const docIds = Object.keys(byShard);
    for (let i = 0; i < docIds.length; i += 20) {
      const group = docIds.slice(i, i + 20);
      await fs.runTransaction(db, async tx => {
        const snaps = [];
        for (const d of group) snaps.push(await tx.get(ref(d)));
        group.forEach((d, gi) => {
          const s = snaps[gi], cur = s.exists() ? (s.data().m || {}) : {};
          const fields = [], fresh = {};
          byShard[d].forEach(id => {
            const theirsJ = cur[id] ? cur[id].j : undefined;
            const theirs = theirsJ === undefined ? undefined : JSON.parse(theirsJ);
            const merged = P && P.has(id) ? merge3(base[id], mine[id], theirs) : (theirs === undefined ? mine[id] : merge3(undefined, mine[id], theirs));
            if (merged === undefined) { if (cur[id]) fields.push(new fs.FieldPath("m", id), fs.deleteField()); return; }
            const j = JSON.stringify(merged);
            if (j.length > 900000) throw new Error("A record in " + key + " is too large to share. Use a link for big files.");
            const val = { o: cur[id] ? cur[id].o || 0 : (ord[id] !== undefined ? ord[id] : Date.now()), j: j };
            if (cur[id] && cur[id].j === j) return;
            fields.push(new fs.FieldPath("m", id), val);
            fresh[id] = val;
          });
          if (d.indexOf("~r~") < 0) {
            const sizeOf = id => (fresh[id] ? fresh[id].j.length : cur[id] ? cur[id].j.length : 0);
            const ids = new Set(Object.keys(cur).concat(Object.keys(fresh)));
            let total = 0; ids.forEach(id => { total += sizeOf(id) + 40; });
            if (total > SHARD_MAX) {
              const big = Object.keys(fresh).sort((a, b) => fresh[b].j.length - fresh[a].j.length);
              const out = new Set();
              for (const id of big) { if (total <= SHARD_MAX) break; out.add(id); total -= fresh[id].j.length; spills.push({ id: id, v: fresh[id] }); }
              if (out.size) {
                const rebuilt = [];
                Object.keys(fresh).forEach(id => { if (!out.has(id)) rebuilt.push(new fs.FieldPath("m", id), fresh[id]); else { delete fresh[id]; if (cur[id]) rebuilt.push(new fs.FieldPath("m", id), fs.deleteField()); } });
                byShard[d].forEach(id => { if (!(id in fresh) && !out.has(id) && cur[id] && !N.has(id)) rebuilt.push(new fs.FieldPath("m", id), fs.deleteField()); });
                fields.length = 0; rebuilt.forEach(x => fields.push(x));
              }
            }
          }
          if (!s.exists()) { if (Object.keys(fresh).length) tx.set(ref(d), { t: "s", k: key, n: d.indexOf("~r~") > -1 ? -1 : parseInt(d.split("~")[1], 10), kind: kind, m: fresh }); }
          else if (fields.length) { if (s.data().kind !== kind) fields.push("kind", kind); tx.update(ref(d), fields[0], fields[1], ...fields.slice(2)); }
        });
      });
    }
    for (const sp of spills) {
      const d = key + "~r~" + hash(sp.id);
      await fs.setDoc(ref(d), { t: "s", k: key, n: -1, kind: kind, m: { [sp.id]: sp.v } });
    }
  };
  handlers._write = write;
  return { kind: "firestore", write: write };
}

// v1 stored each type as one document {v: [...]}. Copy it into shards once.
async function migrate(fs, db, ref) {
  while (!handlers._write) await new Promise(r => setTimeout(r, 50));
  const metaRef = ref("_meta~migrated");
  const meta = await fs.getDoc(metaRef);
  const done = meta.exists() ? (meta.data().keys || {}) : {};
  for (const key of KEYS()) {
    if (done[key]) continue;
    const has = Object.keys(shards).some(d => shards[d].k === key);
    if (!has) {
      const old = await fs.getDoc(ref(key));
      if (old.exists() && old.data().v !== undefined) await handlers._write(key, old.data().v, undefined);
    }
    done[key] = Date.now();
  }
  await fs.setDoc(metaRef, { t: "meta", keys: done }, { merge: true });
}

// Realtime Database: one node per record under ROOT/_r/<key>/<id>, transactions per record.
async function startRtdb(app) {
  const rt = await import(SDK + "firebase-database.js");
  const db = rt.getDatabase(app);
  const R = ROOT();
  const enc = id => id.replace(/[.#$\[\]\/%]/g, c => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  const dec = id => id.replace(/%([0-9A-F]{2})/g, (m, h) => String.fromCharCode(parseInt(h, 16)));
  const getBlob = async id => { const s = await rt.get(rt.ref(db, R + "/_b/" + id)); return s.exists() ? s.val() : null; };
  let firstAll = 0;
  KEYS().forEach(key => {
    rt.onValue(rt.ref(db, R + "/_r/" + key), snap => {
      const v = snap.val() || {};
      const m = {};
      Object.keys(v).filter(x => x !== "_kind").forEach(id => { m[dec(id)] = v[id]; });
      if (snap.exists()) { shards[key + "~0"] = { k: key, kind: v._kind || "arr", m: m }; schedule(key, getBlob); }
      else delete shards[key + "~0"];
      firstAll++;
      if (firstAll === KEYS().length) {
        handlers.onStatus("live", "Realtime Database connected \u2014 per-record sync.");
        migrateRtdb(rt, db, R).catch(() => {});
      }
    }, err => handlers.onStatus("error", friendly(err)));
  });
  const write = async (key, next, prev) => {
    const kind = kindOf(next);
    const N = entries(key, next), P = prev === undefined ? null : entries(key, prev);
    const ids = [];
    N.forEach((v, id) => { if (!P || !P.has(id) || !same(P.get(id), v)) ids.push(id); });
    if (P) P.forEach((v, id) => { if (!N.has(id)) ids.push(id); });
    if (!ids.length) return;
    const found = new Map();
    const ord = kind === "arr" ? orderFor(key, Array.from(N.keys())) : {};
    await rt.set(rt.ref(db, R + "/_r/" + key + "/_kind"), kind);
    for (const id of ids) {
      const mine = N.has(id) ? extract(N.get(id), found) : undefined;
      const base = P && P.has(id) ? extract(P.get(id), null) : undefined;
      for (const [bid, bv] of found) { if (!blobUp.has(bid)) { await rt.set(rt.ref(db, R + "/_b/" + bid), bv); blobUp.add(bid); idbPut(bid, bv); } }
      found.clear();
      await rt.runTransaction(rt.ref(db, R + "/_r/" + key + "/" + enc(id)), cur => {
        const theirs = cur && cur.j ? JSON.parse(cur.j) : undefined;
        const merged = P && P.has(id) ? merge3(base, mine, theirs) : (theirs === undefined ? mine : merge3(undefined, mine, theirs));
        if (merged === undefined) return null;
        return { o: cur && cur.o !== undefined ? cur.o : (ord[id] !== undefined ? ord[id] : Date.now()), j: JSON.stringify(merged) };
      });
    }
  };
  handlers._write = write;
  return { kind: "rtdb", write: write };
}
async function migrateRtdb(rt, db, R) {
  while (!handlers._write) await new Promise(r => setTimeout(r, 50));
  const metaRef = rt.ref(db, R + "/_meta/migrated");
  const meta = (await rt.get(metaRef)).val() || {};
  for (const key of KEYS()) {
    if (meta[key]) continue;
    if (!shards[key + "~0"]) {
      const old = await rt.get(rt.ref(db, R + "/" + key));
      if (old.exists()) await handlers._write(key, old.val(), undefined);
    }
    meta[key] = Date.now();
  }
  await rt.set(metaRef, meta);
}

function friendly(err) {
  const m = (err && err.message) || "Firebase error.";
  if (/permission|insufficient/i.test(m)) return "Firebase refused access. Check the database rule in the publishing guide.";
  if (/too large|exceeds/i.test(m)) return "A record is too large to share. Use links for big files.";
  return m;
}

// Saves run one after another per data type so their order is kept.
export function push(key, value, prev) {
  if (!api || KEYS().indexOf(key) < 0) return;
  queues[key] = (queues[key] || Promise.resolve()).then(() => api.write(key, value, prev)).catch(e => handlers.onStatus("error", friendly(e)));
}
