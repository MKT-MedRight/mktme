// Live sync between MESH (Med Right Employees Service Hub) and Firebase.
// Loaded on demand by the hub; does nothing until firebase-config.js has an apiKey.

import "./firebase-config.js";

const CFG = () => window.MRHUB_FIREBASE || {};
const ROOT = () => window.MRHUB_ROOT || "medright-policy-hub";
const KEYS = ["policies", "accounts", "depts", "cats", "inbox", "notes", "userdata",
  "theme", "ann", "quotes", "chars", "forms", "responses", "svc", "handlers"];
const SDK = "https://www.gstatic.com/firebasejs/10.12.2/";

export function enabled() {
  const c = CFG();
  return !!(c && c.apiKey && (c.projectId || c.databaseURL));
}

function mode() {
  const m = window.MRHUB_MODE || "auto";
  if (m === "firestore" || m === "rtdb") return m;
  return CFG().databaseURL ? "rtdb" : "firestore";
}

let api = null;

export async function start(handlers) {
  const onStatus = handlers.onStatus || function () {};
  const onData = handlers.onData || function () {};
  if (!enabled()) { onStatus("off", "No Firebase config yet."); return null; }

  try {
    const appMod = await import(SDK + "firebase-app.js");
    const app = appMod.initializeApp(CFG());

    if (mode() === "rtdb") {
      const db = await import(SDK + "firebase-database.js");
      const inst = db.getDatabase(app);
      KEYS.forEach(k => {
        db.onValue(db.ref(inst, ROOT() + "/" + k), snap => {
          const v = snap.val();
          if (v !== null && v !== undefined) onData(k, v);
        }, err => onStatus("error", err.message));
      });
      api = { kind: "rtdb", push: (k, v) => db.set(db.ref(inst, ROOT() + "/" + k), v) };
    } else {
      const fs = await import(SDK + "firebase-firestore.js");
      const inst = fs.getFirestore(app);
      KEYS.forEach(k => {
        fs.onSnapshot(fs.doc(inst, ROOT(), k), snap => {
          const d = snap.data();
          if (d && d.v !== undefined) onData(k, d.v);
        }, err => onStatus("error", err.message));
      });
      api = { kind: "firestore", push: (k, v) => fs.setDoc(fs.doc(inst, ROOT(), k), { v: v, at: Date.now() }) };
    }

    onStatus("live", (api.kind === "rtdb" ? "Realtime Database" : "Cloud Firestore") + " connected.");
    return api;
  } catch (e) {
    onStatus("error", e && e.message ? e.message : "Could not reach Firebase.");
    return null;
  }
}

export function push(key, value) {
  if (!api || KEYS.indexOf(key) < 0) return;
  try {
    const p = api.push(key, value);
    if (p && p.catch) p.catch(() => {});
  } catch (e) {}
}
