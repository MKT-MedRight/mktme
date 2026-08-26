// ─────────────────────────────────────────────────────────────
// Med Right Policy Hub — Firebase connection
//
// Paste the config from your Firebase project here:
//   Firebase console → Project settings → General → Your apps
//   → Web app → "SDK setup and configuration" → Config
//
// Leave apiKey empty and the hub simply runs offline in each
// browser, exactly as before.
// ─────────────────────────────────────────────────────────────

window.MRHUB_FIREBASE = {
  apiKey: "",
  authDomain: "",
  projectId: "",
  storageBucket: "",
  messagingSenderId: "",
  appId: "",
  // Only if you are using the Realtime Database (not Firestore):
  databaseURL: ""
};

// "auto"      — Realtime Database when databaseURL is set, otherwise Firestore
// "firestore" — force Cloud Firestore
// "rtdb"      — force Realtime Database
window.MRHUB_MODE = "auto";

// Everything is stored under this one place, so several hubs can
// share a project if you ever need that.
window.MRHUB_ROOT = "medright-policy-hub";
