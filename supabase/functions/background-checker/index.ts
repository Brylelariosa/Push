const SUPA_URL = Deno.env.get('SUPABASE_URL');
const SUPA_KEY = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
const VAPID_PUB = Deno.env.get('VAPID_PUBLIC_KEY') || '';
const VAPID_PRV = Deno.env.get('VAPID_PRIVATE_KEY_JWK') || '';
const MD = 'https://api.mangadex.org';
const CMK = 'https://api.comick.fun';
const DAY = 86400000;
const logs = [];

const UAS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
];

const PROXIES = [
  'https://api.allorigins.win/raw?url=',
  'https://corsproxy.io/?url=',
  'https://cors.eu.org/',
  'https://api.codetabs.com/v1/proxy?quest=',
  'https://corsproxy.org/?',
];

function ua() { return UAS[Math.floor(Math.random() * UAS.length)]; }
function log(tag, msg) {
  const ts = new Date().toISOString().slice(11, 19); // HH:MM:SS
  const s = ts + ' [' + tag + '] ' + msg;
  logs.push(s);
  console.log(s);
}

// Returns null if the manga is due for a check, or a human-readable skip reason.
// Mirrors isDue() logic but explains why something is skipped.
function isDueReason(m, now) {
  const scrapeAt = m.scrapeAt || 0;
  const lastUpdated = m.lastUpdated || 0;
  const addedAt = m.addedAt || 0;
  const hist = m.updateHistory || [];
  const avg = m.avgUpdateIntervalMs || DAY;
  if (!scrapeAt) return null; // never checked — always due
  if (lastUpdated && now - lastUpdated < DAY)
    return 'updated ' + Math.round((now - lastUpdated) / 3600000) + 'h ago';
  if (lastUpdated && now - lastUpdated > avg * 2)
    return (now - scrapeAt < DAY) ? 'checked ' + Math.round((now - scrapeAt) / 3600000) + 'h ago' : null;
  if (hist.length < 5) {
    // Not enough history to predict schedule — check every 1h so we don't miss early chapters
    const interval = 3600000; // 1 hour
    return (now - scrapeAt < interval)
      ? 'checked ' + Math.round((now - scrapeAt) / 60000) + 'min ago (every 1h, sparse data)'
      : null;
  }  const day = detectDay(hist);
  if (day >= 0) {
    const today = jstDay(now);
    const diff = Math.min(Math.abs(today - day), 7 - Math.abs(today - day));
    if (diff > 1) return 'not update day (expects ' + ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][day] + ')';
    return (now - scrapeAt < 2 * 3600000) ? 'checked ' + Math.round((now - scrapeAt) / 3600000) + 'h ago' : null;
  }
  const nextCheck = m.nextCheckAt || 0;
  if (nextCheck && now < nextCheck)
    return 'next in ' + Math.round((nextCheck - now) / 3600000) + 'h';
  return null;
}

async function sb(path, opts) {
  if (!opts) opts = {};
  const hdrs = { 'Content-Type': 'application/json', 'apikey': SUPA_KEY, 'Authorization': 'Bearer ' + SUPA_KEY };
  if (opts.headers) Object.assign(hdrs, opts.headers);
  opts.headers = hdrs;
  const res = await fetch(SUPA_URL + path, opts);
  const txt = await res.text().catch(() => '');
  if (!res.ok) throw new Error('SB ' + res.status + ' ' + txt.slice(0, 100));
  return txt ? JSON.parse(txt) : null;
}

function timer(ms) {
  return new Promise(function(_, r) { setTimeout(function() { r(new Error('timeout')); }, ms); });
}

async function getHtml(url) {
  try {
    const r = await Promise.race([
      fetch(url, { headers: { 'User-Agent': ua(), 'Accept': 'text/html,*/*' } }),
      timer(8000),
    ]);
    if (r.ok) {
      const h = await r.text();
      if (h.length > 500) return h;
    }
  } catch(e) { /* try proxies */ }
  return new Promise(function(resolve) {
    let done = false;
    let n = 0;
    for (let i = 0; i < PROXIES.length; i++) {
      const p = PROXIES[i];
      Promise.race([
        fetch(p + encodeURIComponent(url), { headers: { 'User-Agent': ua() } }),
        timer(12000),
      ]).then(async function(r) {
        n++;
        if (!done && r.ok) {
          const h = await r.text();
          if (h.length > 500) { done = true; resolve(h); return; }
        }
        if (n >= PROXIES.length && !done) resolve(null);
      }).catch(function() {
        n++;
        if (n >= PROXIES.length && !done) resolve(null);
      });
    }
  });
}

async function getJson(url, hdrs) {
  try {
    const h = Object.assign({ 'User-Agent': ua(), 'Accept': 'application/json' }, hdrs || {});
    const r = await Promise.race([fetch(url, { headers: h }), timer(10000)]);
    return r.ok ? await r.json() : null;
  } catch(e) { return null; }
}

function parse(html, url) {
  // Madara fingerprint
  if (html.includes('wp-manga-chapter') || html.includes('listing-chapters_wrap')) {
    const ns = [];
    const re1 = />\s*Chapter\s+([0-9]+(?:\.[0-9]+)?)\s*</gi;
    const re2 = /href="[^"]*\/chapter-([0-9]+(?:\.[0-9]+)?)\//gi;
    let m;
    while ((m = re1.exec(html)) !== null) ns.push(parseFloat(m[1]));
    while ((m = re2.exec(html)) !== null) ns.push(parseFloat(m[1]));
    const v = ns.filter(function(n) { return n > 0 && n < 99999; });
    if (v.length) return { ch: Math.max.apply(null, v), conf: 'HIGH', how: 'madara-html' };
  }
  // Next.js __NEXT_DATA__
  const ndMatch = html.match(/<script[^>]+id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i);
  if (ndMatch) {
    try {
      const s = JSON.stringify(JSON.parse(ndMatch[1]));
      const re = /"(?:chapter_number|chapterNumber|chap|chapter|number)"\s*:\s*"?(\d+(?:\.\d+)?)"?/gi;
      const ns = [];
      let m;
      while ((m = re.exec(s)) !== null) {
        const n = parseFloat(m[1]);
        if (n > 0 && n < 9999) ns.push(n);
      }
      if (ns.length) return { ch: Math.max.apply(null, ns), conf: 'HIGH', how: 'next-data' };
    } catch(e) { /* skip */ }
  }
  // SSR state
  const ssrPats = [
    /window\.__NUXT__\s*=\s*(\{[\s\S]{0,60000}?\})\s*;/,
    /window\.__data__\s*=\s*(\{[\s\S]{0,60000}?\})\s*;/,
  ];
  for (let i = 0; i < ssrPats.length; i++) {
    const sm = html.match(ssrPats[i]);
    if (sm) {
      try {
        const re = /"(?:chapter|chap|num)"\s*:\s*"?(\d+(?:\.\d+)?)"?/gi;
        const ns = [];
        let m;
        while ((m = re.exec(sm[1])) !== null) {
          const n = parseFloat(m[1]);
          if (n > 0 && n < 9999) ns.push(n);
        }
        if (ns.length) return { ch: Math.max.apply(null, ns), conf: 'MEDIUM', how: 'ssr-state' };
      } catch(e) { /* skip */ }
    }
  }
  // Weighted scoring
  const cands = new Map();
  function add(n, w) {
    if (n > 0 && n < 99999 && !isNaN(n)) cands.set(n, (cands.get(n) || 0) + w);
  }
  const pats = [
    [/href="[^"]*\/(?:chapter|ch|ep)[-_](\d+(?:\.\d+)?)["?#\/]/gi, 10],
    [/href="[^"]*\/chapter-(\d+(?:\.\d+)?)\//gi, 10],
    [/data-(?:chapter|chap|num)\s*=\s*"(\d+(?:\.\d+)?)"/gi, 8],
    [/"(?:chapter|chap|chapterNumber|chapter_number)"\s*:\s*"?(\d+(?:\.\d+)?)"?/gi, 7],
    [/>\s*Chapter\s+(\d+(?:\.\d+)?)\s*</gi, 5],
  ];
  for (let i = 0; i < pats.length; i++) {
    let m;
    while ((m = pats[i][0].exec(html)) !== null) add(parseFloat(m[1]), pats[i][1]);
  }
  const nc = html.match(/New\s+Chapter[\s\S]{0,80}?(?:Chapter|Ch\.?)\s*([0-9]+(?:\.[0-9]+)?)/i);
  if (nc) add(parseFloat(nc[1]), 9);
  const slugM = url.match(/\/(?:manga|comic|series|manhwa|manhua|title)\/([^/?#]+)/i);
  if (slugM) {
    try {
      const slug = slugM[1].replace(/\.[a-z0-9]{3,8}$/, '').replace(/-\d+$/, '');
      const esc = slug.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/-/g, '[-_]');
      const sre = new RegExp(esc + '[-_](?:chapter[-_])?([0-9]+(?:\\.[0-9]+)?)', 'gi');
      let m;
      while ((m = sre.exec(html)) !== null) add(parseFloat(m[1]), 6);
    } catch(e) { /* skip */ }
  }
  if (!cands.size) {
    const re = /(?:chapter|ch\.?)\s*([0-9]+(?:\.[0-9]+)?)/gi;
    let m;
    while ((m = re.exec(html)) !== null) add(parseFloat(m[1]), 1);
  }
  if (!cands.size) return null;
  let best = 0, score = 0;
  cands.forEach(function(s, n) {
    if (s > score || (s >= score - 2 && n > best)) { best = n; score = s; }
  });
  if (!best) return null;
  const conf = score >= 10 ? 'HIGH' : score >= 5 ? 'MEDIUM' : 'LOW';
  const how = score >= 10 ? 'href' : score >= 7 ? 'json' : score >= 5 ? 'text' : 'last-resort';
  return { ch: best, conf: conf, how: how };
}

async function mdCheck(id) {
  try {
    const d = await getJson(MD + '/manga/' + id + '/aggregate');
    if (!d) return null;
    const ns = [];
    const vols = Object.values(d.volumes || {});
    for (let i = 0; i < vols.length; i++) {
      const chs = Object.values(vols[i].chapters || {});
      for (let j = 0; j < chs.length; j++) {
        const n = parseFloat(chs[j].chapter);
        if (!isNaN(n) && n > 0 && n < 99999) ns.push(n);
      }
    }
    return ns.length ? Math.max.apply(null, ns) : null;
  } catch(e) { return null; }
}

async function comickCheck(url) {
  const m = url.match(/\/(?:title|comic|manga|series)\/([^/?#]+)/i);
  if (!m) return null;
  const d = await getJson(CMK + '/comic/' + m[1].replace(/\/$/, ''));
  const c = d && d.comic;
  if (!c) return null;
  const last = parseFloat(c.last_chapter);
  if (last > 0) return last;
  if (c.hid) {
    const r = await getJson(CMK + '/comic/' + c.hid + '/chapters?limit=1&order=desc&lang=en');
    const ch = r && r.chapters && r.chapters[0] && parseFloat(r.chapters[0].chap);
    if (ch > 0) return ch;
  }
  return null;
}

async function mangafireCheck(url) {
  const im = url.match(/\/manga\/[^/?#]*\.([a-z0-9]+)(?:[?#\/]|$)/i);
  if (im) {
    const d = await getJson('https://mangafire.to/ajax/manga/' + im[1] + '/chapter/en', { 'Referer': 'https://mangafire.to' });
    if (d && d.result && typeof d.result === 'string') {
      const re = /data-number="([0-9]+(?:\.[0-9]+)?)"/gi;
      const ns = [];
      let m;
      while ((m = re.exec(d.result)) !== null) {
        const n = parseFloat(m[1]);
        if (n > 0 && n < 99999) ns.push(n);
      }
      if (ns.length) return Math.max.apply(null, ns);
    }
  }
  const h = await getHtml(url);
  if (!h) return null;
  const re = /data-number="([0-9]+(?:\.[0-9]+)?)"/gi;
  const ns = [];
  let m;
  while ((m = re.exec(h)) !== null) {
    const n = parseFloat(m[1]);
    if (n > 0 && n < 99999) ns.push(n);
  }
  return ns.length ? Math.max.apply(null, ns) : null;
}

async function madaraCheck(url) {
  const bm = url.match(/^(https?:\/\/[^/]+)/);
  const base = bm ? bm[1] : null;
  const sm = url.match(/\/(?:manga|comic|manhwa|manhua|webtoon|series)\/([^/?#]+)/i);
  const slug = sm ? sm[1].replace(/\/$/, '') : null;
  if (base && slug) {
    const api = await getJson(base + '/wp-json/wp/v2/wp-manga?slug=' + slug + '&_fields=id');
    if (Array.isArray(api) && api.length) {
      const chapUrl = base + '/wp-json/wp/v2/wp-manga-chapter?manga=' + api[0].id
        + '&per_page=1&orderby=chapter_index&order=desc&_fields=chapter_title_raw';
      const chaps = await getJson(chapUrl);
      if (Array.isArray(chaps) && chaps.length) {
        const raw = (chaps[0].chapter_title_raw || '').replace(/[^0-9.]/g, '');
        const n = parseFloat(raw);
        if (n > 0) return n;
      }
    }
  }
  const h = await getHtml(url);
  if (!h) return null;
  const r = parse(h, url);
  return r ? r.ch : null;
}

async function weebcentralCheck(url) {
  const im = url.match(/\/series\/([^/?#]+)/i);
  if (!im) return null;
  const bm = url.match(/^(https?:\/\/[^/]+)/);
  const base = bm ? bm[1] : 'https://weebcentral.com';
  const paths = ['/series/' + im[1] + '/full-chapter-list', '/series/' + im[1]];
  for (let i = 0; i < paths.length; i++) {
    const h = await getHtml(base + paths[i]);
    if (h) { const r = parse(h, url); if (r) return r.ch; }
  }
  return null;
}

async function urlCheck(url) {
  if (!url) return null;
  if (url.includes('comick.')) {
    const n = await comickCheck(url);
    return n ? { ch: n, conf: 'HIGH', how: 'comick-api' } : null;
  }
  if (url.includes('mangafire.to')) {
    const n = await mangafireCheck(url);
    return n ? { ch: n, conf: 'HIGH', how: 'mangafire-api' } : null;
  }
  if (url.includes('weebcentral.com')) {
    const n = await weebcentralCheck(url);
    return n ? { ch: n, conf: 'HIGH', how: 'weebcentral' } : null;
  }
  const MADARA = ['manhwaclan','manhwatop','manhuaplus','toonily','nightscans','zinmanga','isekaiscan'];
  const NEXTJS = ['asurascans','asura.gg','flamescans','reaperscans'];
  const isMadara = MADARA.some(function(d) { return url.includes(d); });
  const isNextJs = NEXTJS.some(function(d) { return url.includes(d); });
  if (isMadara && !isNextJs) {
    const n = await madaraCheck(url);
    return n ? { ch: n, conf: 'HIGH', how: 'madara' } : null;
  }
  const h = await getHtml(url);
  return h ? parse(h, url) : null;
}

function jstDay(ts) {
  return new Date(ts + 9 * 3600000).getUTCDay();
}

function detectDay(hist) {
  if (hist.length < 5) return -1;
  const counts = [0,0,0,0,0,0,0];
  for (let i = 0; i < hist.length; i++) counts[jstDay(hist[i])]++;
  const max = Math.max.apply(null, counts);
  if (max / hist.length < 0.6) return -1;
  return counts.indexOf(max);
}

function isDue(m, now) { return isDueReason(m, now) === null; }

function recordUpdate(m, now) {
  const hist = (m.updateHistory || []).concat([now]);
  const trimmed = hist.length > 10 ? hist.slice(hist.length - 10) : hist;
  m.updateHistory = trimmed;
  if (trimmed.length >= 2) {
    let gap = 0;
    for (let i = 1; i < trimmed.length; i++) gap += trimmed[i] - trimmed[i - 1];
    m.avgUpdateIntervalMs = Math.round(gap / (trimmed.length - 1));
  }
  const avg = m.avgUpdateIntervalMs || DAY;
  m.nextCheckAt = now + avg - DAY * 2;
}

async function vapidJWT(aud) {
  const priv = JSON.parse(VAPID_PRV);
  const now = Math.floor(Date.now() / 1000);
  function b64u(s) { return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, ''); }
  const hdr = b64u(JSON.stringify({ typ: 'JWT', alg: 'ES256' }));
  const pay = b64u(JSON.stringify({ aud: aud, exp: now + 43200, sub: 'mailto:admin@mangatrack.app' }));
  const inp = hdr + '.' + pay;
  const key = await crypto.subtle.importKey('jwk', priv, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['sign']);
  const sig = new Uint8Array(await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, key, new TextEncoder().encode(inp)));
  return inp + '.' + btoa(String.fromCharCode.apply(null, sig)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

async function sendPush(uid, endpoint, title, body) {
  try {
    await sb('/rest/v1/notif_queue', {
      method: 'POST',
      headers: { 'Prefer': 'resolution=merge-duplicates' },
      body: JSON.stringify({ user_id: uid, title: title, body: body, updated_at: Date.now() }),
    });
  } catch(e) { /* skip */ }
  if (!VAPID_PRV || !VAPID_PUB) return false;
  try {
    const jwt = await vapidJWT(new URL(endpoint).origin);
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Authorization': 'vapid t=' + jwt + ',k=' + VAPID_PUB, 'TTL': '86400', 'Urgency': 'normal' },
    });
    return res.status < 300 || res.status === 201;
  } catch(e) { return false; }
}

Deno.serve(async function(_req) {
  logs.length = 0;
  const jitter = Math.floor(Math.random() * 45000);
  await new Promise(function(r) { setTimeout(r, jitter); });
  log('INFO', 'Jitter: ' + Math.round(jitter / 1000) + 's');
  const t0 = Date.now();
  try {
    const users = await sb('/rest/v1/user_data?select=id,lib');
    if (!users || !users.length) {
      return new Response(JSON.stringify({ checked: 0 }), { headers: { 'Content-Type': 'application/json' } });
    }
    const rawSubs = await sb('/rest/v1/push_subscriptions?select=user_id,endpoint').catch(function() { return []; });
    const subs = rawSubs || [];
    const subMap = new Map();
    for (let i = 0; i < subs.length; i++) subMap.set(subs[i].user_id, subs[i].endpoint);
    const userLibs = new Map();
    const mdCache = new Map();
    const urlCache = new Map();
    const mdTitle = new Map();
    const urlTitle = new Map();
    const mdSet = new Set();
    const urlSet = new Set();
    const now = Date.now();
    let skippedTotal = 0;
    let newFoundCount = 0;
    let failedCount = 0;
    const skippedSample = []; // up to 5 examples for the log
    for (let i = 0; i < users.length; i++) {
      try {
        const lib = JSON.parse(users[i].lib || '[]');
        userLibs.set(users[i].id, lib);
        for (let j = 0; j < lib.length; j++) {
          const m = lib[j];
          if (m.archived) continue;
          if (m.readingStatus === 'completed' && m.seriesStatus === 'completed') continue;
          const skipReason = isDueReason(m, now);
          if (skipReason !== null) {
            skippedTotal++;
            if (skippedSample.length < 5) skippedSample.push(m.title.slice(0, 22) + ' [' + skipReason + ']');
            continue;
          }
          if (m.customUrl) {
            urlSet.add(m.customUrl);
            if (!urlTitle.has(m.customUrl)) urlTitle.set(m.customUrl, m.title);
          } else if (m.mdId) {
            mdSet.add(m.mdId);
            if (!mdTitle.has(m.mdId)) mdTitle.set(m.mdId, m.title);
          }
        }
      } catch(e) { /* skip */ }
    }
    log('INFO', 'users:' + users.length + ' to-check:' + (mdSet.size + urlSet.size) + ' MD:' + mdSet.size + ' URL:' + urlSet.size + ' skipped:' + skippedTotal);
    if (skippedSample.length) log('SKIP', skippedSample.join(' | ') + (skippedTotal > 5 ? ' +' + (skippedTotal - 5) + ' more' : ''));
    const mdIds = Array.from(mdSet);
    for (let i = 0; i < mdIds.length; i += 5) {
      const batch = mdIds.slice(i, i + 5);
      await Promise.all(batch.map(async function(id) {
        const t1 = Date.now();
        const n = await mdCheck(id);
        mdCache.set(id, n);
        const t = mdTitle.get(id) || id.slice(0, 8);
        const ms = Date.now() - t1;
        log(n !== null ? 'MD' : 'WARN', t + ' -> ' + (n !== null ? n : 'null') + ' (' + ms + 'ms)');
      }));
      if (i + 5 < mdIds.length) await new Promise(function(r) { setTimeout(r, 300); });
    }
    const urls = Array.from(urlSet);
    for (let i = 0; i < urls.length; i += 4) {
      const batch = urls.slice(i, i + 4);
      await Promise.all(batch.map(async function(url) {
        const t1 = Date.now();
        const r = await urlCheck(url);
        urlCache.set(url, r);
        const t = urlTitle.get(url) || url.replace(/^https?:\/\//, '').split('/')[0];
        const ms = Date.now() - t1;
        if (r) log('URL', t + ' -> Ch.' + r.ch + ' [' + r.conf + ':' + r.how + '] (' + ms + 'ms)');
        else log('WARN', t + ' -> null (' + ms + 'ms)');
      }));
      if (i + 4 < urls.length) await new Promise(function(r) { setTimeout(r, 400); });
    }
    let notified = 0;
    for (let i = 0; i < users.length; i++) {
      try {
        const lib = userLibs.get(users[i].id) || [];
        const newChs = [];
        let changed = false;
        for (let j = 0; j < lib.length; j++) {
          const m = lib[j];
          if (m.archived) continue;
          if (m.readingStatus === 'completed' && m.seriesStatus === 'completed') continue;
          let latest = null;
          let conf = 'HIGH';
          let how = 'mangadex';
          // Was this manga actually attempted this run, or skipped by isDue?
          const wasChecked = (m.customUrl && urlSet.has(m.customUrl)) || (!m.customUrl && m.mdId && mdSet.has(m.mdId));
          if (m.customUrl) {
            const ur = urlCache.get(m.customUrl) || null;
            if (ur) { latest = ur.ch; conf = ur.conf; how = ur.how; }
            if (!latest && m.mdId) { latest = mdCache.get(m.mdId) || null; how = 'md-fallback'; }
          } else if (m.mdId) {
            latest = mdCache.get(m.mdId) || null;
          }
          // Only touch scrape metadata for items actually attempted this run.
          // Skipped items keep their existing scrapeOk/scrapeAt/scrapeFailCount unchanged.
          if (wasChecked) {
            m.scrapeAt = now;
            if (latest !== null) {
              m.scrapeOk = true;
              m.scrapeFailCount = 0;
            } else {
              m.scrapeOk = false;
              m.scrapeFailCount = (m.scrapeFailCount || 0) + 1;
              failedCount++;
              if (m.scrapeFailCount >= 3) log('WARN', m.title + ': scrape fail #' + m.scrapeFailCount);
              changed = true;
            }
          }
          const stored = m.chLatest || 0;
          const plan = m.readingStatus === 'plan';
          if (latest && latest > stored) {
            const jump = latest - stored;
            if (stored > 0 && jump > 50 && conf !== 'HIGH' && m.mdId) {
              const mdN = mdCache.get(m.mdId) || null;
              if (mdN && Math.abs(mdN - latest) > 10) {
                log('WARN', m.title + ': jump ' + stored + '->' + latest + ' vs MD:' + mdN + ' override');
                latest = mdN; conf = 'HIGH'; how = 'md-override';
              }
            }
            m.chLatest = latest;
            m.lastUpdated = now;
            m.hasNew = !plan && latest > (m.chRead || 0);
            recordUpdate(m, now);
            changed = true;
            newFoundCount++;
            const nextStr = m.nextCheckAt ? new Date(m.nextCheckAt).toDateString() : '?';
            log(plan ? 'INFO' : 'NEW', m.title + ': ' + stored + '->' + latest + ' [' + conf + ':' + how + ']' + (plan ? ' [plan]' : '') + ' next:' + nextStr);
            if (!plan) newChs.push(m.title + ' Ch.' + latest);
          } else if (latest && latest < stored) {
            log('SKIP', m.title + ' got=' + latest + ' stored=' + stored);
          }
        }
        if (!changed) continue;
        await sb('/rest/v1/user_data', {
          method: 'POST',
          headers: { 'Prefer': 'resolution=merge-duplicates' },
          body: JSON.stringify({ id: users[i].id, lib: JSON.stringify(lib), updated_at: now }),
        });
        const ep = subMap.get(users[i].id) || '';
        if (ep && newChs.length > 0) {
          const nt = newChs.length === 1 ? newChs[0] : newChs.length + ' new chapters!';
          const nb = newChs.length === 1
            ? 'New chapter out - tap to open!'
            : newChs.slice(0, 3).join(', ') + (newChs.length > 3 ? ' +' + (newChs.length - 3) + ' more' : '');
          const ok = await sendPush(users[i].id, ep, nt, nb);
          if (ok) { notified++; log('PUSH', nt); }
          else await sb('/rest/v1/push_subscriptions?user_id=eq.' + users[i].id, { method: 'DELETE' }).catch(function() {});
        }
      } catch(e) { log('ERR', 'user ' + users[i].id.slice(0, 8) + ': ' + String(e)); }
    }
    const elapsed = Date.now() - t0;
    log('DONE', 'elapsed:' + elapsed + 'ms checked:' + (mdSet.size + urlSet.size) + ' new:' + newFoundCount + ' failed:' + failedCount + ' notified:' + notified + ' skipped:' + skippedTotal);
    try {
      await sb('/rest/v1/checker_log', {
        method: 'POST',
        headers: { 'Prefer': 'return=minimal' },
        body: JSON.stringify({ ran_at: t0, elapsed_ms: elapsed, users_checked: users.length, logs: logs.join('\n') }),
      });
    } catch(e) { /* skip */ }
    return new Response(
      JSON.stringify({ checked: users.length, notified: notified, elapsed_ms: elapsed }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  } catch(e) {
    return new Response('Error: ' + String(e), { status: 500 });
  }
});
