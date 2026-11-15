#!/usr/bin/env node
// ONAIR.MK news feed builder.
// Fetches RSS/Atom feeds of Macedonian news portals and the YouTube RSS feeds
// of Macedonian channels, then writes static JSON files that onair.mk reads.
// No database, no API keys, no AI. Node 20+, zero dependencies.

import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.resolve(ROOT, process.env.FEED_OUT || "out");

const NEWS_KEEP_DAYS = 4;
const NEWS_MAX = 2400;
const LATEST_COUNT = 300;
const ITEMS_PER_SOURCE = 20;
const VIDEO_KEEP_DAYS = 30;
const VIDEO_MAX = 1500;
const VIDEOS_PER_CHANNEL = 15;
const IMAGE_SCRAPES_PER_RUN = 60;
const FETCH_TIMEOUT_MS = 12000;
const UA =
  "Mozilla/5.0 (compatible; ONAIR-MK-Feed/1.0; +https://onair.mk) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36";

// ───────────────────────── helpers ─────────────────────────

const sha = (s) => createHash("sha1").update(s).digest("hex");

async function readJson(file, fallback) {
  try {
    return JSON.parse(await readFile(file, "utf8"));
  } catch {
    return fallback;
  }
}

const FEED_MAX_BYTES = 3 * 1024 * 1024;
const PAGE_MAX_BYTES = 512 * 1024;

/** Only plain public http(s) hosts: no IP literals, no localhost, no internal names. */
function isPublicHttpUrl(value) {
  let u;
  try {
    u = new URL(value);
  } catch {
    return false;
  }
  if (u.protocol !== "https:" && u.protocol !== "http:") return false;
  if (u.username || u.password) return false;
  const host = u.hostname.toLowerCase();
  if (!host.includes(".") || host.endsWith(".")) return false;
  if (/^[0-9.]+$/.test(host) || host.includes(":") || host.startsWith("[")) return false;
  if (/(^|\.)(localhost|local|internal|lan|home|corp|test|invalid|example)$/.test(host)) return false;
  return true;
}

const TWO_LEVEL_SUFFIX = /\.(com|org|net|edu|gov|inf)\.[a-z]{2}$|\.(eu|uk|us)\.com$|\.co\.[a-z]{2}$/;

function baseDomain(value) {
  try {
    const host = new URL(value).hostname.toLowerCase().replace(/^www\./, "");
    const labels = host.split(".");
    return labels.slice(TWO_LEVEL_SUFFIX.test(host) ? -3 : -2).join(".");
  } catch {
    return "";
  }
}

/** True when `url` belongs to the same site as the configured source. */
function isSameSite(url, source) {
  const domain = baseDomain(url);
  return Boolean(domain) && (domain === baseDomain(source.site || "") || domain === baseDomain(source.feed || ""));
}

const PRIVATE_V4 = [
  [0x00000000, 8], [0x0a000000, 8], [0x64400000, 10], [0x7f000000, 8], [0xa9fe0000, 16], [0xac100000, 12],
  [0xc0000000, 24], [0xc0a80000, 16], [0xc6120000, 15], [0xe0000000, 3],
];

function isPrivateAddress(address, family) {
  if (family === 4) {
    const parts = address.split(".").map(Number);
    const n = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
    return PRIVATE_V4.some(([base, bits]) => n >>> (32 - bits) === base >>> (32 - bits));
  }
  const a = address.toLowerCase();
  if (a === "::" || a === "::1") return true;
  if (a.startsWith("::ffff:")) return isPrivateAddress(a.slice(7), 4);
  return /^(fc|fd|fe8|fe9|fea|feb|ff)/.test(a); // unique-local, link-local, multicast
}

/** Reject names that resolve to loopback, private or link-local addresses. */
async function assertPublicHost(url) {
  if (!isPublicHttpUrl(url)) throw new Error("blocked url");
  const addresses = await lookup(new URL(url).hostname, { all: true });
  if (!addresses.length || addresses.some((entry) => isPrivateAddress(entry.address, entry.family))) throw new Error("blocked address");
}

async function fetchText(url, { timeout = FETCH_TIMEOUT_MS, accept, maxBytes = FEED_MAX_BYTES, allow } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    let current = url;
    let res;
    // Redirects are followed by hand so every hop is checked before it is requested.
    for (let hop = 0; ; hop++) {
      if (hop > 5) throw new Error("too many redirects");
      if (allow && hop > 0 && !allow(current)) throw new Error("blocked redirect");
      await assertPublicHost(current);
      res = await fetch(current, {
        redirect: "manual",
        signal: controller.signal,
        headers: {
          "User-Agent": UA,
          Accept: accept || "application/rss+xml, application/atom+xml, application/xml, text/xml, */*;q=0.8",
          "Accept-Language": "mk,sq;q=0.9,en;q=0.7",
        },
      });
      if (res.status < 300 || res.status >= 400) break;
      const location = res.headers.get("location");
      await res.body?.cancel();
      if (!location) throw new Error(`HTTP ${res.status}`);
      current = new URL(location, current).toString();
    }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    if (Number(res.headers.get("content-length") || 0) > maxBytes) throw new Error("response too large");
    const reader = res.body.getReader();
    const chunks = [];
    let size = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      chunks.push(value);
      if (size >= maxBytes) {
        await reader.cancel();
        break;
      }
    }
    return new TextDecoder("utf-8").decode(Buffer.concat(chunks).subarray(0, maxBytes));
  } finally {
    clearTimeout(timer);
  }
}

async function pool(items, size, worker) {
  const results = new Array(items.length);
  let next = 0;
  const run = async () => {
    while (next < items.length) {
      const i = next++;
      try {
        results[i] = await worker(items[i], i);
      } catch (error) {
        results[i] = { error: String(error?.message || error) };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(size, items.length) }, run));
  return results;
}

const ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ", laquo: "«", raquo: "»", ndash: "–", mdash: "—", hellip: "…", bdquo: "„", ldquo: "“", rdquo: "”", lsquo: "‘", rsquo: "’" };

function decodeEntities(text) {
  if (!text) return "";
  return text
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => safeCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, d) => safeCodePoint(parseInt(d, 10)))
    .replace(/&([a-z]+);/gi, (m, name) => ENTITIES[name.toLowerCase()] ?? m);
}

function safeCodePoint(n) {
  try {
    return String.fromCodePoint(n);
  } catch {
    return "";
  }
}

const stripCdata = (s) => (s || "").replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1");

function cleanText(html) {
  // Decode twice: feeds often double-encode entities inside CDATA.
  const decoded = decodeEntities(decodeEntities(stripCdata(html)));
  return decoded
    .replace(/<(script|style)[\s\S]*?<\/\1>/gi, " ")
    .replace(/<[^>]*>?/g, " ") // also drops an unterminated trailing tag
    .replace(/[<>]/g, " ") // plain text only: no markup characters survive
    .replace(/\s+/g, " ")
    .trim();
}

function snippet(text, max = 200) {
  const clean = cleanText(text)
    .replace(/\s*(The post|Објавата)\b.*$/i, "")
    .replace(/^.*(Embedded video for|Фото за вовед).*$/i, "") // player/caption boilerplate, not a summary
    .replace(/\s*\[(&hellip;|…|\.\.\.)\]\s*$/, "…")
    .trim();
  if (clean.length <= max) return clean;
  const cut = clean.slice(0, max);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > max * 0.5 ? cut.slice(0, lastSpace) : cut).replace(/[,;:.\-–\s]+$/, "") + "…";
}

function tag(block, name) {
  const re = new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, "i");
  const m = re.exec(block);
  return m ? m[1].trim() : "";
}

function isHttpUrl(value) {
  try {
    const u = new URL(value);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

const AD_URL = /ireklama\.|doubleclick|googlesyndication|adserver|\/delivery\/|bannerid=|zoneid=|pixel\.|tracking\.|gravatar\.com|feedburner|\/emoji\/|wp-includes\/images|1x1|spacer|blank\.(gif|png)/i;

function normalizeImage(raw, base) {
  if (!raw) return null;
  let url = decodeEntities(stripCdata(raw)).trim();
  if (!url) return null;
  if (url.startsWith("//")) url = "https:" + url;
  try {
    url = new URL(url, base).toString();
  } catch {
    return null;
  }
  if (!/^https?:\/\//i.test(url) || AD_URL.test(url) || !isPublicHttpUrl(url)) return null;
  if (/\.(svg|ico)(\?|$)/i.test(url)) return null;
  return url.replace(/^http:\/\//i, "https://");
}

function imageFromItem(block, base) {
  const candidates = [];
  const push = (v) => v && candidates.push(v);
  for (const m of block.matchAll(/<media:content\b[^>]*>/gi)) {
    const t = m[0];
    const url = /url=["']([^"']+)["']/i.exec(t)?.[1];
    const type = /type=["']([^"']+)["']/i.exec(t)?.[1] || "";
    const medium = /medium=["']([^"']+)["']/i.exec(t)?.[1] || "";
    if (url && (type.startsWith("image/") || medium === "image" || /\.(jpe?g|png|webp|avif|gif)(\?|$)/i.test(url))) push(url);
  }
  push(/<media:thumbnail\b[^>]*url=["']([^"']+)["']/i.exec(block)?.[1]);
  const enclosure = /<enclosure\b[^>]*>/i.exec(block)?.[0];
  if (enclosure) {
    const url = /url=["']([^"']+)["']/i.exec(enclosure)?.[1];
    const type = /type=["']([^"']+)["']/i.exec(enclosure)?.[1] || "";
    if (url && (type.startsWith("image/") || /\.(jpe?g|png|webp|avif|gif)(\?|$)/i.test(url))) push(url);
  }
  push(tag(block, "thumbnail"));
  push(tag(block, "image"));
  for (const field of ["content:encoded", "description", "content", "summary"]) {
    const html = decodeEntities(stripCdata(tag(block, field)));
    push(/<img[^>]+(?:data-src|src)=["']([^"']+)["']/i.exec(html)?.[1]);
  }
  for (const candidate of candidates) {
    const url = normalizeImage(candidate, base);
    if (url) return url;
  }
  return null;
}

function parseDate(raw) {
  if (!raw) return null;
  const t = Date.parse(stripCdata(raw).trim());
  return Number.isFinite(t) ? t : null;
}

function parseFeed(xml, source) {
  const items = [];
  const isAtom = !/<item[\s>]/i.test(xml) && /<entry[\s>]/i.test(xml);
  const re = isAtom ? /<entry[\s>][\s\S]*?<\/entry>/gi : /<item[\s>][\s\S]*?<\/item>/gi;
  for (const m of xml.matchAll(re)) {
    const block = m[0];
    const title = cleanText(tag(block, "title"));
    let link = "";
    if (isAtom) {
      link =
        /<link[^>]*rel=["']alternate["'][^>]*href=["']([^"']+)["']/i.exec(block)?.[1] ||
        /<link[^>]*href=["']([^"']+)["']/i.exec(block)?.[1] ||
        "";
    } else {
      link = stripCdata(tag(block, "link")).trim();
      if (!link) {
        const guid = decodeEntities(stripCdata(tag(block, "guid")).trim());
        if (isHttpUrl(guid) && isSameSite(guid, source)) link = guid;
      }
    }
    link = decodeEntities(link).trim();
    // Items must link to the publisher's own site.
    if (!title || title.length < 8 || !isHttpUrl(link) || !isPublicHttpUrl(link) || !isSameSite(link, source)) continue;
    const description = tag(block, "description") || tag(block, "summary") || tag(block, "content:encoded") || tag(block, "content");
    const published = parseDate(tag(block, "pubDate") || tag(block, "published") || tag(block, "updated") || tag(block, "dc:date"));
    const categories = [...block.matchAll(/<category(?:\s[^>]*)?>([\s\S]*?)<\/category>/gi)].map((c) => cleanText(c[1])).filter(Boolean);
    items.push({
      title,
      link,
      excerpt: snippet(description),
      published,
      image: imageFromItem(block, source.site || link),
      feedCategories: categories.slice(0, 6),
    });
    if (items.length >= ITEMS_PER_SOURCE) break;
  }
  return items;
}

// ───────────────────── language + category ─────────────────────

function detectLanguage(text, fallback) {
  let cyr = 0;
  let lat = 0;
  for (const ch of text) {
    const c = ch.codePointAt(0);
    if (c >= 0x0400 && c <= 0x04ff) cyr++;
    else if ((c >= 0x41 && c <= 0x5a) || (c >= 0x61 && c <= 0x7a) || (c >= 0xc0 && c <= 0x24f)) lat++;
  }
  if (cyr >= lat && cyr > 0) return "mk";
  if (lat > cyr) return fallback === "mk" ? "sq" : fallback || "sq";
  return fallback || "mk";
}

// Order matters: first match wins. Stems are matched against a lower-cased
// "title + feed categories" string (and the excerpt with lower weight).
const CATEGORY_RULES = [
  ["Спорт", /спорт|фудбал|кошарк|ракомет|тенис|одбојк|натпревар|лига на|шампион|репрезентациј|голман|тренер|трансфер|формула 1|олимпи|уефа|фифа|нба\b|вардар|шкендија|sport|futboll|basketboll|ndeshj|kampion|trajner|kombëtar|golash/],
  ["Хроника", /хроник|црна хроника|полициј|уапсен|приведен|убиств|сообраќајна|несреќа|пожар|кражб|грабеж|дрога|обвинителств|притвор|осуден|криминал|мвр\b|судир|загина|повреден|kronik|polici|arrest|vrasj|aksident|zjarr|vjedhj|drog|prokurori|paraburgim/],
  ["Економија", /економ|бизнис|финанс|инфлациј|берза|банка|банки|денар|евро\b|буџет|плат[аи]|пензи|цена|цени|поскап|поевтин|гориво|нафта|струја|бдп|инвестици|извоз|увоз|даноц|ddv|ддв|ekonomi|biznes|financ|inflacion|bank|buxhet|rrog|pension|çmim|investim|tatim/],
  ["Технологија", /технологи|вештачка интелигенција|\bai\b|апликациј|софтвер|смартфон|интернет|сајбер|роботи|гугл|google|apple|microsoft|самсунг|samsung|openai|chatgpt|teknologji|inteligjenc|aplikacion|softuer/],
  ["Култура", /култур|филм|театар|концерт|музик|изложб|фестивал|книга|поезиј|уметн|музеј|опера|балет|евровизи|kultur|film|teatër|koncert|muzik|ekspozit|festival|libër|art\b/],
  ["Здравје", /здравј|здравств|болниц|клиник|лекар|доктор|вакцин|вирус|грип|ковид|пациент|терапиј|шеќер|дијабет|shëndet|spital|mjek|vaksin|virus|pacient/],
  ["Живот", /хороскоп|рецепт|диета|мода|убавина|забав|познат[иа]|естрад|врем[еи]то|временска прогноза|патувањ|horoskop|recet|diet|modë|argëtim|showbiz|moti\b/],
  ["Свет", /свет|трамп|путин|зеленски|русиј|украин|кина\b|сад\b|израел|газа|иран|нато\b|брисел|вашингтон|москва|северна кореја|германиј|франциј|турциј|botë|trump|putin|rusi|ukrain|kinë|izrael|gaza|iran|nato|bruksel|uashington/],
  ["Политика", /политик|влада|собрани|премиер|министер|министерств|претседател|пратени|избори|партиј|вмро|сдсм|дуи\b|коалициј|опозициј|закон|уставн|градоначалник|мицкоски|politik|qeveri|kuvend|kryeministr|ministr|president|deputet|zgjedhj|parti|opozit|ligj|kryetar/],
];

function classify(item, source) {
  if (source.category) return source.category;
  const head = `${item.title} ${item.feedCategories.join(" ")}`.toLowerCase();
  for (const [name, re] of CATEGORY_RULES) if (re.test(head)) return name;
  const body = (item.excerpt || "").toLowerCase();
  for (const [name, re] of CATEGORY_RULES) if (re.test(body)) return name;
  return "Вести";
}

const TRANSLIT = { а: "a", б: "b", в: "v", г: "g", д: "d", ѓ: "gj", е: "e", ж: "zh", з: "z", ѕ: "dz", и: "i", ј: "j", к: "k", л: "l", љ: "lj", м: "m", н: "n", њ: "nj", о: "o", п: "p", р: "r", с: "s", т: "t", ќ: "kj", у: "u", ф: "f", х: "h", ц: "c", ч: "ch", џ: "dzh", ш: "sh", ë: "e", ç: "c" };

function slugify(title, id) {
  const base = title
    .toLowerCase()
    .replace(/[Ѐ-ӿëç]/g, (ch) => TRANSLIT[ch] ?? "")
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 70)
    .replace(/-+$/, "");
  return `${base || "vest"}-${id}`;
}

function canonicalLink(link) {
  try {
    const u = new URL(link);
    u.hash = "";
    for (const key of [...u.searchParams.keys()]) if (/^(utm_|fbclid|gclid|ref$)/i.test(key)) u.searchParams.delete(key);
    return u.toString();
  } catch {
    return link;
  }
}

// ───────────────────────── news ─────────────────────────

const META_IMAGE_PATTERNS = [
  /<meta[^>]{0,300}?(?:property|name)=["']og:image(?::secure_url)?["'][^>]{0,300}?content=["']([^"']{1,2048})["']/i,
  /<meta[^>]{0,300}?content=["']([^"']{1,2048})["'][^>]{0,300}?(?:property|name)=["']og:image(?::secure_url)?["']/i,
  /<meta[^>]{0,300}?(?:property|name)=["']twitter:image(?::src)?["'][^>]{0,300}?content=["']([^"']{1,2048})["']/i,
  /<meta[^>]{0,300}?content=["']([^"']{1,2048})["'][^>]{0,300}?(?:property|name)=["']twitter:image(?::src)?["']/i,
  /<link[^>]{0,300}?rel=["']image_src["'][^>]{0,300}?href=["']([^"']{1,2048})["']/i,
];

async function scrapeImage(article, source) {
  // Only the publisher's own pages are fetched, including after redirects.
  if (!isSameSite(article.url, source)) return null;
  try {
    const html = await fetchText(article.url, {
      timeout: 9000,
      accept: "text/html,application/xhtml+xml",
      maxBytes: PAGE_MAX_BYTES,
      allow: (finalUrl) => isSameSite(finalUrl, source),
    });
    const end = html.search(/<\/head>/i);
    const head = html.slice(0, end > 0 ? Math.min(end, 65536) : 65536);
    for (const re of META_IMAGE_PATTERNS) {
      const image = normalizeImage(re.exec(head)?.[1], article.url);
      if (image && isPublicHttpUrl(image) && !/logo|default|placeholder|favicon/i.test(image)) return image;
    }
  } catch {
    /* ignore: article simply stays without image */
  }
  return null;
}

async function buildNews(sources, previous) {
  const now = Date.now();
  const byId = new Map();
  for (const a of previous) byId.set(a.id, a);

  const stats = [];
  const fresh = [];
  const sourceOf = new Map();

  await pool(sources, 10, async (source) => {
    const stat = { name: source.name, feed: source.feed, ok: false, items: 0, added: 0 };
    stats.push(stat);
    try {
      const xml = await fetchText(source.feed);
      const items = parseFeed(xml, source);
      stat.items = items.length;
      stat.ok = items.length > 0;
      if (!items.length) stat.error = "no items parsed";
      for (const item of items) {
        const url = canonicalLink(item.link);
        const id = sha(url).slice(0, 12);
        if (byId.has(id)) continue;
        // Feeds with wrong timezones publish "future" items: clamp to now.
        const published = Math.min(item.published ?? now, now);
        if (now - published > NEWS_KEEP_DAYS * 86400000) continue;
        const article = {
          id,
          slug: slugify(item.title, id),
          title: item.title,
          excerpt: item.excerpt || "",
          url,
          source: source.name,
          source_url: source.site || null,
          category: classify(item, source),
          lang: detectLanguage(`${item.title} ${item.excerpt}`, source.lang),
          image: item.image,
          published_at: new Date(published).toISOString(),
          fetched_at: new Date(now).toISOString(),
        };
        byId.set(id, article);
        sourceOf.set(id, source);
        fresh.push(article);
        stat.added++;
      }
    } catch (error) {
      stat.error = String(error?.message || error).slice(0, 140);
    }
  });

  // Fill missing images from the article page (og:image), newest first, capped per run.
  const needImage = fresh
    .filter((a) => !a.image)
    .sort((a, b) => b.published_at.localeCompare(a.published_at))
    .slice(0, IMAGE_SCRAPES_PER_RUN);
  await pool(needImage, 8, async (article) => {
    article.image = await scrapeImage(article, sourceOf.get(article.id));
  });

  const cutoff = now - NEWS_KEEP_DAYS * 86400000;
  const articles = [...byId.values()]
    .filter((a) => Date.parse(a.published_at) >= cutoff)
    .sort((a, b) => b.published_at.localeCompare(a.published_at) || a.id.localeCompare(b.id))
    .slice(0, NEWS_MAX);

  stats.sort((a, b) => a.name.localeCompare(b.name));
  return { articles, stats, added: fresh.length };
}

// ───────────────────────── videos ─────────────────────────

function parseYouTubeFeed(xml, channel) {
  const videos = [];
  for (const m of xml.matchAll(/<entry>[\s\S]*?<\/entry>/gi)) {
    const block = m[0];
    const id = tag(block, "yt:videoId");
    const title = cleanText(tag(block, "title"));
    const published = parseDate(tag(block, "published"));
    if (!id || !title || !published) continue;
    const views = Number(/<media:statistics[^>]*views=["'](\d+)["']/i.exec(block)?.[1] || 0);
    videos.push({
      id,
      title,
      description: snippet(tag(block, "media:description"), 180),
      channel: channel.name,
      channel_id: channel.id,
      thumbnail: `https://i.ytimg.com/vi/${id}/hqdefault.jpg`,
      url: `https://www.youtube.com/watch?v=${id}`,
      published_at: new Date(published).toISOString(),
      views,
      news: Boolean(channel.news),
      lang: detectLanguage(title, "mk"),
    });
    if (videos.length >= VIDEOS_PER_CHANNEL) break;
  }
  return videos;
}

async function buildVideos(channels, previous) {
  const now = Date.now();
  const byId = new Map();
  for (const v of previous) byId.set(v.id, v);
  const stats = [];
  let added = 0;

  await pool(channels, 8, async (channel) => {
    const stat = { name: channel.name, id: channel.id, ok: false, items: 0 };
    stats.push(stat);
    try {
      const xml = await fetchText(`https://www.youtube.com/feeds/videos.xml?channel_id=${channel.id}`);
      const videos = parseYouTubeFeed(xml, channel);
      stat.items = videos.length;
      stat.ok = true;
      for (const video of videos) {
        if (!byId.has(video.id)) added++;
        byId.set(video.id, { ...byId.get(video.id), ...video });
      }
    } catch (error) {
      stat.error = String(error?.message || error).slice(0, 140);
    }
  });

  const cutoff = now - VIDEO_KEEP_DAYS * 86400000;
  const videos = [...byId.values()]
    .filter((v) => Date.parse(v.published_at) >= cutoff && Date.parse(v.published_at) <= now + 3600000)
    .sort((a, b) => b.published_at.localeCompare(a.published_at))
    .slice(0, VIDEO_MAX);

  stats.sort((a, b) => a.name.localeCompare(b.name));
  return { videos, stats, added };
}

// ───────────────────────── main ─────────────────────────

const count = (list, key) => {
  const map = {};
  for (const item of list) map[item[key]] = (map[item[key]] || 0) + 1;
  return Object.fromEntries(Object.entries(map).sort((a, b) => b[1] - a[1]));
};

async function main() {
  const started = Date.now();
  const sources = await readJson(path.join(ROOT, "config/sources.json"), []);
  const channels = await readJson(path.join(ROOT, "config/channels.json"), []);
  if (!sources.length || !channels.length) throw new Error("config/sources.json or config/channels.json is empty");

  await mkdir(OUT, { recursive: true });
  const previousNews = (await readJson(path.join(OUT, "news.json"), {})).articles || [];
  const previousVideos = (await readJson(path.join(OUT, "videos.json"), {})).videos || [];

  const [news, video] = await Promise.all([buildNews(sources, previousNews), buildVideos(channels, previousVideos)]);

  const okSources = news.stats.filter((s) => s.ok).length;
  const okChannels = video.stats.filter((s) => s.ok).length;
  // Safety net: never publish an emptier feed than the one already live.
  if (okSources < 5 && previousNews.length) throw new Error(`only ${okSources} news sources responded; keeping previous feed`);

  const generated_at = new Date().toISOString();
  const channelCounts = count(video.videos, "channel_id");
  const write = (name, data) => writeFile(path.join(OUT, name), JSON.stringify(data));

  await write("news.json", { generated_at, count: news.articles.length, articles: news.articles });
  await write("latest.json", { generated_at, count: Math.min(LATEST_COUNT, news.articles.length), articles: news.articles.slice(0, LATEST_COUNT) });
  await write("videos.json", { generated_at, count: video.videos.length, videos: video.videos });
  await write("channels.json", {
    generated_at,
    channels: channels.map((c) => ({ id: c.id, name: c.name, thumbnail: c.thumb || null, news: Boolean(c.news), videos: channelCounts[c.id] || 0 })),
  });
  await write("status.json", {
    generated_at,
    duration_ms: Date.now() - started,
    news: { total: news.articles.length, added: news.added, sources_ok: okSources, sources_total: sources.length, by_category: count(news.articles, "category"), by_lang: count(news.articles, "lang"), with_image: news.articles.filter((a) => a.image).length },
    videos: { total: video.videos.length, added: video.added, channels_ok: okChannels, channels_total: channels.length },
    sources: news.stats,
    channels: video.stats,
  });
  await writeFile(
    path.join(OUT, "index.html"),
    `<!doctype html><meta charset="utf-8"><meta name="robots" content="noindex"><title>ONAIR.MK feed</title><body style="font:16px/1.5 system-ui;margin:40px auto;max-width:640px;padding:0 16px"><h1>ONAIR.MK news feed</h1><p>Updated ${generated_at}. ${news.articles.length} articles from ${okSources}/${sources.length} sources, ${video.videos.length} videos from ${okChannels}/${channels.length} channels.</p><p><a href="latest.json">latest.json</a> · <a href="news.json">news.json</a> · <a href="videos.json">videos.json</a> · <a href="channels.json">channels.json</a> · <a href="status.json">status.json</a></p><p>Headlines and links only; all content belongs to the linked publishers. <a href="https://onair.mk">onair.mk</a></p>`,
  );
  await writeFile(path.join(OUT, ".nojekyll"), "");

  console.log(
    `news: ${news.articles.length} total, +${news.added} new, ${okSources}/${sources.length} sources ok | videos: ${video.videos.length} total, +${video.added} new, ${okChannels}/${channels.length} channels ok | ${Date.now() - started} ms`,
  );
  const failed = news.stats.filter((s) => !s.ok);
  if (failed.length) console.log("failed sources:", failed.map((s) => `${s.name} (${s.error})`).join("; "));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
