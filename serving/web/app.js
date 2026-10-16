/* ShopCart — demo storefront for the recommendation system.
 * Plain JS, no build step. Talks to the FastAPI /api routes (serving/app/routes/shop.py).
 * Every number on screen comes from the batch pipeline (ALS, TF-IDF, session co-occurrence,
 * offline evaluation) or from the live re-ranker — nothing is mocked client-side. */

const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (n) => (n ?? 0).toLocaleString("en-US");
const pct = (v, d = 1) => `${((v ?? 0) * 100).toFixed(d)}%`;
const money = (v) => (v == null ? "—" : `$${Number(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`);

const store = {
  get(key, fallback) {
    try { const v = localStorage.getItem(key); return v === null ? fallback : JSON.parse(v); } catch { return fallback; }
  },
  set(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* private mode */ } },
};

const newGuestId = () => `guest-${Math.random().toString(36).slice(2, 8)}`;

const cartKey = (userId) => `cart:${userId}`;

const state = {
  userId: store.get("userId", null) || newGuestId(),
  cart: [],
  debugOpen: store.get("debugOpen", window.innerWidth > 1100),
  tab: "item",
  demoUsers: [],
  categories: [],
  // what the current page knows, for the debug panel
  view: { kind: "home", context: null, itemDetail: null, recs: null, selectedId: null },
  userDetail: null,
};
store.set("userId", state.userId);
state.cart = store.get(cartKey(state.userId), []);

async function api(path, options = {}) {
  const res = await fetch(path, { headers: { "Content-Type": "application/json" }, ...options });
  if (res.status === 204) return null;
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch { /* not json */ }
    throw new Error(`${res.status}: ${detail}`);
  }
  return res.json();
}

/* ---------------- product visuals ----------------
 * The REES46 dataset has no product titles or images: names are brand + category
 * (built server-side) and the picture is an illustration of the category. */
const ICONS = [
  ["smartphone", "📱"], ["headphone", "🎧"], ["notebook", "💻"], ["tablet", "📲"], ["tv", "📺"], ["clocks", "⌚"],
  ["watch", "⌚"], ["camera", "📷"], ["video", "📹"], ["audio", "🔊"], ["acoustic", "🔊"], ["printer", "🖨️"],
  ["monitor", "🖥️"], ["desktop", "🖥️"], ["mouse", "🖱️"], ["keyboard", "⌨️"], ["refrigerators", "🧊"],
  ["washer", "🫧"], ["vacuum", "🧹"], ["kettle", "🫖"], ["coffee", "☕"], ["microwave", "🍲"], ["oven", "🍲"],
  ["hob", "🍳"], ["blender", "🥤"], ["iron", "👔"], ["hair", "💇"], ["air_conditioner", "❄️"], ["heater", "🔥"],
  ["shoes", "👟"], ["bag", "👜"], ["costume", "👗"], ["jeans", "👖"], ["shirt", "👕"], ["sofa", "🛋️"],
  ["bed", "🛏️"], ["chair", "🪑"], ["table", "🪑"], ["drill", "🪛"], ["saw", "🪚"], ["tools", "🔧"],
  ["toys", "🧸"], ["carriage", "👶"], ["bicycle", "🚲"], ["fitness", "🏋️"], ["tennis", "🎾"], ["ski", "⛷️"],
  ["car", "🚗"], ["tires", "🛞"], ["player", "🎵"], ["telephone", "☎️"], ["light", "💡"], ["bathroom", "🛁"],
  ["kitchen", "🍽️"], ["dishwasher", "🍽️"], ["accessories", "🔌"],
];
const ROOT_ICONS = { electronics: "🔌", appliances: "🏠", computers: "🖥️", apparel: "👕", furniture: "🛋️", construction: "🔧",
  kids: "🧸", auto: "🚗", sport: "⚽", accessories: "👜", country_yard: "🌳", medicine: "💊", stationery: "✏️" };
const TONES = { electronics: "electronics", appliances: "appliances", computers: "computers", apparel: "apparel", accessories: "apparel",
  furniture: "furniture", construction: "furniture", country_yard: "sport", kids: "kids", sport: "sport", auto: "computers" };

function iconFor(item) {
  const code = item?.category_code || "";
  const parts = code.split(".").reverse();
  for (const part of parts) {
    const hit = ICONS.find(([key]) => part.includes(key));
    if (hit) return hit[1];
  }
  return ROOT_ICONS[item?.category_root] || "📦";
}
const LABELS = { clocks: "Watch", tv: "TV", acoustic: "Speaker", hdd: "HDD", ssd: "SSD", cpu: "CPU" };  // mirrors serving/app/services/catalog.py
const humanize = (t) => LABELS[t] || t.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
const toneFor = (item) => `tone-${TONES[item?.category_root] || "other"}`;
const categoryLabel = (item) => (item?.category_path || ["Other"]).join(" › ");
const SOURCE_LABELS = { als: "ALS", content: "TF-IDF", copurchase: "Co-purchase", similar: "ALS neighbour", session: "Session", popular: "Popular", category: "Same category", complement: "Complement" };

/* ---------------- shared UI ---------------- */
function toast(html) {
  const el = $("#toast");
  el.innerHTML = html;
  el.classList.add("show");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => el.classList.remove("show"), 3200);
}

function matchBadge(score) {
  const value = Math.round(score * 100);
  return `<span class="match ${value >= 60 ? "high" : ""}" data-tip="Hybrid re-ranker score: weighted blend of ALS, co-purchase, content, session, category, price, brand and popularity factors">${value}% Match</span>`;
}

function productCard(item, rec = null) {
  const stats = `${fmt(item.views)} views · ${fmt(item.purchases)} bought`;
  return `
    <article class="card" data-item="${item.item_id}">
      ${rec ? matchBadge(rec.score) : ""}
      ${rec ? `<span class="src" data-tip="Candidate source: where this item entered the candidate pool before re-ranking">${esc(SOURCE_LABELS[rec.source] || rec.source)}</span>` : ""}
      <a class="thumb ${toneFor(item)}" href="#/p/${item.item_id}" aria-label="${esc(item.name)}">${iconFor(item)}</a>
      <div class="body">
        <a class="name" href="#/p/${item.item_id}">${esc(item.name)}</a>
        <span class="cat">${esc(categoryLabel(item))}</span>
        <span class="stats">${stats}</span>
        <span class="price">${money(item.price)}</span>
        <div class="actions">
          <button class="btn" data-add="${item.item_id}">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 4h2l2.4 11.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 2-1.5L21 8H6"/></svg>Add
          </button>
        </div>
      </div>
    </article>`;
}

function rail(recs) {
  if (!recs.length) return `<div class="empty">No recommendations — has the pipeline pushed data to Redis?</div>`;
  return `<div class="rail"><div class="rail-track">${recs.map((r) => productCard(r.item, r)).join("")}</div></div>`;
}

function bindCards(root, itemsById) {
  $$("[data-add]", root).forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      const item = itemsById.get(Number(btn.dataset.add));
      if (item) addToCart(item);
    });
  });
  // Hovering a recommended product explains it in the debug panel
  $$(".card", root).forEach((card) => {
    card.addEventListener("mouseenter", () => {
      const id = Number(card.dataset.item);
      if (state.view.recs?.recommendations.some((r) => r.item.item_id === id)) selectRec(id);
    });
  });
}

function selectRec(itemId) {
  if (state.view.selectedId === itemId) return;
  state.view.selectedId = itemId;
  $$(".card.selected").forEach((c) => c.classList.remove("selected"));
  $$(`.card[data-item="${itemId}"]`).forEach((c) => {
    if (c.querySelector(".match")) c.classList.add("selected");
  });
  if (state.tab === "item") renderDebug();
}

/* ---------------- session events ---------------- */
async function sendEvent(itemId, eventType) {
  await api("/api/events", { method: "POST", body: JSON.stringify({ user_id: state.userId, item_id: itemId, event_type: eventType }) });
  state.userDetail = null;
}

async function addToCart(item) {
  if (!state.cart.some((i) => i.item_id === item.item_id)) {
    state.cart.push(item);
    store.set(cartKey(state.userId), state.cart);
  }
  updateCartCount();
  await sendEvent(item.item_id, "cart");
  await refreshRecs(`Added <b>${esc(item.name)}</b> to cart (cart = weight 3)`);
}

async function refreshRecs(message) {
  const before = state.view.recs?.recommendations.map((r) => r.item.item_id) || [];
  await render({ recordView: false });
  const after = state.view.recs?.recommendations.map((r) => r.item.item_id) || [];
  const moved = after.filter((id, i) => before[i] !== id).length;
  const recs = state.view.recs;
  toast(`${message} · re-ranked ${recs ? fmt(recs.candidates_considered) : 0} candidates in <b>${recs?.latency_ms ?? "–"} ms</b>${before.length ? ` · ${moved} positions changed` : ""}`);
}

function updateCartCount() {
  const el = $("#cart-count");
  el.textContent = state.cart.length;
  el.hidden = state.cart.length === 0;
}

/* ---------------- pages ---------------- */
async function pageHome() {
  const [recs, trending] = await Promise.all([
    api(`/api/recommendations/${encodeURIComponent(state.userId)}?limit=12`),
    api(`/api/catalog/items?limit=10`),
  ]);
  state.view = { kind: "home", context: null, itemDetail: null, recs, selectedId: recs.recommendations[0]?.item.item_id ?? null };
  const user = currentUserMeta();
  const main = $("#main");
  main.innerHTML = `
    <section class="hero">
      <div>
        <h1>Recommendations that learn from every click</h1>
        <p>Shopping as <b>${esc(user.label)}</b> — ${esc(recs.strategy_label)}. Open a product, add to cart or buy, and the
          hybrid re-ranker re-scores candidates against your live session. The debug panel shows why each item ranks where it does.</p>
        <div class="steps">
          <span class="step">Kafka → HDFS</span><span class="step">Spark features + time decay</span>
          <span class="step">ALS · TF-IDF · co-purchase</span><span class="step">Redis</span><span class="step">FastAPI re-ranker</span>
        </div>
      </div>
      <button class="btn mint" id="hero-switch">Switch user</button>
    </section>
    <section class="section">
      <div class="section-head"><h2>Recommended for you</h2>
        <span class="hint">${esc(recs.strategy_label)} · ${fmt(recs.candidates_considered)} candidates · ${recs.latency_ms} ms</span></div>
      ${rail(recs.recommendations)}
    </section>
    <section class="section">
      <div class="section-head"><h2>Trending now</h2><span class="hint">Popularity = Σ event weights (view 1 · cart 3 · purchase 5)</span></div>
      <div class="grid">${trending.items.map((i) => productCard(i)).join("")}</div>
    </section>
    <section class="section">
      <div class="section-head"><h2>Shop by category</h2></div>
      <div class="cat-tiles">${state.categories.slice(0, 12).map((c) => `
        <a class="cat-tile ${toneFor({ category_root: c.key })}" href="#/c/${encodeURIComponent(c.key)}">
          <span class="emoji">${ROOT_ICONS[c.key] || "📦"}</span><b>${esc(c.label)}</b><span>${fmt(c.n_items)} products</span>
        </a>`).join("")}</div>
    </section>`;
  $("#hero-switch").addEventListener("click", (e) => { e.stopPropagation(); toggleAccountMenu(true); });
  bindCards(main, new Map([...recs.recommendations.map((r) => [r.item.item_id, r.item]), ...trending.items.map((i) => [i.item_id, i])]));
  selectRecHighlight();
}

async function pageListing(category, query, limit = 48) {
  const params = new URLSearchParams({ limit });
  if (category) params.set("category", category);
  if (query) params.set("q", query);
  const data = await api(`/api/catalog/items?${params}`);
  state.view = { ...state.view, kind: "listing", context: null, itemDetail: null };
  const title = query ? `Results for “${esc(query)}”` : esc(state.categories.find((c) => c.key === category)?.label || "All products");
  const main = $("#main");
  main.innerHTML = `
    <h1 class="page-title">${title} <span class="muted" style="font-size:14px;font-weight:500">${fmt(data.total)} products · sorted by popularity</span></h1>
    ${data.items.length ? `<div class="grid">${data.items.map((i) => productCard(i)).join("")}</div>` : `<div class="section empty">Nothing matches. Try a brand (samsung, apple) or a category (smartphone, headphone).</div>`}
    ${data.total > data.items.length ? `<p style="text-align:center"><button class="btn" id="more">Load more</button></p>` : ""}`;
  bindCards(main, new Map(data.items.map((i) => [i.item_id, i])));
  $("#more")?.addEventListener("click", () => pageListing(category, query, limit + 48));
  renderDebug();
}

async function pageProduct(itemId, recordView) {
  if (recordView) await sendEvent(itemId, "view").catch(() => {});
  const [detail, recs] = await Promise.all([
    api(`/api/items/${itemId}`),
    api(`/api/recommendations/${encodeURIComponent(state.userId)}?context_item=${itemId}&limit=12`),
  ]);
  state.view = { kind: "product", context: detail, itemDetail: detail, recs, selectedId: recs.recommendations[0]?.item.item_id ?? null };
  const conv = detail.views ? (detail.carts + detail.purchases) / detail.views : 0;
  const tomorrow = new Date(Date.now() + 864e5).toLocaleDateString("en-US", { weekday: "short", day: "numeric", month: "short" });
  const main = $("#main");
  main.innerHTML = `
    <section class="section">
      <div class="pdp">
        <div class="pdp-image ${toneFor(detail)}">${iconFor(detail)}<span class="sku">SKU ${detail.item_id}</span></div>
        <div>
          <div class="crumbs">${detail.category_path.map((p, i) => i === 0
            ? `<a href="#/c/${encodeURIComponent(detail.category_root)}">${esc(p)}</a>` : esc(p)).join(" › ")}</div>
          <h1>${esc(detail.name)}</h1>
          <div class="muted">${fmt(detail.views)} views · ${fmt(detail.carts)} add-to-carts · ${fmt(detail.purchases)} purchases · ${fmt(detail.unique_users)} shoppers</div>
          <div class="price-row"><b>${money(detail.price)}</b>
            <span class="pill" data-tip="(carts + purchases) / views for this item in the event stream">${pct(conv, 0)} conversion</span></div>
          <div class="muted">Average price across the event stream (USD)</div>
          <div class="facts">
            <div class="fact"><b>${detail.popularity_score ?? "—"}</b><span>Popularity score</span></div>
            <div class="fact"><b>${detail.conversion_rate != null ? pct(detail.conversion_rate) : "—"}</b><span>Conversion (pipeline)</span></div>
            <div class="fact"><b>${detail.embedding_dims || "—"}</b><span>ALS embedding dims</span></div>
            <div class="fact"><b>${detail.also.length}</b><span>Co-session items</span></div>
          </div>
          <div class="buy-row">
            <button class="btn primary" id="add-cart"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 4h2l2.4 11.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 2-1.5L21 8H6"/></svg>Add to Cart</button>
            <button class="btn light" id="buy-now">Buy Now</button>
          </div>
          <div class="perks">
            <div><svg viewBox="0 0 24 24"><path d="M3 7h11v9H3zM14 10h4l3 3v3h-7"/><circle cx="7" cy="18" r="1.6"/><circle cx="17" cy="18" r="1.6"/></svg>
              <span>FREE delivery by ${tomorrow}<br><span class="muted">Demo store — nothing ships</span></span></div>
            <div><svg viewBox="0 0 24 24"><path d="M12 3 4 6v6c0 5 3.5 8 8 9 4.5-1 8-4 8-9V6z"/><path d="m9 12 2 2 4-4"/></svg>
              <span>Every click here is a session event<br><span class="muted">view = 1 · cart = 3 · purchase = 5, newest weighted most</span></span></div>
          </div>
          <p class="note">Product names and pictures are generated from brand + category: the REES46 event dataset has no titles or photos.</p>
        </div>
      </div>
    </section>
    <section class="section">
      <div class="section-head"><h2>Customers also bought</h2>
        <span class="hint">Alternatives + complements · ${fmt(recs.candidates_considered)} candidates re-ranked in ${recs.latency_ms} ms · hover a card to see why</span></div>
      ${rail(recs.recommendations)}
    </section>`;
  $("#add-cart").addEventListener("click", () => addToCart(detail));
  $("#buy-now").addEventListener("click", async () => {
    await sendEvent(detail.item_id, "purchase");
    await refreshRecs(`Bought <b>${esc(detail.name)}</b> (purchase = weight 5)`);
  });
  bindCards(main, new Map(recs.recommendations.map((r) => [r.item.item_id, r.item])));
  selectRecHighlight();
}

async function pageCart() {
  state.view = { ...state.view, kind: "cart", context: null, itemDetail: null };
  const total = state.cart.reduce((s, i) => s + (i.price || 0), 0);
  const main = $("#main");
  main.innerHTML = `
    <h1 class="page-title">Your cart</h1>
    <section class="section">
      ${state.cart.length ? state.cart.map((i) => `
        <div class="cart-row">
          <a class="thumb ${toneFor(i)}" href="#/p/${i.item_id}">${iconFor(i)}</a>
          <div><a href="#/p/${i.item_id}"><b>${esc(i.name)}</b></a><div class="muted">${esc(categoryLabel(i))}</div></div>
          <b>${money(i.price)}</b>
          <button class="btn" data-remove="${i.item_id}">Remove</button>
        </div>`).join("") + `
        <div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px">
          <b style="font-size:18px">Total ${money(total)}</b>
          <button class="btn primary" id="checkout">Checkout</button>
        </div>
        <p class="note">Checkout logs a purchase event per item; Remove logs remove_from_cart (weight 0).</p>`
      : `<div class="empty">Your cart is empty. <a href="#/" style="text-decoration:underline">Keep shopping</a></div>`}
    </section>`;
  $$("[data-remove]", main).forEach((btn) => btn.addEventListener("click", async () => {
    const id = Number(btn.dataset.remove);
    state.cart = state.cart.filter((i) => i.item_id !== id);
    store.set(cartKey(state.userId), state.cart);
    updateCartCount();
    await sendEvent(id, "remove_from_cart");
    pageCart();
    toast("Removed from cart (remove_from_cart = weight 0)");
  }));
  $("#checkout")?.addEventListener("click", async () => {
    for (const item of state.cart) await sendEvent(item.item_id, "purchase");
    const n = state.cart.length;
    state.cart = [];
    store.set(cartKey(state.userId), state.cart);
    updateCartCount();
    toast(`Order placed (demo): logged <b>${n}</b> purchase events to your session`);
    location.hash = "#/";
  });
  renderDebug();
}

/* ---------------- insights ---------------- */
const MODEL_META = {
  als: { label: "ALS (collaborative)", color: "var(--s-als)" },
  popularity: { label: "Popularity baseline", color: "var(--s-pop)" },
  content_tfidf: { label: "TF-IDF content-based", color: "var(--s-tfidf)" },
};
const METRICS = [
  ["hit_rate", "Hit rate", "Share of users with at least one held-out item in their top-K"],
  ["recall", "Recall", "Held-out items found in the top-K ÷ all held-out items, averaged per user"],
  ["precision", "Precision", "Held-out items found in the top-K ÷ K, averaged per user"],
  ["coverage", "Catalog coverage", "Distinct items recommended ÷ catalog size"],
];

function evalChart(rows, metric, k) {
  const segments = [["all", "All evaluable users"], ["cold_start", "Cold-start users (≤ 3 events)"]];
  const filtered = rows.filter((r) => r.k === k);
  const max = Math.max(...filtered.map((r) => r[metric]), 0.0001);
  return segments.map(([seg, label]) => {
    const segRows = filtered.filter((r) => r.segment === seg);
    if (!segRows.length) return "";
    return `<div style="margin-bottom:12px"><div class="muted" style="font-size:12px;margin-bottom:6px">${label} · n = ${fmt(segRows[0].n_users)}</div>
      <div class="bar-list">${segRows.map((r) => `
        <div class="row" data-tip="${esc(MODEL_META[r.model]?.label)} · ${label}<br>${metric.replace("_", " ")}@${k} = ${pct(r[metric], 2)}">
          <span>${esc(MODEL_META[r.model]?.label || r.model)}</span>
          <div class="track"><div class="fill" style="width:${(r[metric] / max) * 100}%;background:${MODEL_META[r.model]?.color}"></div></div>
          <span class="val">${pct(r[metric])}</span>
        </div>`).join("")}</div></div>`;
  }).join("");
}

function barList(rows, color = "var(--ink-2)") {
  const max = Math.max(...rows.map((r) => r.value), 1e-9);
  return `<div class="bar-list">${rows.map((r) => `
    <div class="row" data-tip="${esc(r.tip || `${r.label}: ${r.display}`)}"><span>${esc(r.label)}</span>
      <div class="track"><div class="fill" style="width:${(r.value / max) * 100}%;background:${color}"></div></div>
      <span class="val">${r.display}</span></div>`).join("")}</div>`;
}

function columnChart(points) {
  if (!points.length) return "";
  const w = 640, h = 180, pad = { l: 44, r: 8, t: 10, b: 26 };
  const max = Math.max(...points.map((p) => p.value));
  const step = (w - pad.l - pad.r) / points.length;
  const bw = Math.max(4, Math.min(48, step - 2));
  const y = (v) => pad.t + (h - pad.t - pad.b) * (1 - v / max);
  const ticks = [0, max / 2, max];
  const labelEvery = Math.ceil(points.length / 8);
  return `<svg class="chart" viewBox="0 0 ${w} ${h}" role="img" aria-label="Events per hour">
    ${ticks.map((t) => `<line x1="${pad.l}" x2="${w - pad.r}" y1="${y(t)}" y2="${y(t)}" stroke="#e4e6ea" stroke-width="1"/>
      <text x="${pad.l - 6}" y="${y(t) + 4}" text-anchor="end" font-size="11" fill="#80858f">${fmt(Math.round(t))}</text>`).join("")}
    ${points.map((p, i) => {
      const x = pad.l + i * step + (step - bw) / 2;
      const top = y(p.value);
      return `<g data-tip="${esc(p.label)}<br>${fmt(p.value)} events">
        <rect x="${pad.l + i * step}" y="${pad.t}" width="${step}" height="${h - pad.t - pad.b}" fill="transparent"/>
        <path d="M${x},${h - pad.b} V${top + 4} q0,-4 4,-4 h${bw - 8} q4,0 4,4 V${h - pad.b} Z" fill="var(--s-als)"/>
        ${i % labelEvery === 0 ? `<text x="${x + bw / 2}" y="${h - 8}" text-anchor="middle" font-size="11" fill="#80858f">${esc(p.short)}</text>` : ""}
      </g>`;
    }).join("")}
  </svg>`;
}

async function pageInsights() {
  const { pipeline: p, evaluation, ranking_profiles: profiles } = await api("/api/metrics");
  state.view = { ...state.view, kind: "insights", context: null, itemDetail: null };
  const main = $("#main");
  if (!p) {
    main.innerHTML = `<section class="section empty">No pipeline metrics in Redis yet. Run <code>python run.py</code> (or the
      nightly_retrain DAG) so export_serving_features.py can publish them.</section>`;
    return;
  }
  const k = state.evalK || 10;
  const et = p.event_types || {};
  main.innerHTML = `
    <h1 class="page-title">How the recommendations are built
      <span class="muted" style="font-size:13px;font-weight:500">pipeline run ${esc(p.generated_at.replace("T", " "))} UTC</span></h1>
    <div class="kpis" style="margin-bottom:20px">
      <div class="kpi"><b>${fmt(p.events)}</b><span>Events ingested (Kafka → HDFS)</span></div>
      <div class="kpi"><b>${fmt(p.users)}</b><span>Users</span></div>
      <div class="kpi"><b>${fmt(p.items)}</b><span>Products</span></div>
      <div class="kpi"><b>${fmt(p.sessions)}</b><span>Sessions</span></div>
      <div class="kpi"><b>${fmt(p.interaction_pairs)}</b><span>(user, item) pairs</span></div>
      <div class="kpi"><b>${pct(p.matrix_density, 3)}</b><span>Matrix density</span></div>
      <div class="kpi"><b>${pct(p.cold_start_users / Math.max(p.users, 1))}</b><span>Cold-start users (≤ ${p.cold_start_threshold} events)</span></div>
      <div class="kpi"><b>${pct(p.recommended_catalog_coverage)}</b><span>Catalog reached by top-${p.config.top_n}</span></div>
    </div>
    <section class="section">
      <div class="section-head"><h2>Pipeline</h2><span class="hint">Airflow DAG nightly_retrain · same order as run.py</span></div>
      <div class="flow">
        ${[
          ["Kafka", `${fmt(p.events)} events`], ["HDFS", "year/month/day parquet"],
          ["Feature engineering", `weights ${Object.entries(p.config.event_weights).map(([k2, v]) => `${k2.replace("remove_from_cart", "remove")} ${v}`).join(" · ")}, ${p.config.time_decay_half_life_days}-day half-life`],
          ["ALS", `rank ${p.config.als.rank} · ${p.config.als.maxIter} iters · λ ${p.config.als.regParam}`],
          ["TF-IDF cold-start", `${fmt(p.users_with_recs.content || 0)} users · pool ${fmt(p.config.content_candidate_pool)}`],
          ["Evaluation", "80/20 hold-out"],
          ["Redis", `${fmt((p.users_with_recs.als || 0) + (p.users_with_recs.content || 0))} users · ${fmt(p.items)} items`],
          ["FastAPI re-ranker", `${profiles.product.length} factors + live session`],
        ].map(([t, s], i) => `${i ? '<span class="arrow">→</span>' : ""}<div class="stage"><b>${t}</b><span>${esc(s)}</span></div>`).join("")}
      </div>
    </section>
    <section class="section">
      <div class="section-head"><h2>Offline evaluation</h2>
        <span class="hint">K = <button class="btn" data-k="10" ${k === 10 ? 'style="border-color:#111"' : ""}>10</button>
          <button class="btn" data-k="20" ${k === 20 ? 'style="border-color:#111"' : ""}>20</button></span></div>
      ${evaluation.length ? `
        <div class="legend">${Object.values(MODEL_META).map((m) => `<span><i style="background:${m.color}"></i>${m.label}</span>`).join("")}</div>
        <div class="two-col">${METRICS.map(([key, label, tip]) => `
          <div><h3 style="margin:4px 0 2px;font-size:15px" data-tip="${esc(tip)}">${label}@${k}</h3>
          <div class="muted" style="font-size:12px;margin-bottom:10px">${esc(tip)}</div>${evalChart(evaluation, key, k)}</div>`).join("")}</div>
        <details class="table-view"><summary>Table view</summary>
          <table class="data"><thead><tr><th>Segment</th><th>Model</th><th class="num">K</th><th class="num">Users</th>
            <th class="num">Precision</th><th class="num">Recall</th><th class="num">Hit rate</th><th class="num">Coverage</th></tr></thead>
          <tbody>${evaluation.map((r) => `<tr><td>${esc(r.segment)}</td><td>${esc(MODEL_META[r.model]?.label || r.model)}</td>
            <td class="num">${r.k}</td><td class="num">${fmt(r.n_users)}</td><td class="num">${pct(r.precision, 2)}</td>
            <td class="num">${pct(r.recall, 2)}</td><td class="num">${pct(r.hit_rate, 2)}</td><td class="num">${pct(r.coverage, 2)}</td></tr>`).join("")}</tbody></table>
          <p class="note">Evaluated ${esc(evaluation[0].evaluated_at)}. Seen-in-train items are never counted as recommendations.</p>
        </details>`
      : `<div class="empty">evaluate_model.py hasn't written metrics yet.</div>`}
    </section>
    <div class="two-col">
      <section class="section">
        <div class="section-head"><h2>Event mix</h2><span class="hint">${esc(p.date_range[0])} → ${esc(p.date_range[1])}</span></div>
        ${barList(Object.entries(et).sort((a, b) => b[1] - a[1]).map(([key, v]) => ({
          label: key, value: v, display: pct(v / p.events), tip: `${key}: ${fmt(v)} events (${pct(v / p.events)})` })), "var(--s-als)")}
      </section>
      <section class="section">
        <div class="section-head"><h2>Top categories</h2><span class="hint">by events</span></div>
        ${barList(p.top_categories.map((c) => ({ label: c.root, value: c.events, display: fmt(c.events),
          tip: `${c.root}: ${fmt(c.events)} events, ${fmt(c.purchases)} purchases` })), "var(--s-als)")}
      </section>
    </div>
    <section class="section">
      <div class="section-head"><h2>Events per hour</h2><span class="hint">event-time, UTC</span></div>
      ${columnChart(p.events_per_hour.map((r) => ({ label: r.hour, short: r.hour.slice(5).replace(" ", " "), value: r.events })))}
    </section>
    <section class="section">
      <div class="section-head"><h2>Hybrid re-ranker weights</h2><span class="hint">serving/app/config.py · final score = Σ weight × factor</span></div>
      <div class="two-col">
        <div><h3 style="margin:0 0 10px;font-size:15px">Home feed — about the shopper</h3>
          ${barList(profiles.home.map((f) => ({ label: f.label, value: f.weight, display: pct(f.weight, 0) })), "var(--s-tfidf)")}</div>
        <div><h3 style="margin:0 0 10px;font-size:15px">Product page — about the product on screen</h3>
          ${barList(profiles.product.map((f) => ({ label: f.label, value: f.weight, display: pct(f.weight, 0) })), "var(--s-tfidf)")}</div>
      </div>
    </section>`;
  $$("[data-k]", main).forEach((b) => b.addEventListener("click", () => { state.evalK = Number(b.dataset.k); pageInsights(); }));
  renderDebug();
}

/* ---------------- debug panel ---------------- */
function dItem(item, extra = "") {
  return `<div class="d-item"><div class="thumb ${toneFor(item)}">${iconFor(item)}</div>
    <div><b>${esc(item.name)}</b><span>SKU ${item.item_id}</span><span>Category: ${esc(categoryLabel(item))}</span></div>${extra}</div>`;
}

function dList(neighbours, valueFn, subFn) {
  if (!neighbours.length) return `<p class="d-note">None — this item has no neighbours in the current data slice.</p>`;
  return `<div class="d-list">${neighbours.map((n) => `
    <a href="#/p/${n.item.item_id}"><span class="mini ${toneFor(n.item)}">${iconFor(n.item)}</span>
      <span>${esc(n.item.name)}<span class="sub">${esc(subFn ? subFn(n) : categoryLabel(n.item))}</span></span>
      <span class="pct">${valueFn(n)}</span></a>`).join("")}</div>`;
}

function complementList(complements) {
  if (!complements?.length) return `<p class="d-note">No complementary categories for this product.</p>`;
  return `<div class="d-bars">${complements.map((c) => `
    <div class="row" data-tip="${c.kind === "learned"
      ? `${fmt(c.users)} shoppers engaged with both categories (cosine, shrunk for small samples)`
      : "Too little shared-shopper signal in this data slice: filled in from the same department"}">
      <span>${esc(categoryLabel({ category_path: c.category.split(".").map(humanize) }).split(" › ").slice(-1)[0])}${c.kind === "learned" ? "" : " · dept"}</span>
      <div class="track"><div class="fill" style="width:${c.value * 100}%;${c.kind === "learned" ? "" : "opacity:.45"}"></div></div>
      <span class="val">${c.kind === "learned" ? fmt(c.users) : "—"}</span></div>`).join("")}</div>`;
}

function debugItemTab() {
  const { recs, context, selectedId, kind } = state.view;
  if (!recs || !["home", "product"].includes(kind)) {
    return `<p class="d-note">Open the home page or a product to see how its recommendations were scored.</p>`;
  }
  const rec = recs.recommendations.find((r) => r.item.item_id === selectedId) || recs.recommendations[0];
  let html = "";
  html += `<div class="d-label">${context ? "Selected Item" : "Context"}</div>`;
  html += context ? `<div class="d-card">${dItem(context)}</div>`
    : `<div class="d-strategy">${esc(recs.strategy_label)}. No product open, so co-purchase anchors come from the live session only.</div>`;
  if (!rec) return html + `<p class="d-note">No candidates were found.</p>`;

  const factors = rec.factors;
  html += `<div class="d-label">Recommendation Details</div>
    <div class="d-card">
      ${dItem(rec.item, `<div class="d-score"><small>Match Score</small><b>${Math.round(rec.score * 100)}%</b></div>`)}
      <div class="d-rows">
        ${factors.map((f) => `<div class="d-row" data-tip="weight ${pct(f.weight, 0)} → adds ${(f.weight * f.value).toFixed(3)} to the final score${f.key === "als" && rec.als_prediction != null ? `<br>raw ALS prediction ${rec.als_prediction} (clipped to 0–1)` : ""}">
          <span>${esc(f.label)}<span class="w">×${f.weight}</span></span><b>${f.value.toFixed(3)}</b></div>`).join("")}
        <div class="d-row final"><span>Final Score</span><b>${rec.score.toFixed(3)}</b></div>
        ${rec.variety && rec.variety.multiplier < 1 ? `<div class="d-row" data-tip="Variety pass: each earlier pick from the same category multiplies the ranking score by ${context ? 0.6 : 0.75}. Match % stays the raw score.">
          <span>Variety adjustment<span class="w">${rec.variety.same_category_before} same-category item${rec.variety.same_category_before === 1 ? "" : "s"} ranked above</span></span>
          <b>×${rec.variety.multiplier} → ${rec.variety.rank_score.toFixed(3)}</b></div>` : ""}
      </div>
      <p class="d-note">Candidate source: <b>${esc(SOURCE_LABELS[rec.source] || rec.source)}</b> · blended relevance, not a calibrated probability.</p>
    </div>`;

  const sorted = [...factors].sort((a, b) => b.contribution - a.contribution).filter((f) => f.contribution > 0);
  const max = Math.max(...sorted.map((f) => f.contribution), 1e-9);
  html += `<div class="d-label">Top Contributing Factors</div>
    <div class="d-bars">${sorted.map((f) => `<div class="row" data-tip="${esc(f.label)}: ${pct(f.contribution)} of the final score">
      <span>${esc(f.label)}</span><div class="track"><div class="fill" style="width:${(f.contribution / max) * 100}%"></div></div>
      <span class="val">${pct(f.contribution, 0)}</span></div>`).join("")}</div>`;

  if (context) {
    html += `<div class="d-label">Complementary categories · shared shoppers</div>${complementList(recs.complements)}`;
    html += `<div class="d-label">Similar Items (Top 5) · ALS embedding cosine</div>`;
    html += dList((state.view.itemDetail?.similar || []).slice(0, 5), (n) => pct(n.score, 0));
  }
  html += `<p class="d-note">${fmt(recs.candidates_considered)} candidates · ${recs.session_events} session events · served in ${recs.latency_ms} ms</p>`;
  return html;
}

async function debugUserTab() {
  if (!state.userDetail) state.userDetail = await api(`/api/users/${encodeURIComponent(state.userId)}`);
  const u = state.userDetail;
  const p = u.profile;
  const segment = p ? p.segment : "guest";
  let html = `<div class="d-label">User</div>
    <div class="d-card"><div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
      <b>${esc(u.user_id)}</b><span class="seg ${segment}">${segment === "cold_start" ? "cold-start" : segment === "guest" ? "new visitor" : "warm"}</span></div>
      <div class="d-strategy" style="margin-top:10px">${esc(u.strategy_label)}${u.precomputed_count ? ` · ${u.precomputed_count} precomputed recs in Redis` : ""}</div>
    </div>`;
  if (p) {
    html += `<div class="d-label">Batch history (HDFS)</div>
      <div class="d-kv">
        <div><b>${fmt(p.n_events)}</b><span>events</span></div><div><b>${fmt(p.n_sessions)}</b><span>sessions</span></div>
        <div><b>${fmt(p.views)} / ${fmt(p.carts)} / ${fmt(p.purchases)}</b><span>views / carts / purchases</span></div>
        <div><b>${money(p.avg_price)}</b><span>avg price → price fit</span></div>
      </div>
      <p class="d-note">${esc(p.first_seen)} → ${esc(p.last_seen)} · ALS user vector: ${p.embedding_dims || "none"} dims</p>`;
    const shares = (rows) => `<div class="d-bars">${(rows || []).map((r) => `<div class="row"><span>${esc(r.key)}</span>
      <div class="track"><div class="fill" style="width:${r.share * 100}%"></div></div><span class="val">${pct(r.share, 0)}</span></div>`).join("") || '<p class="d-note">—</p>'}</div>`;
    html += `<div class="d-label">Category affinity (weighted share)</div>${shares(p.categories)}`;
    html += `<div class="d-label">Brand affinity</div>${shares(p.brands)}`;
    const tokens = Object.entries(p.profile_tokens || {}).sort((a, b) => b[1] - a[1]).slice(0, 14);
    html += `<div class="d-label">TF-IDF profile (content similarity)</div>
      <div class="d-chips">${tokens.map(([t, w]) => `<span class="d-chip">${esc(t)}<b>${w.toFixed(2)}</b></span>`).join("") || '<span class="d-note">—</span>'}</div>`;
    html += `<div class="d-label">Recent history</div>${dList(p.recent || [], (n) => esc(n.event_type), (n) => n.at)}`;
  } else {
    html += `<p class="d-note">Not in the batch data: no ALS vector or history, so the re-ranker leans on popularity and the live session — the cold-start path.</p>`;
  }
  html += `<div class="d-label" style="display:flex;justify-content:space-between;align-items:center">Live session (Redis, 2h TTL)
    <button class="btn" id="reset-session">Reset</button></div>
    ${u.session.length ? dList(u.session, (n) => esc(n.event_type), (n) => n.at.replace("T", " ").slice(0, 19))
      : '<p class="d-note">No clicks yet. View a product, add to cart or buy — each event re-ranks the next recommendations.</p>'}`;
  return html;
}

function debugSimilarTab() {
  const detail = state.view.itemDetail;
  const rec = state.view.recs?.recommendations.find((r) => r.item.item_id === state.view.selectedId);
  if (!detail) {
    return `<p class="d-note">Open a product to see its neighbours. ${rec ? `Currently hovered: ${esc(rec.item.name)} — <a href="#/p/${rec.item.item_id}" style="color:var(--d-accent)">open it</a>.` : ""}</p>`;
  }
  const tokens = Object.entries(detail.tokens || {}).sort((a, b) => b[1] - a[1]);
  return `<div class="d-card">${dItem(detail)}</div>
    <div class="d-label">ALS embedding neighbours (cosine)</div>
    ${dList(detail.similar, (n) => pct(n.score, 0))}
    <p class="d-note">Nearest items in the ${detail.embedding_dims || 20}-dim latent space learned by ALS. Users who interact with one tend to interact with the other.</p>
    <div class="d-label">Bought / viewed in the same session</div>
    ${dList(detail.also, (n) => pct(n.score, 0), (n) => `${fmt(n.sessions)} shared session${n.sessions === 1 ? "" : "s"}`)}
    <p class="d-note">Session co-occurrence, each pair weighted by the weaker interaction (view 1 · cart 3 · purchase 5), cosine-normalised.</p>
    <div class="d-label">Complementary categories</div>
    ${complementList(detail.complements)}
    <p class="d-note">Categories this product's shoppers also engaged with (≥ 2 shared shoppers). These supply the headphones-for-a-phone part of “Customers also bought”.</p>
    <div class="d-label">TF-IDF tokens</div>
    <div class="d-chips">${tokens.map(([t, w]) => `<span class="d-chip">${esc(t)}<b>${w.toFixed(2)}</b></span>`).join("")}</div>`;
}

async function renderDebug() {
  const body = $("#debug-body");
  $$(".tabs button").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.tab === state.tab)));
  try {
    body.innerHTML = state.tab === "item" ? debugItemTab() : state.tab === "user" ? await debugUserTab() : debugSimilarTab();
  } catch (err) {
    body.innerHTML = `<p class="d-note">Couldn't load: ${esc(err.message)}</p>`;
  }
  $("#reset-session")?.addEventListener("click", async () => {
    await api(`/api/session/${encodeURIComponent(state.userId)}`, { method: "DELETE" });
    state.userDetail = null;
    await refreshRecs("Session cleared");
  });
}

function selectRecHighlight() {
  const id = state.view.selectedId;
  $$(`.card[data-item="${id}"]`).forEach((c) => { if (c.querySelector(".match")) c.classList.add("selected"); });
  renderDebug();
}

/* ---------------- account / users ---------------- */
function currentUserMeta() {
  const demo = state.demoUsers.find((u) => String(u.user_id) === state.userId);
  if (state.userId.startsWith("guest-")) return { label: "a new visitor", short: "Guest", segment: "guest" };
  if (demo) return { label: `user ${demo.user_id}`, short: `${demo.user_id}`, segment: demo.segment };
  return { label: `user ${state.userId}`, short: state.userId, segment: "warm" };
}

function renderAccountMenu() {
  const users = state.demoUsers;
  const opt = (u) => `<button class="user-opt ${String(u.user_id) === state.userId ? "current" : ""}" data-user="${u.user_id}">
    <span class="seg ${u.segment}">${u.segment === "cold_start" ? "cold" : "warm"}</span>
    <span><b>${u.user_id}</b><span class="meta"> · ${fmt(u.n_events)} events · ${fmt(u.purchases)} buys${u.top_category ? ` · ${esc(u.top_category)}` : ""}</span></span></button>`;
  $("#account-menu").innerHTML = `
    <h4>Try as a returning shopper (ALS)</h4>${users.filter((u) => u.source === "als").map(opt).join("") || '<p class="muted" style="padding:0 10px">No demo users yet — run the pipeline.</p>'}
    <h4>Try as a cold-start shopper (TF-IDF, ≤ 3 events)</h4>${users.filter((u) => u.source === "content").map(opt).join("")}
    <h4>Or</h4>
    <button class="user-opt" data-user="__guest"><span class="seg guest">new</span><span><b>New visitor</b><span class="meta"> · no history, popularity + live session</span></span></button>
    <form id="user-form"><input name="uid" placeholder="Any user_id from the dataset" inputmode="numeric"><button class="btn">Go</button></form>`;
  $$("[data-user]").forEach((b) => b.addEventListener("click", () => switchUser(b.dataset.user === "__guest" ? newGuestId() : b.dataset.user)));
  $("#user-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const uid = new FormData(e.target).get("uid").trim();
    if (uid) switchUser(uid);
  });
}

function toggleAccountMenu(open) {
  const menu = $("#account-menu");
  const show = open ?? menu.hidden;
  if (show) renderAccountMenu();
  menu.hidden = !show;
  $("#account-button").setAttribute("aria-expanded", String(show));
}

function switchUser(userId) {
  state.userId = String(userId);
  store.set("userId", state.userId);
  state.cart = store.get(cartKey(state.userId), []);
  updateCartCount();
  state.userDetail = null;
  toggleAccountMenu(false);
  updateAccountLabel();
  render({ recordView: false });
  toast(`Now shopping as <b>${esc(currentUserMeta().label)}</b>`);
}

function updateAccountLabel() { $("#account-label").textContent = currentUserMeta().short; }

/* ---------------- router ---------------- */
async function render({ recordView = true } = {}) {
  const hash = location.hash.replace(/^#/, "") || "/";
  const [, page, arg] = hash.split("/");
  $$("#catnav a").forEach((a) => a.classList.toggle("active", a.getAttribute("href") === `#${hash}`));
  try {
    if (page === "p" && arg) await pageProduct(Number(arg), recordView);
    else if (page === "c") await pageListing(decodeURIComponent(arg || ""), null);
    else if (page === "search") await pageListing(null, decodeURIComponent(arg || ""));
    else if (page === "cart") await pageCart();
    else if (page === "insights") await pageInsights();
    else await pageHome();
  } catch (err) {
    $("#main").innerHTML = `<section class="section empty"><b>Couldn't load this page.</b><br>${esc(err.message)}<br><br>
      Is the stack up and has the pipeline run? Try <code>python run.py status</code>.</section>`;
    renderDebug();
  }
}

async function checkHealth() {
  const badge = $("#live-badge");
  try {
    const h = await api("/health");
    const ok = h.redis && h.features_loaded;
    badge.classList.toggle("down", !ok);
    $("#live-text").textContent = ok ? "Live" : h.redis ? "No data" : "Redis down";
  } catch {
    badge.classList.add("down");
    $("#live-text").textContent = "Offline";
  }
}

function setDebugOpen(open) {
  state.debugOpen = open;
  store.set("debugOpen", open);
  document.body.classList.toggle("debug-open", open);
}

function initTooltips() {
  const tip = $("#tooltip");
  document.addEventListener("mouseover", (e) => {
    const el = e.target.closest("[data-tip]");
    if (!el) { tip.classList.remove("show"); return; }
    tip.innerHTML = el.dataset.tip;
    tip.classList.add("show");
  });
  document.addEventListener("mousemove", (e) => {
    if (!tip.classList.contains("show")) return;
    const x = Math.min(e.clientX + 14, window.innerWidth - tip.offsetWidth - 8);
    const y = e.clientY + 18 + tip.offsetHeight > window.innerHeight ? e.clientY - tip.offsetHeight - 10 : e.clientY + 18;
    tip.style.left = `${x}px`;
    tip.style.top = `${y}px`;
  });
}

async function init() {
  setDebugOpen(state.debugOpen);
  updateCartCount();
  initTooltips();
  checkHealth();
  setInterval(checkHealth, 15000);

  $("#debug-toggle").addEventListener("click", () => setDebugOpen(!state.debugOpen));
  $("#debug-close").addEventListener("click", () => setDebugOpen(false));
  $$(".tabs button").forEach((b) => b.addEventListener("click", () => { state.tab = b.dataset.tab; renderDebug(); }));
  $("#account-button").addEventListener("click", (e) => { e.stopPropagation(); toggleAccountMenu(); });
  document.addEventListener("click", (e) => { if (!e.target.closest(".account")) toggleAccountMenu(false); });
  $("#search-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const q = $("#search-input").value.trim();
    location.hash = q ? `#/search/${encodeURIComponent(q)}` : "#/";
  });
  window.addEventListener("hashchange", () => { window.scrollTo(0, 0); render(); });

  const [categories, demoUsers] = await Promise.all([
    api("/api/catalog/categories").catch(() => []),
    api("/api/users/demo").catch(() => []),
  ]);
  state.categories = categories;
  state.demoUsers = demoUsers;
  $("#catnav").innerHTML = `<a href="#/">All</a>` + categories.slice(0, 9)
    .map((c) => `<a href="#/c/${encodeURIComponent(c.key)}">${esc(c.label)}</a>`).join("");
  // First visit: start as the most active demo shopper so the ALS path is visible immediately
  if (!store.get("userChosen", false) && demoUsers.length && state.userId.startsWith("guest-")) {
    state.userId = String(demoUsers[0].user_id);
    store.set("userId", state.userId);
    state.cart = store.get(cartKey(state.userId), []);
    updateCartCount();
  }
  store.set("userChosen", true);
  updateAccountLabel();
  render();
}

init();
