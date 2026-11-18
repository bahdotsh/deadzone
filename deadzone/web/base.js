// Shared by every page: the live snapshot, the switches, and a few formatters.

function watch(render) {
  const source = new EventSource("/state");
  source.onmessage = (event) => render(JSON.parse(event.data));
}

function act(action) {
  return fetch("/action", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(action) });
}

function esc(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// "8 s", "3 min 12 s", "1 h 4 min"
function span(seconds) {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
  return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min`;
}

function clock(ts) {
  return new Date(ts * 1000).toLocaleTimeString([], { hour12: false });
}

function feedHtml(items) {
  if (!items.length) return '<li class="empty">Nothing yet.</li>';
  return items.map(([ts, text]) => `<li><time>${clock(ts)}</time><span>${esc(text)}</span></li>`).join("");
}

function plural(n, word) {
  return `${n} ${word}${n === 1 ? "" : "s"}`;
}
