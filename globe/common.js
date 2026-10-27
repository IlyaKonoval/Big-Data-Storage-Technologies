// Shared by the 3D pages: ClickHouse access through the read-only /ch/ proxy, number
// formatting and the textured globe.
const EARTH_KM = 6371;
const TEXTURES = 'https://cdn.jsdelivr.net/npm/three-globe@2.45.3/example/img/';

async function ch(sql) {
  // GET requests run read-only in ClickHouse; the format goes into the query itself and
  // large results come back gzip-compressed.
  const r = await fetch('/ch/?enable_http_compression=1&query='
                        + encodeURIComponent(sql + ' FORMAT JSONCompact'));
  if (!r.ok) throw new Error(await r.text());
  return (await r.json()).data;
}

const fmt = n => Math.round(n).toLocaleString('ru');
const tip = html => `<div class="tip">${html}</div>`;

function makeGlobe(el) {
  return Globe()(el)
    .backgroundImageUrl(TEXTURES + 'night-sky.png')
    .globeImageUrl(TEXTURES + 'earth-blue-marble.jpg')
    .bumpImageUrl(TEXTURES + 'earth-topology.png')
    .atmosphereColor('#6ea8ff').atmosphereAltitude(0.18);
}

function setStatus(text) {
  const s = document.getElementById('status');
  s.style.display = text ? 'block' : 'none';
  s.textContent = text || '';
}
