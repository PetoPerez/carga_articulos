// ── app.js · shopify-uploader ──────────────────────────────────────────

// ── Toast ──────────────────────────────────────────────────────────────
// Requiere un <div id="toast"> en el HTML
const _toast = document.getElementById('toast');
let _toastTimer = null;

function toast(msg, type = '') {
  if (!_toast) return;
  _toast.textContent = msg;
  _toast.className   = 'show ' + type;
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => (_toast.className = ''), 3000);
}

// ── Escaping HTML ──────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Formato de bytes legible ───────────────────────────────────────────
function fmtBytes(bytes) {
  if (bytes < 1024)       return bytes + ' B';
  if (bytes < 1048576)    return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1048576).toFixed(1) + ' MB';
}

// ── Drag & drop genérico ───────────────────────────────────────────────
// zoneId   : id del contenedor .drop-zone
// inputId  : id del <input type="file">
// onFiles  : callback(FileList → Array)
function setupDrop(zoneId, inputId, onFiles) {
  const zone  = document.getElementById(zoneId);
  const input = document.getElementById(inputId);
  if (!zone || !input) return;

  zone.addEventListener('dragover', e => {
    e.preventDefault();
    zone.classList.add('dragover');
  });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('dragover');
    onFiles([...e.dataTransfer.files]);
  });
  input.addEventListener('change', () => onFiles([...input.files]));
}

// ── Spinner helper ─────────────────────────────────────────────────────
// Reemplaza el contenido del botón con spinner + texto mientras carga,
// y lo restaura si falla.
// Devuelve una función restore() para llamar manualmente si hace falta.
function btnLoading(btn, loadingText = 'Cargando...') {
  const original = btn.innerHTML;
  btn.disabled   = true;
  btn.innerHTML  = `<span class="spinner"></span> ${loadingText}`;
  return function restore() {
    btn.disabled  = false;
    btn.innerHTML = original;
  };
}

// ── Status polling ─────────────────────────────────────────────────────
// Actualiza los dots del header consultando /status cada `interval` ms.
// Llama a startStatusPolling() desde el HTML que quiera usarlo.
function startStatusPolling(interval = 30000) {
  async function check() {
    try {
      const res  = await fetch('/status');
      const data = await res.json();
      _setDot('dot-db',      data.database?.connected);
      _setDot('dot-shopify', data.shopify?.connected);
    } catch (_) { /* silencioso */ }
  }
  check();
  setInterval(check, interval);
}

function _setDot(id, ok) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('ok',  !!ok);
  el.classList.toggle('err', !ok);
}

// ── Download helper ────────────────────────────────────────────────────
function downloadTracked() {
  window.location.href = '/download/tracked.xlsx';
}

// ── Clear uploads ──────────────────────────────────────────────────────
async function clearUploads() {
  if (!confirm('¿Limpiar todos los archivos subidos?')) return;
  try {
    const res = await fetch('/clear-uploads', { method: 'DELETE' });
    if (res.ok) {
      toast('Archivos limpiados', 'ok');
      setTimeout(() => location.reload(), 800);
    } else {
      toast('Error al limpiar', 'err');
    }
  } catch (_) {
    toast('Error al limpiar', 'err');
  }
}