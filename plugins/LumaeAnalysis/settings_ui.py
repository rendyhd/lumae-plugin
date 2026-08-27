"""Background status updates for the settings page, without navigating away."""


SETTINGS_STATUS_SCRIPT = r"""
<script>
(() => {
  const root = document.querySelector('.lumae-analysis-settings');
  if (!root || root.dataset.polling) return;
  root.dataset.polling = 'true';
  const notice = root.querySelector('[data-lumae-refresh-notice]');
  const snapshots = new Map();
  let timer, controller, stopped = false;
  const delay = () => root.querySelector('[data-lumae-active="true"]') ? 5000 : 30000;
  const stateKey = element => JSON.stringify([
    element.closest('[data-lumae-source]')?.dataset.lumaeSource ||
      element.closest('article[aria-label]')?.getAttribute('aria-label') || '',
    element.name || element.querySelector('summary')?.textContent.trim() || ''
  ]);

  function updatePanel(panel, html) {
    // Leave a panel alone while someone is typing, using its controls, or
    // selecting text. Other panels can still report background progress.
    const selection = window.getSelection();
    if (panel.contains(document.activeElement) ||
        (selection && !selection.isCollapsed &&
         (panel.contains(selection.anchorNode) || panel.contains(selection.focusNode)))) return;
    if (snapshots.get(panel) === html) return;
    const fragment = document.createElement('template');
    fragment.innerHTML = html;
    const fields = new Map();
    panel.querySelectorAll('input:not([type="hidden"]):not([type="file"])').forEach(input => {
      if (input.value !== input.defaultValue || input.checked !== input.defaultChecked) {
        fields.set(stateKey(input), {value: input.value, checked: input.checked});
      }
    });
    const details = new Map(Array.from(panel.querySelectorAll('details'),
      detail => [stateKey(detail), detail.open]));
    fragment.content.querySelectorAll('input:not([type="hidden"]):not([type="file"])').forEach(input => {
      const state = fields.get(stateKey(input));
      if (state) { input.value = state.value; input.checked = state.checked; }
    });
    fragment.content.querySelectorAll('details').forEach(detail => {
      const key = stateKey(detail);
      if (details.has(key)) detail.open = details.get(key);
    });
    panel.replaceChildren(fragment.content);
    panel.hidden = !html.trim();
    snapshots.set(panel, html);
  }

  function schedule(milliseconds = delay()) {
    clearTimeout(timer);
    if (!stopped) timer = setTimeout(poll, milliseconds);
  }

  async function poll() {
    if (stopped || controller) return;
    if (!root.isConnected) { stop(); return; }
    if (document.hidden) { schedule(); return; }
    controller = new AbortController();
    const timeout = setTimeout(() => controller?.abort(), 15000);
    let retry = false;
    try {
      const response = await fetch(root.dataset.statusUrl, {
        cache: 'no-store', credentials: 'same-origin', signal: controller.signal,
        headers: {'Accept': 'application/json'}
      });
      if (!response.ok) throw new Error('Status unavailable');
      const snapshot = await response.json();
      const panels = Array.from(root.querySelectorAll('[data-lumae-status-panel]'));
      if (!snapshot.panels || panels.some(panel =>
          typeof snapshot.panels[panel.dataset.lumaeStatusPanel] !== 'string')) {
        throw new Error('Invalid status response');
      }
      if (stopped || document.hidden || !root.isConnected) return;
      const x = window.scrollX, y = window.scrollY;
      panels.forEach(panel => updatePanel(panel, snapshot.panels[panel.dataset.lumaeStatusPanel]));
      if (window.scrollX !== x || window.scrollY !== y) window.scrollTo(x, y);
      notice.textContent = 'Status updates automatically without reloading this page.';
    } catch (error) {
      retry = true;
      if (!stopped) notice.textContent = 'Live status is temporarily unavailable. Retrying automatically; you can still edit settings.';
    } finally {
      clearTimeout(timeout);
      controller = null;
      schedule(retry ? 30000 : delay());
    }
  }

  function stop() {
    stopped = true;
    clearTimeout(timer);
    controller?.abort();
  }
  root.addEventListener('submit', stop);
  window.addEventListener('pagehide', stop);
  window.addEventListener('pageshow', event => {
    if (event.persisted) { stopped = false; schedule(); }
  });
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && !controller) schedule();
  });
  schedule();
})();
</script>
"""
