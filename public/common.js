/* 公共前端逻辑：带自动重连的 WebSocket 封装。
 * clientId 存于 localStorage，断线重连后服务端以下发的全量状态快照恢复页面。 */
'use strict';

const CueClient = (() => {
  function getClientId() {
    let id = localStorage.getItem('cueClientId');
    if (!id) {
      id = crypto.randomUUID();
      localStorage.setItem('cueClientId', id);
    }
    return id;
  }

  /**
   * @param {object} opts
   * @param {string} opts.role - 'director' | 'control' | 'position'
   * @param {() => (string|null)} [opts.getPosition] - 岗位端返回岗位名
   * @param {(state: object) => void} opts.onState
   * @param {(message: string) => void} [opts.onError]
   * @param {(connected: boolean) => void} [opts.onConnChange]
   */
  function connect(opts) {
    let ws = null;
    let closed = false;

    function hello() {
      const payload = { type: 'hello', role: opts.role, clientId: getClientId() };
      if (opts.role === 'position' && opts.getPosition) {
        payload.position = opts.getPosition();
      }
      send(payload);
    }

    function open() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${location.host}/ws`);

      ws.onopen = () => {
        if (opts.onConnChange) opts.onConnChange(true);
        hello();
      };
      ws.onmessage = (ev) => {
        let data;
        try { data = JSON.parse(ev.data); } catch { return; }
        if (data.type === 'state') opts.onState(data.state);
        else if (data.type === 'error' && opts.onError) opts.onError(data.message);
      };
      ws.onclose = () => {
        if (opts.onConnChange) opts.onConnChange(false);
        if (!closed) setTimeout(open, 1000); // 断线自动重连
      };
      ws.onerror = () => ws.close();
    }

    function send(payload) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(payload));
        return true;
      }
      return false;
    }

    open();
    return { send, rehello: hello, close: () => { closed = true; ws && ws.close(); } };
  }

  function fmtTime(isoOrMs) {
    const d = new Date(isoOrMs);
    return d.toLocaleTimeString('zh-CN', { hour12: false });
  }

  const EVENT_LABELS = {
    'sequence-set': '序列已保存',
    'session-start': '演出开始',
    'cue-started': '发出口令',
    'cue-revised': '撤回纠正重发',
    'cue-revision-sealed': '旧轮次封存',
    'cue-completed': '口令完成',
    'cue-timeout': '确认超时',
    'confirm': '岗位确认',
    'confirm-late': '迟到确认',
    'confirm-duplicate': '重复确认',
    'confirm-stale-rejected': '旧轮次确认被拒',
    'stale-timeout-rejected': '旧定时器回调被拒',
    'position-substituted': '临时替岗',
    'sequence-finished': '全部口令完成',
    'session-reset': '场次重置',
    'position-online': '岗位上线',
    'position-offline': '岗位离线',
  };

  function renderLog(log) {
    return log.slice().reverse().map((e) => {
      const label = EVENT_LABELS[e.event] || e.event;
      const detail = Object.entries(e.detail || {})
        .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join('/') : JSON.stringify(v)}`)
        .join(' ');
      const cls = ['cue-timeout', 'confirm-duplicate', 'confirm-late',
        'confirm-stale-rejected', 'stale-timeout-rejected'].includes(e.event)
        ? 'log-warn' : '';
      return `<div class="log-row ${cls}"><span class="log-ts">${fmtTime(e.ts)}</span>` +
        `<span class="log-event">${label}</span><span class="log-detail">${escapeHtml(detail)}</span></div>`;
    }).join('');
  }

  // 当前修订号徽标：revision > 1 时显示
  function revisionBadge(cue) {
    if (!cue || (cue.revision || 1) <= 1) return '';
    return `<span class="rev-badge" title="本口令已被撤回并纠正重发">修订 #${cue.revision}</span>`;
  }

  // 旧轮次（已撤回封存）只读结果列表，按修订号倒序
  function renderRevisionHistory(cue) {
    if (!cue || !Array.isArray(cue.history) || cue.history.length === 0) return '';
    const esc = escapeHtml;
    return cue.history.slice().reverse().map((h) => {
      const confs = Object.entries(h.confirmations || {});
      const confHtml = confs.length
        ? confs.map(([p, c]) =>
            `<span class="chip ${c.late ? 'late' : 'ok'}">${esc(p)}${c.late ? ' · 迟到' : ''}</span>`).join('')
        : '<span class="muted">无</span>';
      const timeoutHtml = (h.timedOut || []).length
        ? h.timedOut.map((p) => `<span class="chip timeout">${esc(p)}</span>`).join('')
        : '<span class="muted">无</span>';
      const subHtml = Object.entries(h.substitutions || {}).length
        ? Object.entries(h.substitutions).map(([from, s]) =>
            `<span class="chip subbed">${esc(from)}</span> → <span class="chip">${esc(s.substitute)}</span>`).join('；')
        : '<span class="muted">无</span>';
      const resultText = h.result === 'timeout'
        ? `旧轮次超时收场（${h.confirmedCount}/${h.totalCount} 已确认）`
        : `撤回时仍在等待确认（${h.confirmedCount}/${h.totalCount} 已确认）`;
      return `<div class="rev-item">
        <div class="rev-item-head">🔒 第 ${h.revision} 轮（旧 attemptId <code>${esc(h.attemptId.slice(0, 8))}</code>）
          <span class="chip late">${esc(resultText)}</span>
          <span class="rev-item-time">封存于 ${fmtTime(h.sealedAt)}</span></div>
        <div class="rev-item-text">旧口令：${esc(h.text)}</div>
        <div class="rev-item-reason">撤回原因：${esc(h.reason)}</div>
        <div class="rev-item-row">已确认：${confHtml}</div>
        <div class="rev-item-row">超时未确认：${timeoutHtml}</div>
        <div class="rev-item-row">替岗：${subHtml}</div>
      </div>`;
    }).join('');
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  return {
    connect, getClientId, fmtTime, renderLog, escapeHtml,
    revisionBadge, renderRevisionHistory,
  };
})();
