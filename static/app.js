const boot = window.ASIM_BOOT || { modelOptions: [], poligonOptions: [], currentUser: null };
const sections = {
  processing: document.getElementById('processing-section'),
  results: document.getElementById('results-section'),
  poligons: document.getElementById('poligons-section'),
  snapshots: document.getElementById('snapshots-section'),
  logs: document.getElementById('logs-section'),
  admin: document.getElementById('admin-section'),
};

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  let data = null;
  try { data = await response.json(); } catch (_) {}
  if (!response.ok) throw new Error(data?.error || `Ошибка ${response.status}`);
  return data;
}

function showError(error) { alert(error.message || 'Произошла ошибка'); }

async function readFileAsText(file) {
  return await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ''));
    reader.onerror = () => reject(new Error('Не удалось прочитать файл'));
    reader.readAsText(file, 'utf-8');
  });
}

function scrollLogToBottom(el) { el.scrollTop = el.scrollHeight; }

function renderTableRows(targetId, rows, includeDownload = false, includeDelete = false, includeResultActions = false) {
  const tbody = document.getElementById(targetId);
  tbody.innerHTML = '';
  const cols = includeDownload || includeDelete || includeResultActions ? 7 : 6;
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="${cols}" class="table-center">Нет данных</td></tr>`;
    return;
  }
  rows.forEach((row) => {
    const period1 = `<span class="date-line">${row.start}</span><span class="date-line">${row.end}</span>`;
    const period2 = (row.start2 && row.start2 !== '—') ? `<span class="date-line">${row.start2}</span><span class="date-line">${row.end2}</span>` : '—';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="table-left">${row.model}</td>
      <td class="table-left">${row.poligon}</td>
      <td class="table-center">${row.cloud}</td>
      <td class="table-center">${period1}</td>
      <td class="table-center">${period2}</td>
      <td class="table-center">${row.time}</td>
      ${includeResultActions ? `<td class="table-center"><div class="inline-actions"><a class="soft-btn link-btn" href="${row.download_url}">Скачать</a><button class="soft-btn" data-result-delete="${row.uuid}">Удалить</button></div></td>` : ''}
      ${includeDownload && !includeResultActions ? `<td class="table-center"><a class="soft-btn link-btn" href="${row.download_url}">Скачать</a></td>` : ''}
      ${includeDelete ? `<td class="table-center"><button class="soft-btn" data-processing-delete="${row.uuid}">Удалить</button></td>` : ''}
    `;
    tbody.appendChild(tr);
  });
}

function renderPoligons(rows) {
  const tbody = document.getElementById('poligons-body');
  tbody.innerHTML = '';
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="table-center">Нет данных</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="table-left">${row.name}</td>
      <td class="table-left">${row.file}</td>
      <td class="table-center">${row.public}</td>
      <td class="table-center"><div class="inline-actions"><button class="soft-btn" data-poligon-edit="${row.id}">Редактировать</button><button class="soft-btn" data-poligon-delete="${row.id}">Удалить</button></div></td>
    `;
    tbody.appendChild(tr);
  });
}

function renderUsers(rows) {
  const tbody = document.getElementById('users-body');
  if (!tbody) return;
  tbody.innerHTML = '';
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="table-center">Нет данных</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="table-left">${row.username}</td>
      <td class="table-left">${row.name}</td>
      <td class="table-center">${row.group}</td>
      <td class="table-center"><div class="inline-actions"><button class="soft-btn" data-user-edit="${row.username}">Редактировать</button><button class="soft-btn" data-user-password="${row.username}">Сменить пароль</button><button class="soft-btn" data-user-delete="${row.username}">Удалить</button></div></td>
    `;
    tbody.appendChild(tr);
  });
}

async function loadProcessing() { renderTableRows('processing-body', await fetchJson('/api/processing'), false, true); }
async function loadResults() { renderTableRows('results-body', await fetchJson('/api/results'), false, false, true); }
async function loadPoligons() { renderPoligons(await fetchJson('/api/poligons')); }
async function loadUsers() { if (boot.currentUser.group === 'admin') renderUsers(await fetchJson('/api/users')); }

async function loadLogs() {
  const logs = await fetchJson('/api/logs');
  const stack = document.getElementById('logs-stack');
  stack.innerHTML = '';
  const order = ['asim', 'task', 'web', 'cron', 'down'];
  const titles = { asim: 'asim.log', task: 'task.log', web: 'web.log', cron: 'cron.log', 'down': 'sentinel2-download.log' };
  order.forEach((key) => {
    if (!(key in logs)) return;
    const wrap = document.createElement('div');
    wrap.innerHTML = `<h3>${titles[key]}</h3><pre class="log-box" id="log-${key}"></pre>`;
    stack.appendChild(wrap);
    const pre = wrap.querySelector('pre');
    pre.textContent = logs[key];
    scrollLogToBottom(pre);
  });
}

function setActiveSection(name) {
  document.querySelectorAll('.menu-item').forEach((button) => button.classList.toggle('active', button.dataset.section === name));
  Object.entries(sections).forEach(([key, el]) => { if (el) el.classList.toggle('hidden', key !== name); });
  if (name === 'processing') loadProcessing().catch(showError);
  if (name === 'results') loadResults().catch(showError);
  if (name === 'poligons') loadPoligons().catch(showError);
  if (name === 'logs') loadLogs().catch(showError);
  if (name === 'admin') initAdminSection().catch(showError);
  if (name === 'snapshots') {
    const frame = document.getElementById('snapshots-frame');
    if (frame) frame.src = '/admin/snapshots';
  }
}

document.querySelectorAll('.menu-item').forEach((button) => button.addEventListener('click', () => setActiveSection(button.dataset.section)));

function openModal(content, large = false) {
  const backdrop = document.getElementById('modal-backdrop');
  backdrop.innerHTML = `<div class="modal glass-card ${large ? 'large' : ''}">${content}</div>`;
  backdrop.classList.remove('hidden');
  backdrop.onclick = (e) => { if (e.target === backdrop || e.target.matches('[data-close-modal]')) closeModal(); };
}
function closeModal() { const backdrop = document.getElementById('modal-backdrop'); backdrop.classList.add('hidden'); backdrop.innerHTML = ''; }

function processingModalTemplate() {
  const modelOptions = boot.modelOptions.map((item) => `<option value="${item.key}" data-inputs="${item.inputs}" data-season="${item.season || ''}">${item.description}</option>`).join('');
  const poligonOptions = boot.poligonOptions.map((item) => `<option value="${item.key}">${item.name}</option>`).join('');
  return `
    <div class="section-header"><h3>Создать обработку</h3><button class="ghost-btn" data-close-modal>✕</button></div>
    <form id="create-processing-form" class="form-grid">
      <label>Модель<select id="model" name="model" required><option value="">Выберите модель</option>${modelOptions}</select></label>
      <label>Локация<select id="poligon" name="poligon" required><option value="">Выберите локацию</option>${poligonOptions}</select></label>
      <label id="start-label">Дата начала<input type="date" id="start" name="start" required></label>
      <label id="end-label">Дата завершения<input type="date" id="end" name="end" required></label>
      <div id="second-inputs" class="hidden full form-grid">
        <label>Дата начала (start2)<input type="date" id="start2" name="start2"></label>
        <label>Дата завершения (end2)<input type="date" id="end2" name="end2"></label>
      </div>
      <div id="season-year-inputs" class="hidden full form-grid">
        <label>Год нового снимка<select id="year" name="year"><option value="">Выберите год</option></select></label>
        <label>Год базового снимка<select id="year2" name="year2"><option value="">Выберите год</option></select></label>
        <div id="season-hint" class="muted full"></div>
      </div>
      <label class="full">Облачность: <span id="cloud-value">50</span>%<input type="range" id="cloud" name="cloud" min="0" max="100" value="50"></label>
      <div class="full"><button class="primary-btn" type="submit">Создать</button></div>
      <div id="create-error" class="alert hidden full"></div>
    </form>`;
}

function bindProcessingModal() {
  const modelSelect = document.getElementById('model');
  const secondInputs = document.getElementById('second-inputs');
  const seasonYearInputs = document.getElementById('season-year-inputs');
  const startLabel = document.getElementById('start-label');
  const endLabel = document.getElementById('end-label');
  const startEl = document.getElementById('start');
  const endEl = document.getElementById('end');

  const SEASON_RANGES = {
    spring: (y) => [`${y}-03-01`, `${y}-05-31`],
    summer: (y) => [`${y}-06-01`, `${y}-08-31`],
    autumn: (y) => [`${y}-09-01`, `${y}-11-30`],
    winter: (y) => [`${y}-12-01`, `${y + 1}-02-28`],
  };
  const SEASON_LABELS = { spring: 'весна', summer: 'лето', autumn: 'осень', winter: 'зима' };

  let cachedYears = [];

  async function loadYears() {
    if (cachedYears.length) return;
    try { cachedYears = await fetchJson('/api/available-years'); } catch (_) { cachedYears = []; }
  }

  function fillYearSelects() {
    ['year', 'year2'].forEach((id) => {
      const sel = document.getElementById(id);
      const prev = sel.value;
      sel.innerHTML = '<option value="">Выберите год</option>' +
        cachedYears.map((y) => `<option value="${y}"${String(y) === prev ? ' selected' : ''}>${y}</option>`).join('');
    });
  }

  function updateSeasonHint(season) {
    const hint = document.getElementById('season-hint');
    const y = Number(document.getElementById('year').value);
    const y2 = Number(document.getElementById('year2').value);
    const label = SEASON_LABELS[season] || season;
    if (y && y2 && SEASON_RANGES[season]) {
      const [s1, e1] = SEASON_RANGES[season](y);
      const [s2, e2] = SEASON_RANGES[season](y2);
      hint.textContent = `${label}: ${s1}–${e1}  |  базовый: ${s2}–${e2}`;
    } else {
      hint.textContent = label ? `Сезон: ${label}` : '';
    }
  }

  modelSelect.addEventListener('change', async () => {
    const sel = modelSelect.options[modelSelect.selectedIndex];
    const inputs = Number(sel.dataset.inputs || '1');
    const season = sel.dataset.season || '';

    // Сбросить всё в исходное состояние
    secondInputs.classList.add('hidden');
    seasonYearInputs.classList.add('hidden');
    startLabel.classList.remove('hidden');
    endLabel.classList.remove('hidden');
    startEl.required = true;
    endEl.required = true;
    document.getElementById('start2').required = false;
    document.getElementById('end2').required = false;
    document.getElementById('year').required = false;
    document.getElementById('year2').required = false;

    if (season) {
      // Сезонная модель: скрываем ввод дат, показываем выбор года.
      // Работает независимо от inputs — год выбирается всегда когда задан season.
      startLabel.classList.add('hidden');
      endLabel.classList.add('hidden');
      startEl.required = false;
      endEl.required = false;
      await loadYears();
      fillYearSelects();
      seasonYearInputs.classList.remove('hidden');
      document.getElementById('year').required = true;
      // year2 нужен только при inputs=2 (базовый период)
      const year2Label = document.querySelector('label[for="year2"], #season-year-inputs label:nth-child(2)');
      if (inputs === 2) {
        document.getElementById('year2').required = true;
        if (year2Label) year2Label.classList.remove('hidden');
        updateSeasonHint(season);
        ['year', 'year2'].forEach((id) =>
          document.getElementById(id).addEventListener('change', () => updateSeasonHint(season)));
      } else {
        document.getElementById('year2').required = false;
        if (year2Label) year2Label.classList.add('hidden');
        const hint = document.getElementById('season-hint');
        if (hint) hint.classList.add('hidden');
        document.getElementById('year').addEventListener('change', () => updateSeasonHint(season));
      }
    } else if (inputs === 2) {
      // Не сезонная, но двухпериодная: ручной ввод второго диапазона дат
      secondInputs.classList.remove('hidden');
      document.getElementById('start2').required = true;
      document.getElementById('end2').required = true;
    }
  });

  const cloud = document.getElementById('cloud');
  cloud.addEventListener('input', () => document.getElementById('cloud-value').textContent = cloud.value);

  document.getElementById('create-processing-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const errorBox = document.getElementById('create-error');
    errorBox.classList.add('hidden');
    const sel = modelSelect.options[modelSelect.selectedIndex];
    const season = sel.dataset.season || '';
    const inputs = Number(sel.dataset.inputs || '1');
    const payload = {
      model: document.getElementById('model').value,
      poligon: document.getElementById('poligon').value,
      cloud: Number(document.getElementById('cloud').value),
    };
    if (season) {
      // Сезонная модель: передаём год(ы), бэкенд сам вычислит даты
      payload.year = Number(document.getElementById('year').value);
      if (inputs === 2) {
        payload.year2 = Number(document.getElementById('year2').value);
      }
    } else {
      payload.start = document.getElementById('start').value;
      payload.end = document.getElementById('end').value;
      if (inputs === 2) {
        payload.start2 = document.getElementById('start2').value;
        payload.end2 = document.getElementById('end2').value;
      }
    }
    try {
      await fetchJson('/api/processing/create', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      closeModal();
      await loadProcessing();
    } catch (error) { errorBox.textContent = error.message; errorBox.classList.remove('hidden'); }
  });
}

document.getElementById('show-create-processing-modal')?.addEventListener('click', () => { openModal(processingModalTemplate(), true); bindProcessingModal(); });

function poligonModalTemplate(mode, data = {}) {
  const readonly = boot.currentUser.group === 'user';
  const identifier = data.id || crypto.randomUUID().replaceAll('-', '');
  const publicValue = data.public ?? 2;
  const fileName = data.file || '';
  return `
    <div class="section-header"><h3>${mode === 'create' ? 'Создать полигон' : 'Редактировать полигон'}</h3><button class="ghost-btn" data-close-modal>✕</button></div>
    <form id="poligon-form" class="form-grid" enctype="multipart/form-data">
      <label>Идентификатор<input type="text" id="poly-id" value="${identifier}" ${readonly ? 'disabled' : ''} required></label>
      <label>Публичность<input type="number" id="poly-public" min="0" max="2" value="${publicValue}" ${readonly ? 'disabled' : ''} required></label>
      <label class="full">Название полигона<input type="text" id="poly-name" value="${data.name || ''}" required></label>
      <div class="full">
        <div class="file-row">
          <input type="file" id="poly-file" accept=".geojson" ${mode === 'create' ? 'required' : ''}>
          <span class="note" id="poly-file-note">${fileName ? `Текущий файл: ${fileName}` : 'Файл не выбран'}</span>
          ${mode === 'edit' && fileName ? '<button type="button" class="ghost-btn" id="poly-remove-file">Удалить файл</button>' : ''}
        </div>
      </div>
      <div class="full inline-actions"><button class="primary-btn" type="submit">${mode === 'create' ? 'Создать' : 'Сохранить'}</button></div>
      <div id="poligon-error" class="alert hidden full"></div>
    </form>`;
}

function bindPoligonModal(mode, originalId = null) {
  let removeFile = false;
  document.getElementById('poly-remove-file')?.addEventListener('click', () => {
    removeFile = true;
    document.getElementById('poly-file-note').textContent = 'Текущий файл будет удалён. Загрузите новый .geojson';
  });
  document.getElementById('poly-file')?.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) document.getElementById('poly-file-note').textContent = `Выбран файл: ${file.name}`;
  });
  document.getElementById('poligon-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const errorBox = document.getElementById('poligon-error');
    errorBox.classList.add('hidden');
    const file = document.getElementById('poly-file').files[0];
    const payload = {
      identifier: document.getElementById('poly-id').value,
      public: document.getElementById('poly-public').value || '2',
      name: document.getElementById('poly-name').value,
      remove_existing_file: removeFile ? '1' : '0',
      replace_file: file ? '1' : '0',
    };
    if (file) {
      payload.file_name = file.name;
      payload.file_content = await readFileAsText(file);
    }
    try {
      const url = mode === 'create' ? '/api/poligons/create' : `/api/poligons/${originalId}/update`;
      await fetchJson(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      closeModal();
      await loadPoligons();
    } catch (error) { errorBox.textContent = error.message; errorBox.classList.remove('hidden'); }
  });
}

document.getElementById('show-create-poligon-modal')?.addEventListener('click', () => { openModal(poligonModalTemplate('create'), true); bindPoligonModal('create'); });

document.addEventListener('click', async (event) => {
  const pDel = event.target.closest('[data-processing-delete]');
  if (pDel) {
    if (!confirm('Удалить выбранную обработку?')) return;
    try { await fetchJson(`/api/processing/delete/${pDel.dataset.processingDelete}`, { method: 'DELETE' }); await loadProcessing(); } catch (error) { showError(error); }
    return;
  }
  const resultDelete = event.target.closest('[data-result-delete]');
  if (resultDelete) {
    if (!confirm('Удалить выбранный результат?')) return;
    try { await fetchJson(`/api/results/delete/${resultDelete.dataset.resultDelete}`, { method: 'DELETE' }); await loadResults(); } catch (error) { showError(error); }
    return;
  }
  const polyEdit = event.target.closest('[data-poligon-edit]');
  if (polyEdit) {
    try {
      const data = await fetchJson(`/api/poligons/${polyEdit.dataset.poligonEdit}`);
      openModal(poligonModalTemplate('edit', data), true);
      bindPoligonModal('edit', polyEdit.dataset.poligonEdit);
    } catch (error) { showError(error); }
    return;
  }
  const polyDel = event.target.closest('[data-poligon-delete]');
  if (polyDel) {
    if (!confirm('Удалить полигон и его GeoJSON-файл?')) return;
    try { await fetchJson(`/api/poligons/${polyDel.dataset.poligonDelete}`, { method: 'DELETE' }); await loadPoligons(); } catch (error) { showError(error); }
    return;
  }
  const userEdit = event.target.closest('[data-user-edit]');
  if (userEdit) {
    const row = [...document.querySelectorAll('#users-body tr')].find((tr) => tr.querySelector(`[data-user-edit="${userEdit.dataset.userEdit}"]`));
    const cells = row.querySelectorAll('td');
    openModal(`
      <div class="section-header"><h3>Редактировать пользователя</h3><button class="ghost-btn" data-close-modal>✕</button></div>
      <form id="user-edit-form" class="stack-form">
        <label>Идентификатор<input id="edit-user-username" value="${cells[0].textContent.trim()}" required></label>
        <label>Имя<input id="edit-user-name" value="${cells[1].textContent.trim()}" required></label>
        <label>Группа<select id="edit-user-group"><option value="admin">admin</option><option value="user">user</option><option value="satellite">satellite</option></select></label>
        <button class="primary-btn" type="submit">Сохранить</button>
        <div id="user-edit-error" class="alert hidden"></div>
      </form>`);
    document.getElementById('edit-user-group').value = cells[2].textContent.trim();
    document.getElementById('user-edit-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const err = document.getElementById('user-edit-error');
      err.classList.add('hidden');
      try {
        await fetchJson(`/api/users/${userEdit.dataset.userEdit}/update`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: document.getElementById('edit-user-username').value, name: document.getElementById('edit-user-name').value, group: document.getElementById('edit-user-group').value }) });
        closeModal(); await loadUsers();
      } catch (error) { err.textContent = error.message; err.classList.remove('hidden'); }
    });
    return;
  }
  const userDelete = event.target.closest('[data-user-delete]');
  if (userDelete) {
    if (!confirm('Удалить пользователя?')) return;
    try { await fetchJson(`/api/users/${userDelete.dataset.userDelete}`, { method: 'DELETE' }); await loadUsers(); } catch (error) { showError(error); }
    return;
  }
  const userPassword = event.target.closest('[data-user-password]');
  if (userPassword) {
    const generatedPassword = Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 8);
    openModal(`
      <div class="section-header"><h3>Смена пароля пользователя</h3><button class="ghost-btn" data-close-modal>✕</button></div>
      <form id="user-password-form" class="stack-form">
        <label>Новый пароль<input id="reset-user-password" value="${generatedPassword}"></label>
        <div class="note">Пароль сгенерирован автоматически. Его можно оставить как есть или изменить вручную.</div>
        <button class="primary-btn" type="submit">Сохранить</button>
        <div id="user-password-result" class="alert hidden"></div>
      </form>`);
    document.getElementById('user-password-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const box = document.getElementById('user-password-result');
      try {
        const data = await fetchJson(`/api/users/${userPassword.dataset.userPassword}/password`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: document.getElementById('reset-user-password').value }) });
        box.textContent = `Новый пароль: ${data.password}`; box.classList.remove('hidden');
      } catch (error) { box.textContent = error.message; box.classList.remove('hidden'); }
    });
    return;
  }

});

function initAdminSection() {
  document.getElementById('profile-name').textContent = boot.currentUser.name;
  document.getElementById('profile-username').textContent = boot.currentUser.username;
  document.getElementById('profile-group').textContent = boot.currentUser.group;
  document.getElementById('users-admin-panel').classList.toggle('hidden', boot.currentUser.group !== 'admin');
  return loadUsers();
}

document.getElementById('change-password-form')?.addEventListener('submit', async (event) => {
  event.preventDefault();
  const box = document.getElementById('password-message');
  box.classList.add('hidden');
  try {
    await fetchJson('/api/profile/password', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ old_password: document.getElementById('old-password').value, new_password: document.getElementById('new-password').value, confirm_password: document.getElementById('confirm-password').value }) });
    box.textContent = 'Пароль обновлён'; box.classList.remove('hidden'); event.target.reset();
  } catch (error) { box.textContent = error.message; box.classList.remove('hidden'); }
});

document.getElementById('show-create-user-modal')?.addEventListener('click', () => {
  const generatedPassword = Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 8);
  openModal(`
    <div class="section-header"><h3>Добавить пользователя</h3><button class="ghost-btn" data-close-modal>✕</button></div>
    <form id="user-create-form" class="stack-form">
      <label>Идентификатор<input id="create-user-username" required></label>
      <label>Имя<input id="create-user-name" required></label>
      <label>Группа<select id="create-user-group"><option value="user">user</option><option value="admin">admin</option><option value="satellite">satellite</option></select></label>
      <label>Пароль<input id="create-user-password" value="${generatedPassword}"></label>
      <div class="note">Пароль сгенерирован автоматически. Его можно оставить как есть или изменить вручную.</div>
      <button class="primary-btn" type="submit">Создать</button>
      <div id="user-create-result" class="alert hidden"></div>
    </form>`);
  document.getElementById('user-create-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const box = document.getElementById('user-create-result');
    box.classList.add('hidden');
    try {
      const data = await fetchJson('/api/users/create', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: document.getElementById('create-user-username').value, name: document.getElementById('create-user-name').value, group: document.getElementById('create-user-group').value, password: document.getElementById('create-user-password').value }) });
      box.textContent = `Пользователь создан. Пароль: ${data.password}`; box.classList.remove('hidden');
      await loadUsers();
    } catch (error) { box.textContent = error.message; box.classList.remove('hidden'); }
  });
});

loadProcessing().catch(showError);