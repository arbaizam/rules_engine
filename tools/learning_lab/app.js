'use strict';
const $ = (s, root = document) => root.querySelector(s);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const pretty = value => JSON.stringify(value, null, 2);
const code = value => `<pre>${esc(typeof value === 'string' ? value : pretty(value))}</pre>`;
const STORE = 'rules-engine-field-guide-v1';
let boot, lessonId = 'contract', labId = 'atomic', result = null, baseline = null;
let rowIndex = 0, stepIndex = 0, moduleId = 'runtime', sourcePath = 'src/rules_engine/runtime.py';
let progress = {answers:{}, notes:{}, completed:{}, drafts:{}, reviews:{}};
let sourceCache = {}, running = false, noticeTimer, sourceRequest = 0;
try { progress = {...progress, ...JSON.parse(localStorage.getItem(STORE) || '{}')}; } catch { /* Private browsing still supports this session. */ }
const route = () => location.hash.slice(1).split('/')[0] || 'learn';
const lab = () => boot.curriculum.labs.find(item => item.id === labId);
function save() { try { localStorage.setItem(STORE, JSON.stringify(progress)); } catch { notify('Notes are available for this session; export them before closing.'); } updateProgress(); }
function notify(message) { $('#notice').textContent = message; clearTimeout(noticeTimer); noticeTimer = setTimeout(() => $('#notice').textContent = '', 4500); }
function updateProgress() {
  if (!boot) return;
  const all = boot.curriculum.lessons.flatMap(l => l.questions);
  const right = all.filter(q => progress.answers[q.id]?.correct).length;
  $('#progress-summary').textContent = `${right} / ${all.length} reasoning checks passed`;
}
function head(kicker, title, subtitle, badge = '') {
  return `<div class="pagehead"><div><p class="eyebrow">${esc(kicker)}</p><h1>${esc(title)}</h1><p class="lede">${esc(subtitle)}</p></div>${badge ? `<span class="tag">${esc(badge)}</span>` : ''}</div>`;
}
function refs(items) {
  return `<div class="evidence">${items.map(r => `<button class="source-ref" data-source="${esc(r.path)}" data-symbol="${esc(r.symbol || '')}">${esc(r.path.split('/').pop())}${r.symbol ? ` · ${esc(r.symbol)}` : ''} ↗</button>`).join('')}</div>`;
}
function render() {
  if (!boot) return;
  $$('#nav a').forEach(a => a.classList.toggle('active', a.dataset.route === route()));
  const routes = {learn:renderLearn, lab:renderLab, map:renderMap, source:renderSourcePage, reference:renderReference, review:renderReview};
  $('#main').innerHTML = (routes[route()] || renderLearn)();
  if (route() === 'source') loadSource(sourcePath, '', $('#source-reader'));
  if (route() === 'lab' && result) renderResult();
  if (route() === 'map') renderModule();
  updateProgress();
}
const $$ = (s, root = document) => Array.from(root.querySelectorAll(s));
function renderLearn() {
  const lessons = boot.curriculum.lessons, item = lessons.find(l => l.id === lessonId);
  return head('01 / Learning path', 'Build a model you can defend.', 'Nine source-backed modules. Predict behavior, inspect the implementation, then prove it with an experiment.', '≈ 4 hours + team workshop') +
    `<div class="course-layout"><div class="lesson-menu" aria-label="Learning modules">${lessons.map((l,i) => `<button class="lesson-link ${l.id === lessonId ? 'selected' : ''}" data-lesson="${l.id}" aria-current="${l.id === lessonId ? 'step' : 'false'}"><span class="number">${progress.completed[l.id] ? '✓' : String(i+1).padStart(2,'0')}</span><span><strong>${esc(l.title)}</strong><small>${l.time} · ${l.questions.length} reasoning checks</small></span></button>`).join('')}</div>
    <article class="panel lesson"><p class="eyebrow">MODULE ${String(lessons.indexOf(item)+1).padStart(2,'0')} / ${item.time}</p><h2 class="lesson-title">${esc(item.title)}</h2><p class="lede">${esc(item.subtitle)}</p>
    ${item.sections.map((s,i) => `<section><h3><span class="section-no">0${i+1}</span>${esc(s.title)}</h3><p>${esc(s.text)}</p></section>`).join('')}
    <div class="section-title">Inspect the evidence</div>${refs(item.refs)}
    <div class="callout">Test this model in <strong>${esc(boot.curriculum.labs.find(l=>l.id===item.lab).title)}</strong>.<div class="actions"><button class="text-button" data-open-lab="${item.lab}">Open the execution experiment →</button></div></div>
    <section><h3>Check your reasoning</h3><p class="muted small">Choose an answer, then explain why the other outcomes are impossible.</p>${item.questions.map(renderQuiz).join('')}</section>
    <section><label class="field-label" for="lesson-notes">Your explanation, counterexample, or open question</label><textarea id="lesson-notes" class="note" data-note="lesson:${item.id}" placeholder="Record what you would explain to a teammate…">${esc(progress.notes['lesson:'+item.id] || '')}</textarea><div class="runbar"><span class="lesson-state">Notes and progress stay in this browser. They are not shared team records.</span><button class="primary" data-complete="${item.id}">${progress.completed[item.id] ? 'Completed ✓' : 'Complete module →'}</button></div></section></article></div>`;
}
function renderQuiz(q) {
  const answer = progress.answers[q.id];
  return `<form class="quiz" data-quiz="${q.id}"><fieldset><legend>${esc(q.prompt)}</legend>${q.options.map((option,i) => `<label class="option"><input type="radio" name="${q.id}" value="${i}" ${answer?.choice === i ? 'checked' : ''}>${esc(option)}</label>`).join('')}</fieldset><div class="actions"><button type="submit">Check reasoning</button><button type="button" class="text-button" data-clear-answer="${q.id}">Clear answer</button></div><div class="feedback ${answer?.correct ? 'good' : 'bad'}" aria-live="polite">${answer ? `${answer.correct ? 'Correct.' : 'Reconsider this.'} ${esc(q.why)}` : ''}</div></form>`;
}
function draft() {
  const item = lab();
  if (!progress.drafts[labId]) progress.drafts[labId] = {yaml:item.yaml, rows:pretty(item.rows), schema:item.schema || ''};
  return progress.drafts[labId];
}
function renderLab() {
  const item = lab(), d = draft();
  return head('02 / Execution lab', 'Predict. Run. Trace the decision.', 'Edit real YAML and rows. Every result below comes from this checkout’s compiler, validator, and row execution loop.', 'REAL ENGINE · LOCAL ROWS') +
    `<div class="lab-controls"><label for="lab-select" class="small">Experiment</label><select id="lab-select">${boot.curriculum.labs.map(l => `<option value="${l.id}" ${labId === l.id ? 'selected' : ''}>${esc(l.title)} — ${esc(l.level)}</option>`).join('')}</select><button data-action="reset-lab">Restore example</button><button data-action="download-lab">Export experiment ↓</button></div>
    <div class="predict"><label class="field-label" for="prediction">${esc(item.question)}</label><textarea id="prediction" data-note="prediction:${labId}" placeholder="Write your prediction before running. Include matches, values, and possible errors.">${esc(progress.notes['prediction:'+labId] || '')}</textarea></div>
    <div class="lab-editors"><div class="panel tight"><div class="editor-top"><label class="field-label" for="yaml-editor">CANONICAL YAML</label><span class="tag">Editable</span></div><textarea id="yaml-editor" class="code-editor" spellcheck="false" data-draft="yaml">${esc(d.yaml)}</textarea></div>
    <div><div class="panel tight"><div class="editor-top"><label class="field-label" for="rows-editor">INPUT ROWS · JSON ARRAY</label><span class="tag">Max 100</span></div><textarea id="rows-editor" class="row-editor" spellcheck="false" data-draft="rows">${esc(d.rows)}</textarea><p class="small muted">JSON fractions are parsed as Decimal. Missing fields behave like null in this row helper; use schema preflight to check Spark column requirements.</p></div>
    <details ${d.schema ? 'open' : ''}><summary>Optional Spark schema preflight</summary><p class="small muted">Paste StructType JSON. Requires PySpark, but no Spark session. Results diagnose compatibility; the local row experiment still runs independently.</p><label for="schema-editor" class="field-label">INPUT SCHEMA · STRUCTTYPE JSON</label><textarea id="schema-editor" spellcheck="false" data-draft="schema" rows="7" placeholder='{"type":"struct","fields":[]}'>${esc(d.schema)}</textarea></details></div></div>
    <div class="runbar"><div class="small muted" id="run-status" aria-live="polite">Compile → semantic validation → exact round trip → observed row execution</div><button class="primary" data-action="run" ${running ? 'disabled' : ''}>${running ? 'Running…' : 'Run experiment →'}</button></div>
    <div id="result" class="result"></div><details><summary>Debrief & deliberate mutations</summary><p>${esc(item.explanation)}</p><ul class="mutations">${item.mutations.map(m=>`<li>${esc(m)}</li>`).join('')}</ul>${refs(item.refs)}</details>`;
}
async function runExperiment() {
  if (running) return;
  running = true; const runButton = $('[data-action="run"]');
  runButton.disabled = true; runButton.textContent = 'Running…';
  const submittedLab = labId, inputs = {...draft()};
  $('#run-status').textContent = 'Executing the current experiment…';
  try {
    const response = await fetch('/api/evaluate', {method:'POST',headers:{'Content-Type':'application/json','X-Lab-Token':boot.token},body:JSON.stringify(inputs)});
    if (!response.ok) { const message = await response.json(); throw Error(message.error || `HTTP ${response.status}`); }
    const next = await response.json();
    if (labId !== submittedLab) return;
    result = {...next, inputs, labId:submittedLab}; rowIndex = 0; stepIndex = 0;
    if (route()==='lab') { renderResult(); $('#run-status').textContent = next.ok ? 'Execution complete. Inspect each row; a row error is not a successful outcome.' : `Stopped at ${next.stage}. Edit the input and run again.`; }
  } catch (error) { notify(`Experiment could not run: ${error.message}`); }
  finally { running=false; const b = $('[data-action="run"]'); if(b){b.disabled=false;b.textContent='Run experiment →';} }
}
function displayValue(value) {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'object' && value.$decimal !== undefined) return `${value.$decimal} (Decimal)`;
  if (typeof value === 'object' && value.$integer !== undefined) return `${value.$integer} (integer)`;
  return typeof value === 'string' ? JSON.stringify(value) : JSON.stringify(value);
}
function renderResult() {
  const target = $('#result'); if (!target || !result) return;
  if (!result.ok) {
    target.innerHTML = `<div class="callout error"><strong>Stopped at ${esc(result.stage)}</strong><p>${esc(result.error ? `${result.error.type}: ${result.error.message}` : 'The compiled ruleset failed semantic validation.')}</p>${result.issues ? result.issues.map(i=>`<p><code>${esc(i.check_name)}</code><br>${esc(i.message)} <span class="small">(${esc(i.object_id)})</span></p>`).join('') : ''}</div>`; return;
  }
  const schema = result.schema;
  target.innerHTML = `<div id="stale-result"></div><div class="result-head"><h2>Execution evidence</h2><div class="actions"><button data-action="baseline">Pin this run for comparison</button><button data-action="download-result">Export result ↓</button></div></div>
    ${schema ? `<details open><summary>Spark schema preflight · ${schema.available ? (schema.passed ? 'passed' : 'failed') : 'unavailable'}</summary>${schema.available ? `${schema.issues.length ? `<div class="callout error">${schema.issues.map(i=>`${esc(i.check_name)}: ${esc(i.message)}`).join('<br>')}</div>` : '<p class="good">Schema compatibility passed. This does not execute Spark rows.</p>'}${code({required_columns:schema.required_columns,assignment_schema:schema.assignment_schema})}` : `<p>${esc(schema.message)}</p>`}</details>` : ''}
    <div class="row-tabs" aria-label="Select input row">${result.rows.map((r,i)=>`<button data-row="${i}" class="${i===rowIndex?'selected':''}">Row ${i+1} · ${r.error ? 'error' : r.outcome.matched ? 'matched' : 'no match'}</button>`).join('')}</div><div id="row-evidence"></div>
    <details><summary>Compiled model, canonical export & persistence</summary><p class="small">SHA-256: <code>${esc(result.content_hash)}</code><br>Export/recompile hash: <strong>${result.round_trip_equal ? 'identical' : 'DIFFERENT'}</strong>. The version row is serialized in memory; nothing is published.</p><div class="tabs"><button data-artifact="canonical_yaml">Canonical YAML</button><button data-artifact="model">Compiled model</button><button data-artifact="persistence">Version row</button><button data-artifact="function_dependencies">Function dependencies</button></div><div id="artifact-view">${code(result.canonical_yaml)}</div></details>
    ${baseline ? `<details open><summary>Compare with pinned run · ${esc(baseline.labId)}</summary><p class="small">Content hash ${baseline.content_hash===result.content_hash?'unchanged':'changed'}. Rows compare by input position, not business keys. A hash change alone does not prove a behavior change.</p><div class="grid-two"><div><h3>Pinned outcomes</h3>${code(baseline.rows.map(r=>r.error || r.outcome))}</div><div><h3>Current outcomes</h3>${code(result.rows.map(r=>r.error || r.outcome))}</div></div><button class="text-button" data-action="clear-baseline">Clear pinned run</button></details>` : ''}`;
  renderRow();
  updateStaleResult();
}
function updateStaleResult() {
  const banner=$('#stale-result');
  if(banner) banner.innerHTML=JSON.stringify(result.inputs)!==JSON.stringify(draft()) ? '<div class="callout warn">These results belong to the last submitted inputs. Run again to evaluate your edits.</div>' : '';
}
function renderRow() {
  const row = result.rows[rowIndex];
  const chips = row.outcome ? Object.entries(row.outcome.assign).map(([k,v])=>`<span class="chip ${v.applied?'':'neutral'}">${esc(k)} · ${v.applied ? esc(displayValue(v.value)) : 'not applied'}</span>`).join('') : '';
  $('#row-evidence').innerHTML = `<div class="panel"><h3>Final outcome · Row ${rowIndex+1}</h3>${row.error ? `<div class="callout error"><strong>${esc(row.error.type)}</strong>: ${esc(row.error.message)}<p class="small">No successful row outcome. Earlier observations below are diagnostic history, not a partial production result.</p></div>` : `<div class="result-strip">${chips}</div>`}<details><summary>Original input & exact outcome</summary>${code({input:row.input,outcome:row.outcome})}</details>
    <div class="section-title">Follow the execution · all observations, including non-matches</div><div class="step-layout"><div class="timeline">${row.steps.map((s,i)=>`<button data-step="${i}" class="step-button ${i===stepIndex?'selected':''}"><small>ORDER ${s.order}</small><strong>${esc(s.rule_id)}</strong><small>${esc(s.status)}</small></button>`).join('')}${row.skipped.length ? `<div class="callout warn small">Not reached: ${esc(row.skipped.join(', '))}</div>` : ''}</div><div id="step-detail"></div></div></div>`;
  renderStep();
}
function renderStep() {
  const steps = result.rows[rowIndex].steps, s = steps[stepIndex];
  if (!s) { $('#step-detail').innerHTML='<p class="empty">No rule was evaluated.</p>'; return; }
  $('#step-detail').innerHTML=`<div class="result-head"><h3>${esc(s.name)}</h3><div class="actions"><button data-action="prev-step" ${stepIndex===0?'disabled':''} aria-label="Previous rule">←</button><span class="small">${stepIndex+1} / ${steps.length}</span><button data-action="next-step" ${stepIndex===steps.length-1?'disabled':''} aria-label="Next rule">→</button></div></div>
    <div class="small muted">${s.stop_on_match ? 'Stops later rules if this rule matches and commits.' : 'Continues after this rule.'}</div>
    <div class="state-grid"><div><h3>Committed values before this rule</h3>${code(s.before)}</div><div><h3>Committed values after this rule</h3>${code(s.after)}</div></div>
    <details><summary>Compiled condition tree · ${esc(s.rule_id)}</summary>${conditionTree(result.model.rules.find(r=>r.rule_id===s.rule_id).root_group,s.conditions)}</details>
    <div class="section-title">Condition resolution</div>${s.conditions.map(conditionHTML).join('')}
    <div class="section-title">Assignments · resolved together, then committed</div>${s.assignments.length ? `<div class="assignment muted"><span>Target</span><span>Previous value</span><span>Proposed value</span></div>${s.assignments.map(a=>`<div class="assignment"><strong>${esc(a.target)}</strong><span>${esc(displayValue(a.old))}</span><span>${esc(displayValue(a.value))}</span></div>`).join('')}` : '<p class="small muted">No assignments committed by this rule.</p>'}
    ${refs([{path:'src/rules_engine/runtime.py',symbol:'_execute_prepared'}])}`;
}
function conditionTree(group,traces) {
  return `<div class="boolean-group"><strong>${esc(group.logical_operator).toUpperCase()}</strong><span class="small muted"> ${esc(group.condition_group_id)}</span>${group.conditions.map(c=>{const trace=traces.find(t=>t.condition_id===c.condition_id);return `<div class="boolean-leaf"><code>${esc(c.condition_id)}</code><span class="tag">${esc(c.operator)}</span><span class="${trace?.passed?'good':'muted'}">${trace ? trace.error?'ERROR':trace.passed?'PASS':trace.active_flag?'NO PASS':'INACTIVE' : 'NOT REACHED'}</span></div>`;}).join('')}${group.groups.map(g=>conditionTree(g,traces)).join('')}</div>`;
}
function conditionHTML(c) {
  if(c.error) return `<div class="condition bad"><div class="condition-top">${esc(c.condition_id)} · ERROR</div>${esc(c.error)}</div>`;
  const operand = o => !o ? '—' : `${o.kind === 'field' ? `field(${o.field_name})` : o.kind === 'assigned' ? `assigned(${o.target_field})` : o.kind === 'custom_function' ? `${o.function_name}(…)` : 'literal'} = ${o.evaluated===false ? 'not evaluated' : displayValue(Object.hasOwn(o,'resolved_value') ? o.resolved_value : o.value)}`;
  return `<div class="condition"><div class="condition-top"><span>${esc(c.condition_id)}</span><strong class="${c.passed?'good':'bad'}">${c.active_flag ? c.passed?'PASS':'NO PASS' : 'INACTIVE'}</strong></div><div class="condition-values">${esc(operand(c.left))} <strong>${esc(c.operator)}</strong> ${esc(operand(c.right))}</div><div class="condition-meta">Group ${esc(c.condition_group_operator)} · comparison=${esc(displayValue(c.comparison_result))} · tolerance=${esc(c.tolerance_abs)}${c.left?.default_applied || c.right?.default_applied ? ' · operand default applied' : ''}${c.left?.produced_by_rule_id ? ` · left produced by ${esc(c.left.produced_by_rule_id)}` : ''}</div><details><summary>Resolved operand evidence</summary>${code(c)}</details></div>`;
}
const layers = [
  ['AUTHOR','Strict YAML → model','compiler_yaml validates shape; frozen models hold canonical metadata.'],
  ['VALIDATE','Semantic + schema gates','validator checks contracts; spark_validator adds source and target types.'],
  ['PUBLISH','Immutable version row','publish → repository → serializer / model_codec / canonical_values.'],
  ['EXECUTE','Driver → Python worker','spark_runtime prepares the UDF; runtime owns row decisions and commits.'],
  ['CONSUME','Results, business rows, audit','dataframe_evaluation exposes projections; analytics measures coverage.']
];
function renderMap() {
  const modules = Object.entries(boot.sources).filter(([p])=>p.startsWith('src/'));
  return head('03 / Architecture atlas','Find the owner of each decision.','The lifecycle below is curated. Module dependencies and symbol locations are extracted from the live checkout. Select a module to inspect its relationships.',`${modules.length} source modules`) +
    `<div class="map-pipeline">${layers.map(([label,title,desc])=>`<div class="boundary"><span>${label}</span><h3>${title}</h3><p>${desc}</p></div>`).join('')}</div><div class="callout small">Imports show navigation relationships, including type imports and lazy imports. They are not a complete call graph or a guarantee of semantic impact.</div><div class="module-grid">${modules.map(([path,s])=>{const name=path.split('/').pop().replace('.py','');return `<button class="module ${name===moduleId?'selected':''}" data-module="${name}"><strong>${esc(name)}</strong><small>${s.lines} lines · ${s.symbols.length} symbols</small></button>`;}).join('')}</div><div id="module-detail" class="panel module-detail"></div>`;
}
function renderModule() {
  const path=`src/rules_engine/${moduleId}.py`, s=boot.sources[path];
  const consumers=Object.entries(boot.sources).filter(([,v])=>v.imports.includes(moduleId));
  $('#module-detail').innerHTML=`<p class="eyebrow">Selected module</p><h2>${esc(moduleId)}.py</h2><p>${esc(s.description || 'Inspect source for the module contract.')}</p><div class="grid-two"><div><h3>Imports engine modules</h3>${refs(s.imports.map(n=>({path:`src/rules_engine/${n}.py`})))}${!s.imports.length?'<p class="small muted">No direct rules_engine module imports.</p>':''}<h3>Imported by source or tests</h3>${refs(consumers.map(([path])=>({path})))}</div><div><h3>Jump to implementation</h3>${refs(s.symbols.filter(n=>n.kind==='ClassDef'||!n.name.startsWith('_')).slice(0,14).map(n=>({path,symbol:n.name})))}<button data-source="${path}">Open full module →</button></div></div>`;
}
async function getSource(path) {
  if(sourceCache[path]) return sourceCache[path];
  const response=await fetch('/api/source?path='+encodeURIComponent(path));
  if(!response.ok) throw Error('Source is unavailable.');
  const data=await response.json();sourceCache[path]=data;return data;
}
function sourceHTML(path,s,symbol='') {
  const selected=s.symbols.find(n=>n.name===symbol), start=selected?.line || 1, end=selected?.end || 0;
  return `<div class="source-toolbar"><h3>${esc(path)}</h3><select aria-label="Jump to source symbol" data-source-symbols="${esc(path)}"><option value="">Jump to symbol…</option>${s.symbols.map(n=>`<option value="${esc(n.name)}" ${n.name===symbol?'selected':''}>${n.line} · ${esc(n.name)}</option>`).join('')}</select></div><div class="source-code" tabindex="0" aria-label="Source code"><code>${s.text.split('\n').map((line,i)=>`<span class="source-line ${i+1>=start&&i+1<=end?'highlight':''}" data-line="${i+1}"><span class="line-no">${i+1}</span>${esc(line)||' '}</span>`).join('')}</code></div><p class="small muted">Read from this checkout · SHA-256 ${esc(s.sha256.slice(0,16))} · ${s.lines} lines</p>`;
}
async function loadSource(path,symbol,target) {
  const request=++sourceRequest;
  target.innerHTML='<div class="loading">Reading source…</div>';
  try {const s=await getSource(path);if(request!==sourceRequest)return;target.innerHTML=sourceHTML(path,s,symbol);const match=s.symbols.find(n=>n.name===symbol);if(match){const line=$(`[data-line="${match.line}"]`,target);$('.source-code',target).scrollTop=Math.max(0,line.offsetTop-$('.source-code',target).offsetTop-60);}}
  catch(error){target.innerHTML=`<p class="bad">${esc(error.message)}</p>`;}
}
function openSource(path,symbol) {$('#source-title').textContent=`Source evidence / ${path.split('/').pop()}`;$('#source-dialog').showModal();loadSource(path,symbol,$('#source-body'));}
function renderSourcePage() {
  return head('05 / Source & tests','Read the evidence in context.','Search paths and symbol names across source, tests, guides and the production checklist. Links track symbols rather than hard-coded line numbers.') +
    `<label class="field-label" for="source-search">FIND A MODULE, TEST, OR SYMBOL</label><input class="search" id="source-search" placeholder="Try atomic, serialize, assigned, or test_spark…"><div class="source-layout"><div class="source-list" id="source-list">${sourceList('')}</div><div id="source-reader"></div></div>`;
}
function sourceList(term) {
  term=term.toLowerCase();
  const items=Object.entries(boot.sources).filter(([p,s])=>p.toLowerCase().includes(term)||s.symbols.some(n=>n.name.toLowerCase().includes(term)));
  return items.length ? items.map(([p,s])=>`<button class="source-item" data-read-source="${esc(p)}">${esc(p)}<span class="muted"> · ${s.lines} lines</span></button>`).join('') : '<p class="empty">No matching paths or symbols.</p>';
}
function renderReference() {
  const m=boot.manifest;
  return head('04 / Contract explorer','Inspect what authors can actually use.','Generated by build_authoring_manifest from the real registry. This is the installed contract, not a hand-maintained operator list.',`ENGINE ${boot.version}`) +
    `<div class="panel"><h2>Comparison operators</h2><div class="table-wrap"><table><thead><tr><th>Operator</th><th>Arity</th><th>Right operand</th><th>Tolerance allowed</th></tr></thead><tbody>${m.comparison_operators.map(o=>`<tr><td><code>${esc(o.name)}</code></td><td>${o.arity}</td><td>${esc(o.right_operand_shape)}</td><td>${o.supports_tolerance?'Yes':'No'}</td></tr>`).join('')}</tbody></table></div><p class="small muted">Allowed does not mean identical behavior: inspect the evaluator for each operator’s exact tolerance semantics.</p>${refs([{path:'src/rules_engine/authoring.py',symbol:'build_authoring_manifest'},{path:'src/rules_engine/runtime.py',symbol:'_compare_values'}])}</div>
    <div class="grid-two module-detail"><div class="panel"><h2>Operand kinds</h2>${m.operand_kinds.map(k=>`<span class="chip">${esc(k)}</span>`).join(' ')}<p class="small muted">field reads the source; assigned reads prior commits; literal is metadata; custom_function resolves a registered callable.</p></div><div class="panel"><h2>Literal type hints</h2>${code(m.literal_type_hints)}<p class="small muted">Type-hint aliases are explicitly declared here. They do not imply aliases for operator names or YAML keys.</p></div></div>
    <h2>Registered functions <span class="muted small">${m.functions.length} contracts</span></h2><label for="function-search" class="field-label">SEARCH FUNCTION CONTRACTS</label><input class="search" id="function-search" placeholder="Function name, return type, or argument…"><div id="function-list">${functionList('')}</div><details><summary>Full authoring manifest</summary>${code(m)}</details>`;
}
function functionList(term) {
  const functions=boot.manifest.functions.filter(f=>JSON.stringify(f).toLowerCase().includes(term.toLowerCase()));
  return functions.length?functions.map(f=>`<details class="function-card"><summary>${esc(f.function_name || f.name)}</summary>${code(f)}${refs([{path:'src/rules_engine/standard_functions.py'},{path:'src/rules_engine/registry.py',symbol:'CustomFunctionSpec'}])}</details>`).join(''):'<p class="empty">No matching functions.</p>';
}
function renderReview() {
  return head('06 / Change review','Make the reasoning reviewable.','Use these playbooks to connect an incident or proposed change to contracts, implementation owners, and the evidence needed to accept it.')+
    `<div class="grid-two"><div>${boot.curriculum.playbooks.map((p,i)=>`<details class="panel review-card" ${i===0?'open':''}><summary>${esc(p.title)}</summary><p class="muted">${esc(p.trigger)}</p><ol>${p.steps.map(s=>`<li>${esc(s)}</li>`).join('')}</ol><div class="callout small"><strong>Required evidence:</strong> ${esc(p.proof)}</div>${refs(p.files.map(f=>({path:`src/rules_engine/${f}.py`})))}<label class="field-label" for="review-${i}">Your proposed invariant, counterexample, and tests</label><textarea id="review-${i}" class="note" data-note="review:${i}">${esc(progress.notes['review:'+i] || '')}</textarea></details>`).join('')}</div><div><div class="panel"><p class="eyebrow">TEAM SESSION / 90 MINUTES</p><h2>Practice explaining the system.</h2><p class="small muted">An expert can predict an unfamiliar case, identify the owning boundary, and choose a test that could prove them wrong.</p>${boot.curriculum.workshop.map(w=>`<div class="condition"><span class="tag">${w.duration}</span><h3>${esc(w.title)}</h3><p class="small">${esc(w.text)}</p></div>`).join('')}</div><div class="panel module-detail"><h2>Readiness review</h2>${['I can explain field versus assigned and the pre-rule snapshot.','I can distinguish a clean no-match, a row error, and an explicit null assignment.','I can trace a result through compiler, validator, Spark worker and projection.','I can explain immutable versions, hashes and declared function identity.','I can identify what local tests do not prove and name the target-runtime checks.'].map((s,i)=>`<label class="checklist-label"><input type="checkbox" data-readiness="${i}" ${progress.reviews[i]?'checked':''}>${esc(s)}</label>`).join('')}<p class="small muted">This is self-assessment, not production certification.</p>${refs([{path:'docs/rules_engine_production_checklist.md'}])}</div></div></div>`;
}
function download(name,data) {const url=URL.createObjectURL(new Blob([typeof data==='string'?data:pretty(data)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
document.addEventListener('click', event => {
  const el=event.target.closest('button');if(!el||!boot)return;
  if(el.dataset.lesson){lessonId=el.dataset.lesson;render();}
  if(el.dataset.clearAnswer){delete progress.answers[el.dataset.clearAnswer];delete progress.completed[lessonId];save();render();}
  if(el.dataset.openLab){selectLab(el.dataset.openLab);location.hash='lab';if(route()==='lab')render();}
  if(el.dataset.source)openSource(el.dataset.source,el.dataset.symbol||'');
  if(el.dataset.module){moduleId=el.dataset.module;render();}
  if(el.dataset.readSource){sourcePath=el.dataset.readSource;loadSource(sourcePath,'',$('#source-reader'));}
  if(el.dataset.row!==undefined){rowIndex=Number(el.dataset.row);stepIndex=0;renderResult();}
  if(el.dataset.step!==undefined){stepIndex=Number(el.dataset.step);renderRow();}
  if(el.dataset.artifact)$('#artifact-view').innerHTML=code(result[el.dataset.artifact]);
  if(el.dataset.complete){const item=boot.curriculum.lessons.find(l=>l.id===el.dataset.complete);if(!item.questions.every(q=>progress.answers[q.id]?.correct)){notify('Pass both reasoning checks before completing this module.');return;}progress.completed[item.id]=true;save();const index=boot.curriculum.lessons.indexOf(item);if(index<boot.curriculum.lessons.length-1)lessonId=boot.curriculum.lessons[index+1].id;render();notify('Module completed. Your notes stay available.');}
  const action=el.dataset.action;
  if(action==='run')runExperiment();
  if(action==='reset-lab'){delete progress.drafts[labId];result=null;save();render();}
  if(action==='baseline'){baseline=JSON.parse(JSON.stringify(result));renderResult();notify('Pinned. Edit an input and run again to compare.');}
  if(action==='clear-baseline'){baseline=null;renderResult();}
  if(action==='download-lab')download(`rules-engine-${labId}.json`,{lab:labId,...draft(),prediction:progress.notes['prediction:'+labId]||'',engine_version:boot.version,source_fingerprint:boot.fingerprint});
  if(action==='download-result')download(`rules-engine-${labId}-evidence.json`,{...result,engine_version:boot.version,source_fingerprint:boot.fingerprint});
  if(action==='prev-step'){stepIndex--;renderRow();}
  if(action==='next-step'){stepIndex++;renderRow();}
});
function selectLab(id){labId=id;result=null;rowIndex=0;stepIndex=0;}
document.addEventListener('change',event=>{
  const el=event.target;
  if(el.id==='lab-select'){selectLab(el.value);render();}
  if(el.dataset.sourceSymbols){const target=el.closest('#source-body')||$('#source-reader');loadSource(el.dataset.sourceSymbols,el.value,target);}
  if(el.dataset.readiness!==undefined){progress.reviews[el.dataset.readiness]=el.checked;save();}
});
document.addEventListener('input',event=>{
  const el=event.target;
  if(el.dataset.note){progress.notes[el.dataset.note]=el.value;save();}
  if(el.dataset.draft){draft()[el.dataset.draft]=el.value;save();if($('#run-status'))$('#run-status').textContent='Input changed. Run again to refresh the evidence.';if(result?.ok)updateStaleResult();}
  if(el.id==='source-search')$('#source-list').innerHTML=sourceList(el.value);
  if(el.id==='function-search')$('#function-list').innerHTML=functionList(el.value);
});
document.addEventListener('submit',event=>{
  const form=event.target.closest('[data-quiz]');if(!form)return;event.preventDefault();
  const chosen=$('input:checked',form);if(!chosen){notify('Choose an answer first.');return;}
  const q=boot.curriculum.lessons.flatMap(l=>l.questions).find(q=>q.id===form.dataset.quiz);
  const answer={choice:Number(chosen.value),correct:Number(chosen.value)===q.answer};progress.answers[q.id]=answer;save();
  const feedback=$('.feedback',form);feedback.className=`feedback ${answer.correct?'good':'bad'}`;feedback.textContent=`${answer.correct?'Correct.':'Reconsider this.'} ${q.why}`;
});
$('#close-source').addEventListener('click',()=>$('#source-dialog').close());
$('#export-progress').addEventListener('click',()=>download('rules-engine-learning-notes.json',{...progress,engine_version:boot.version,source_fingerprint:boot.fingerprint,exported_at:new Date().toISOString()}));
window.addEventListener('hashchange',render);
async function start(){try{const r=await fetch('/api/bootstrap');if(!r.ok)throw Error(`HTTP ${r.status}`);boot=await r.json();$('#engine-version').textContent='v'+boot.version;$('#fingerprint').textContent='SOURCE '+boot.fingerprint.slice(0,12);render();}catch(error){$('#main').innerHTML=`<h1>The local workbench is unavailable.</h1><p>${esc(error.message)}</p><p>Start the server with <code>python tools/learning_lab/server.py</code>, then reload.</p>`;}}
start();
