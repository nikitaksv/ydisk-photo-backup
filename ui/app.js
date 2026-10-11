'use strict';
const $=id=>document.getElementById(id);
const fmt=(n,d=0)=>Number(n||0).toLocaleString('ru-RU',{maximumFractionDigits:d,minimumFractionDigits:d});
const bytes=n=>n>=1e9?fmt(n/1e9,2)+' ГБ':n>=1e6?fmt(n/1e6,1)+' МБ':fmt(n/1e3,0)+' КБ';
const time=iso=>new Date(iso).toLocaleTimeString('ru-RU',{timeZone:'Europe/Moscow',hour:'2-digit',minute:'2-digit',second:'2-digit'});
const dateTime=iso=>new Date(iso).toLocaleString('ru-RU',{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
const duration=s=>{let m=Math.max(1,Math.round(s/60));return m>=1440?Math.floor(m/1440)+' д '+Math.floor(m%1440/60)+' ч':m>=60?Math.floor(m/60)+' ч '+m%60+' мин':m+' мин'};
const interval=m=>m%1440===0?m/1440+' д':m%60===0?m/60+' ч':m+' мин';
const text=(id,value)=>{const node=$(id);if(node.textContent!==String(value))node.textContent=value};
const hide=(id,hidden=true)=>$(id).hidden=hidden;
const store=(key,value)=>{try{localStorage.setItem('photoslice.'+key,value)}catch{}};
const remembered=(key,fallback)=>{try{return localStorage.getItem('photoslice.'+key)||fallback}catch{return fallback}};
let snapshot=null,token='',view='overview',offset=0,listTotal=0,lastList=0,refreshing=false,listLoading=false,mutation=false,replaceAccess=false,firstStatus=true;
const speedHistory=[];
const views=['overview','files','journal','settings'];
const pageSize=25;
const phases={idle:'Готов к запуску',running:'Скачивание',previews:'Превью · первый проход',originals:'Оригиналы · второй проход',zipping:'Пакетная загрузка ZIP',rate_limited:'Пауза Яндекса',upgrading:'Проверка качества',checking:'Проверка входа',completed:'Проход завершён',stopped:'Остановлено',attention:'Требуется внимание'};
function switchView(next,focus=false){
 if(!views.includes(next))next='overview';view=next;store('view',next);
 for(const name of views){hide('view-'+name,name!==next);const tab=$('tab-'+name);tab.setAttribute('aria-selected',String(name===next));tab.tabIndex=name===next?0:-1}
 if(focus)$('tab-'+next).focus();
 if(next==='files')loadFiles();if(next==='journal')renderJournal();
}
async function api(path,options={}){
 const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),12000);
 try{const response=await fetch(path,{...options,signal:abort.signal,cache:'no-store'});if(!response.ok)throw new Error((await response.text()).slice(0,700)||'Ошибка '+response.status);return await response.json()}finally{clearTimeout(timer)}
}
function showError(message){text('alertText',message);hide('alert',false)}
function rateText(d){const r=d.rate,t=d.transfer;if(!r)return '—';return(d.phase==='rate_limited'?'пауза':fmt(t?.rps??r.observed_rps,1)+' RPS')+' · '+fmt(r.interval_ms)+' мс'}
function renderSpeed(d){
 const t=d.transfer,paused=d.phase==='rate_limited',moving=d.running&&!paused,unit=$('speedUnit').value;
 text('networkSpeed',t?fmt(moving?t.network_mb_per_second:0,2):'—');
 const rate=unit==='second'?t?.files_per_second:unit==='hour'?t?.files_per_hour:t?.files_per_minute;
 text('fileSpeed',t?fmt(moving?rate:0,unit==='second'?2:unit==='minute'?1:0):'—');
 const multiplier=unit==='second'?1:unit==='hour'?3600:60;
 const versions=(t?.versions_per_second||0)*multiplier;
 hide('versionSpeed',!moving||!t||versions<Number(rate||0)+.05);
 text('versionSpeed',fmt(versions,unit==='second'?2:0)+' версий '+(unit==='second'?'/ с':unit==='hour'?'/ час':'/ мин'));
 text('speedScope',moving?'Сеть · 10 с / новые файлы · 60 с':paused?'Запросы приостановлены':'Загрузка не идёт');
 const etaStates={measuring:'Замер…',idle:'—',rate_limited:'После паузы',no_progress:'Недостаточно данных',known_complete:'Копии есть у всех'};
 const etaState=paused?'rate_limited':!d.running&&t?.eta_state!=='known_complete'?'idle':t?.eta_state;
 text('eta',etaState==='estimate'?'≈ '+duration(t.eta_seconds):etaStates[etaState]||'—');
 text('etaDetail',etaState==='estimate'?'До '+dateTime(t.finish_at)+' МСК · по найденному списку':etaState==='known_complete'?d.stats.no_original+d.stats.no_preview?'Осталось проверить качество':'Оригиналы и превью сохранены':d.running&&!paused?'Прогноз появится при устойчивом прогрессе':'');
 if(t&&moving){speedHistory.push(Number(t.network_mb_per_second)||0);if(speedHistory.length>60)speedHistory.shift()}
 hide('history',speedHistory.length<8||!moving);
 if(speedHistory.length>=8&&moving){const peak=Math.max(...speedHistory,.01);$('sparkline').replaceChildren(...speedHistory.map(v=>{const bar=document.createElement('i');bar.style.height=Math.max(1,v/peak*25)+'px';return bar}));text('historyPeak','пик '+fmt(peak,2))}
}
function renderZIP(d){
 const z=d.zip||{},queued=z.queued||0,unavailable=z.stage==='unavailable',previewPass=d.running&&d.download_stage==='previews',inFlight=!previewPass&&['preparing','downloading','verifying'].includes(z.stage),deferred=z.stage==='deferred'&&queued>0;
 hide('zipLine',!queued&&!inFlight&&!z.archives);
 text('zipLabel',previewPass?'Очередь оригиналов':unavailable?'Серверный ZIP отключён':inFlight?(z.stage==='verifying'?'Проверка ZIP':'Скачивание ZIP'):deferred?'ZIP отложен':queued?'Накопление ZIP':'ZIP сохранены');
 text('zipValue',inFlight?'пачка '+z.batch+' / '+z.batches:queued?fmt(queued)+(queued<500&&!deferred?' / '+(z.batch_size||500):' в очереди'):fmt(z.archives));
 hide('zipBar',unavailable||previewPass||!queued||inFlight||deferred);$('zipBar').max=z.batch_size||500;$('zipBar').value=Math.min(queued,z.batch_size||500);
 let detail=deferred?(d.phase==='rate_limited'?'Очередь сохранена · после ограничения Яндекса':'Очередь сохранена · следующая фоновая проверка'):z.archives?fmt(z.imported)+' оригиналов из ZIP'+(z.missing?' · '+fmt(z.missing)+' пропусков на повторную проверку':''):queued>=500?'Пачка готова · архивы скачиваются по очереди':'';
 if(previewPass&&queued)detail='После прохода превью всех файлов';
 if(unavailable)detail='Яндекс: MPFS 394 · оригиналы загружаются отдельно';
 text('zipDetail',detail);hide('zipDetail',!detail);
}
function renderJournal(){
 if(!snapshot)return;const lines=snapshot.logs||[],events=lines.filter(line=>!line.includes('Сохранён:')&&!line.includes('Без копии:'));
 const full=$('importantOnly').checked?events:lines;
 const node=$('logs'),bottom=node.scrollHeight-node.scrollTop-node.clientHeight<40;
 text('logs',full.join('\n'));hide('logEmpty',full.length>0);if(bottom)node.scrollTop=node.scrollHeight;
 const recentEvents=events.filter(line=>!/Проверка сохранённых файлов:|Обновление метаданных:/.test(line)).slice(-2);
 const lastSaved=lines.filter(line=>line.includes('Сохранён:')).slice(-1);
 const activity=[...recentEvents,...lastSaved].slice(-3);
 text('activity',activity.join('\n'));hide('activityPanel',activity.length===0||!snapshot.running);
}
function render(d){
 snapshot=d;token=d.token;const s=d.stats,t=d.transfer,r=d.rate,z=d.zip,a=r?.tune;
 const active=d.running,missing=s.no_original+s.no_preview,paused=d.phase==='rate_limited';
	 document.querySelector('.primary').hidden=!s.total;document.querySelector('.overview-grid').hidden=!s.total;
	 document.querySelectorAll('.tempo,.forecast').forEach(node=>node.hidden=!active);document.querySelector('.headline').classList.toggle('inactive',!active);hide('speedScope',!active);
 text('connection','● live');$('connection').className='connection';text('out',d.out);$('out').title=d.out;
 text('phase',phases[d.phase]||d.phase);$('dot').className='dot '+(paused||d.phase==='attention'?'paused':active?'':'idle');
 text('stageNote',active?'':d.watching?'следующая проверка '+dateTime(d.next_check)+' МСК':'');
 let notice=paused?'HTTP 429 · общая пауза до '+time(r.limited_until)+' МСК · '+r.interval_ms+' мс между запросами':d.phase==='attention'?d.message:'';
 text('notice',notice);hide('notice',!notice);
 text('done',fmt(s.done));text('total',fmt(s.total));text('percent',s.total?fmt(s.done/s.total*100,1)+'%':'');$('bar').max=Math.max(s.total,1);$('bar').value=s.done;
 text('inventory',s.inventory==='incomplete'?'! Полнота списка пока не подтверждена':s.total?'Список полный':'Список ещё не собран');$('inventory').className=s.inventory==='incomplete'?'muted incomplete':'muted';
 renderSpeed(d);renderZIP(d);
 const quality=[['original',s.original],['preview',s.only_preview],['thumbnail',s.only_thumbnail],['missing',s.total-s.done]];
 $('qualityStack').replaceChildren(...quality.filter(([,n])=>n>0).map(([className,n])=>{const segment=document.createElement('span');segment.className=className;segment.style.width=n/Math.max(s.total,1)*100+'%';return segment}));
 text('original',fmt(s.original));text('preview',fmt(s.only_preview));text('thumbnail',fmt(s.only_thumbnail));text('noCopy',fmt(s.total-s.done));
 hide('thumbRow',s.only_thumbnail===0);hide('missingRow',s.done===s.total);hide('problemSplit',!s.failed&&!s.pending);hide('pendingFilter',!s.pending);hide('failedFilter',!s.failed);text('failed',fmt(s.failed));text('pending',fmt(s.pending));
	 document.querySelector('.quality-row[data-filter="original-best"]').hidden=!s.original;document.querySelector('.quality-row[data-filter="preview-best"]').hidden=!s.only_preview;
 for(const [id,key] of [['hasOriginal','original'],['hasPreview','preview'],['hasThumbnail','thumbnail'],['noOriginal','no_original'],['noPreview','no_preview'],['noThumbnail','no_thumbnail']])text(id,fmt(s[key]));
 text('mediaSize',bytes(s.bytes));text('folderSize',d.storage?.updated?(d.storage.partial?'≈ ':'')+bytes(d.storage.folder_bytes):'Замер…');text('diskFree',d.storage?.updated?bytes(d.storage.volume_free_bytes):'—');hide('spaceLine',!d.storage?.updated);
 hide('rateLine',!active||paused);text('rateStatus',rateText(d));
 hide('watchLine',!d.watching);text('watchValue','каждые '+interval(d.watch_interval_minutes));text('watchNext',active?'Следующий запуск — после текущего прохода':'Следующий запуск '+dateTime(d.next_check)+' МСК');
 hide('stop',!active);hide('start',active||!d.cookies||s.total>0);hide('resume',active||!d.cookies||!s.total||s.done===s.total&&!missing&&s.inventory==='complete');hide('goSetup',d.cookies);
 for(const id of ['start','resume','stop','check','upgrade','zip','tune','rescan','watch'])$(id).disabled=mutation;
 const needsAccess=!d.cookies||d.phase==='attention'&&/сесси|CAPTCHA|cookies|авториз/i.test(d.message||'');
 hide('connectedAccess',needsAccess);hide('cookieForm',!needsAccess&&!replaceAccess);hide('replaceCookies',active);$('cookiesFile').disabled=active||mutation;hide('check',!d.cookies||active);hide('zipAction',active||!d.cookies||!s.no_original);hide('rescan',active||!d.cookies||s.inventory!=='incomplete');
 $('upgrade').disabled=active||!d.cookies||!missing||mutation;$('watch').disabled=!d.cookies||!s.total||!missing&&!d.watching||mutation;text('watch',d.watching?'Выключить':'Включить');
	 hide('upgrade',active||!d.cookies||!missing);hide('tune',!active);
	 $('watchStatus').closest('.panel').hidden=!s.total||!missing&&!d.watching;$('tuneStatus').closest('.panel').hidden=!s.total;$('settingsOut').closest('.panel').hidden=!s.total;
 if(document.activeElement!==$('interval'))$('interval').value=String(d.watch_interval_minutes);
 text('watchStatus',d.watching?active?'Включена · после завершения текущего прохода':'Включена · следующий запуск '+dateTime(d.next_check)+' МСК':'Выключена');
 $('tune').disabled=!active||mutation;text('tune',a?.state==='probing'?'Остановить подбор':'Подбирать RPS');
 text('tuneStatus',a?.state==='probing'?'Проверяется '+a.test_interval_ms+' мс · '+Math.round(a.elapsed_seconds)+' / '+a.step_seconds+' с':rateText(d)+(a?.state==='bounded'?' · разгон остановлен после 429':''));
 hide('rateHistory',!a?.trials?.length);text('tuneResults',(a?.trials||[]).map(row=>String(row.interval_ms).padStart(4)+' мс  '+fmt(row.rps,2).padStart(6)+' RPS  '+(row.limited?'429':'✓')).join('\n'));
 text('settingsOut',d.out);text('archiveCount',fmt(s.archive_files??s.archives)+(s.zip_archives?' · пакетных '+fmt(s.zip_archives):''));
 text('footerStatus',d.watching?'качество: авто · '+interval(d.watch_interval_minutes):'прогресс сохранён в SQLite');
 const filters={'original-best':s.original,'missing-original':s.no_original,'missing-preview':s.no_preview,'missing-thumbnail':s.no_thumbnail,'no-copy':s.total-s.done,failed:s.failed,pending:s.pending,'preview-best':s.only_preview,'thumbnail-best':s.only_thumbnail};
 for(const option of $('filter').options)option.hidden=option.value!=='all'&&filters[option.value]===0&&option.value!==$('filter').value;
 renderJournal();
 if(firstStatus){firstStatus=false;if(needsAccess)switchView('settings')}
}
async function refresh(){
 if(refreshing)return;refreshing=true;
 try{const d=await api('/api/status');render(d);if(view==='files'&&Date.now()-lastList>10000)await loadFiles()}
 catch(error){text('connection','● нет связи');$('connection').className='connection offline';text('notice','Нет связи с программой · данные не обновляются');hide('notice',false);for(const id of ['start','resume','stop','check','upgrade','zip','tune','rescan','watch'])$(id).disabled=true}
 finally{refreshing=false}
}
async function loadFiles(){
 if(view!=='files'||listLoading)return;listLoading=true;const filter=$('filter').value,pageOffset=offset;
 try{const d=await api('/api/files?filter='+encodeURIComponent(filter)+'&offset='+pageOffset+'&limit='+pageSize);if(filter!==$('filter').value||offset!==pageOffset){lastList=0;return}listTotal=d.total;
 const rows=d.files.map(file=>{const row=document.createElement('tr'),name=document.createElement('td'),title=document.createElement('strong'),created=document.createElement('small');title.textContent=file.name;name.title=[file.id,file.error].filter(Boolean).join('\n');created.textContent=file.created||'дата неизвестна';name.append(title,created);if(file.error){const error=document.createElement('small');error.className='file-error';error.textContent='ошибка · '+file.error;name.append(error)}row.append(name);for(const quality of ['original','preview','thumbnail']){const cell=document.createElement('td');cell.textContent=file[quality]?'✓':'·';cell.className=file[quality]?'present':'absent';cell.setAttribute('aria-label',file[quality]?'сохранено':'копии нет');row.append(cell)}return row});
 if(!rows.length){const row=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=4;cell.textContent='Нет файлов по этому фильтру.';row.append(cell);rows.push(row)}$('filesBody').replaceChildren(...rows);
 text('pageInfo',listTotal?fmt(pageOffset+1)+'–'+fmt(Math.min(pageOffset+pageSize,listTotal))+' / '+fmt(listTotal):'0 файлов');$('prevPage').disabled=offset===0;$('nextPage').disabled=offset+pageSize>=listTotal;lastList=Date.now();
 }catch(error){text('pageInfo',error.message);lastList=Date.now()}finally{listLoading=false;if(view==='files'&&(filter!==$('filter').value||offset!==pageOffset))setTimeout(loadFiles,0)}
}
async function action(path,body,json=false){
 if(mutation||!token)return;mutation=true;hide('alert');if(snapshot)render(snapshot);
 try{const headers={'X-Backup-Token':token};if(json)headers['Content-Type']='application/json';await api(path,{method:'POST',headers,body:json?JSON.stringify(body):body});replaceAccess=false;lastList=0;await refresh()}
 catch(error){showError(error.message)}finally{mutation=false;if(snapshot)render(snapshot)}
}
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>switchView(button.dataset.view)));
document.querySelectorAll('[data-open]').forEach(button=>button.addEventListener('click',()=>switchView(button.dataset.open)));
document.querySelectorAll('[data-filter]').forEach(button=>button.addEventListener('click',()=>{$('filter').value=button.dataset.filter;offset=0;lastList=0;switchView('files')}));
document.addEventListener('keydown',event=>{if(event.ctrlKey||event.altKey||event.metaKey||event.target.closest('input,select,textarea,[contenteditable=true]'))return;if(/[1-4]/.test(event.key)&&event.key.length===1){event.preventDefault();switchView(views[Number(event.key)-1],true)}else if(event.key.toLowerCase()==='r'){event.preventDefault();lastList=0;refresh()}else if(event.target.getAttribute('role')==='tab'&&['ArrowLeft','ArrowRight','Home','End'].includes(event.key)){event.preventDefault();const index=views.indexOf(view);switchView(event.key==='Home'?views[0]:event.key==='End'?views.at(-1):views[(index+(event.key==='ArrowLeft'?3:1))%4],true)}});
$('dismissAlert').addEventListener('click',()=>hide('alert'));
$('goSetup').addEventListener('click',()=>switchView('settings'));
$('replaceCookies').addEventListener('click',()=>{replaceAccess=true;hide('cookieForm',false);$('cookiesFile').focus()});
$('cookiesFile').addEventListener('change',async event=>{const file=event.target.files[0];if(!file)return;const body=new FormData();body.append('cookies',file);text('fileLabel',file.name);await action('/api/cookies',body)});
for(const [id,path] of [['start','start'],['rescan','start'],['stop','stop'],['check','check'],['upgrade','upgrade'],['resume','resume'],['zip','zip']])$(id).addEventListener('click',()=>action('/api/'+path));
$('watch').addEventListener('click',()=>action('/api/watch',{enabled:!snapshot?.watching,interval_minutes:Number($('interval').value)},true));
$('interval').addEventListener('change',()=>{if(snapshot?.watching)action('/api/watch',{enabled:true,interval_minutes:Number($('interval').value)},true)});
$('tune').addEventListener('click',()=>action('/api/rate-tune',{enabled:snapshot?.rate?.tune?.state!=='probing'},true));
$('filter').addEventListener('change',()=>{offset=0;lastList=0;loadFiles()});$('prevPage').addEventListener('click',()=>{offset=Math.max(0,offset-pageSize);loadFiles()});$('nextPage').addEventListener('click',()=>{offset+=pageSize;loadFiles()});
$('importantOnly').addEventListener('change',renderJournal);
$('speedUnit').value=remembered('unit','minute');if(!$('speedUnit').value)$('speedUnit').value='minute';$('speedUnit').addEventListener('change',()=>{store('unit',$('speedUnit').value);if(snapshot)renderSpeed(snapshot)});
switchView(remembered('view','overview'));refresh();setInterval(refresh,2000);
