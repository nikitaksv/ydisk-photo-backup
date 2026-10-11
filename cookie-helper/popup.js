document.getElementById('export').addEventListener('click',async()=>{
 const button=document.getElementById('export'),status=document.getElementById('status');button.disabled=true;
 try{
  const groups=await Promise.all(['yandex.ru','yandex.com'].map(domain=>chrome.cookies.getAll({domain})));
  const cookies=groups.flat().map(c=>({name:c.name,value:c.value,domain:c.domain,path:c.path,secure:c.secure,httpOnly:c.httpOnly,hostOnly:c.hostOnly,expirationDate:c.expirationDate,session:c.session,sameSite:c.sameSite}));
  if(!cookies.some(c=>['Session_id','sessionid2'].includes(c.name)))throw new Error('Не найдены cookies входа. Войди в Яндекс Диск в этом браузере и повтори экспорт.');
  const b=new Blob([JSON.stringify(cookies,null,2)],{type:'application/json'}),url=URL.createObjectURL(b);
  try{await chrome.downloads.download({url,filename:'cookies.json',saveAs:true});status.textContent='Сохранение начато. Выбери cookies.json в локальном интерфейсе программы.'}finally{setTimeout(()=>URL.revokeObjectURL(url),60000)}
 }catch(e){status.textContent=e.message}finally{button.disabled=false}
});
