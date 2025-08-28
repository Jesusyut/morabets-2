from flask import Blueprint, Response
import datetime

evdebug_bp = Blueprint("evdebug", __name__)

HTML = """<!doctype html><meta charset="utf-8"/><title>EV Debug</title>
<pre id="s">Loading…</pre><script>
const d=new Date(),mm=String(d.getMonth()+1).padStart(2,'0'),dd=String(d.getDate()).padStart(2,'0');
const date=`${d.getFullYear()}-${mm}-${dd}`;
async function run(){
  async function j(url){ const r=await fetch(url); if(!r.ok) throw new Error(url+': '+r.status); return r.json(); }
  let data=null, tried=[];
  try { tried.push('/api/ev-plays'); data = await j(`/api/ev-plays?league=mlb&date=${date}&novig=1`); }
  catch(e1){
    try { tried.push('/api/ev-plays-simple'); data = await j(`/api/ev-plays-simple?league=mlb&date=${date}`); }
    catch(e2){ document.getElementById('s').textContent='Failed: '+tried.join(' then ')+'\\n'+e2; return; }
  }
  const props=(data.props||[]).length, lines=(data.lines||[]).length;
  document.getElementById('s').textContent = JSON.stringify({date, props, lines, tried}, null, 2);
}
run();
</script>"""

@evdebug_bp.get("/ev-debug")
def evdebug():
    return Response(HTML, mimetype="text/html") 