const net         = require('net');
const fs          = require('fs');
const path        = require('path');
const { Worker }  = require('worker_threads');
const vm          = require('vm');

// ───── CONFIG ─────────────────────────────────────────────────────────────────
const PORT           = 6379;
const HOST           = '127.0.0.1';
const SNAPSHOT_MS    = 30_000;             // periodic RDB dump
const WHEEL_SIZE     = 60;                 // TTL wheel slots (1s granularity)
const AOF_PATH       = './appendonly.aof';
const RDB_PATH       = './dump.rdb';
const SLOWLOG_MAX    = 128;                
const SLOWLOG_TH_MS  = 1;                  // log commands >1 ms
let   MAXMEMORY      = 50 * 1024 * 1024;   // 50 MB cap

// ───── UTILITIES ───────────────────────────────────────────────────────────────
const nowMs     = () => Date.now();
let   startTime = nowMs();
const writeAOF  = cmd => aofWorker.postMessage(cmd);
const memUsage  = () => process.memoryUsage().rss;

// ───── STORAGE & EVICTION ───────────────────────────────────────────────────────
class TTLWheel {
  constructor(size) {
    this.buckets = Array.from({ length: size }, () => new Set());
    this.cursor  = 0;
    setInterval(() => this.tick(), 1000);
  }
  schedule(key, when) {
    const delay = Math.ceil((when - nowMs()) / 1000);
    const idx   = (this.cursor + (delay > 0 ? delay : 0)) % this.buckets.length;
    this.buckets[idx].add(key);
    const m = metadata.get(key) || {};
    m.expiresAt = when;
    metadata.set(key, m);
  }
  tick() {
    const b = this.buckets[this.cursor];
    for (let k of b) {
      const m = metadata.get(k);
      if (m && m.expiresAt <= nowMs()) {
        store.delete(k);
        metadata.delete(k);
      }
    }
    b.clear();
    this.cursor = (this.cursor + 1) % this.buckets.length;
  }
}

const store     = new Map();    // key → { type, value }
const metadata  = new Map();    // key → { expiresAt?, version }
const ttlWheel  = new TTLWheel(WHEEL_SIZE);

// ───── SLOWLOG, STATS, CLIENTS ─────────────────────────────────────────────────
const slowLog   = [];
const cmdStats  = new Map();
const clients   = new Map();
let   clientId  = 0;

// ───── RESP PARSER & ENCODER ───────────────────────────────────────────────────
class RESP {
  constructor(){ this.buf = Buffer.alloc(0); }
  ingest(chunk) {
    this.buf = Buffer.concat([ this.buf, Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk) ]);
    const cmds = [];
    while (this.buf[0] === 0x2a /* '*' */) {
      const end = this.buf.indexOf('\r\n');
      if (end < 0) break;
      const count = parseInt(this.buf.slice(1, end).toString(), 10);
      let offset = end + 2;
      const parts = [];
      for (let i = 0; i < count; i++) {
        if (this.buf[offset] !== 0x24 /* '$' */) { offset = this.buf.length; break; }
        const le = this.buf.indexOf('\r\n', offset);
        if (le < 0) return cmds;
        const len = parseInt(this.buf.slice(offset+1, le).toString(), 10);
        if (this.buf.length < le+2+len+2) return cmds;
        parts.push(this.buf.slice(le+2, le+2+len).toString());
        offset = le + 2 + len + 2;
      }
      if (parts.length === count) {
        cmds.push(parts);
        this.buf = this.buf.slice(offset);
      } else break;
    }
    return cmds;
  }
  static encode(x) {
    if (x === null)               return '$-1\r\n';
    if (x instanceof Error)       return `-${x.message}\r\n`;
    if (Array.isArray(x))         return `*${x.length}\r\n` + x.map(v=>RESP.encode(v)).join('');
    if (typeof x === 'string')    return `$${Buffer.byteLength(x)}\r\n${x}\r\n`;
    if (typeof x === 'number')    return `:${x}\r\n`;
    // auto-flatten maps for INFO:
    if (x instanceof Map) {
      const arr = [];
      for (let [k,v] of x) arr.push(k, String(v));
      return RESP.encode(arr);
    }
    return `-ERR unsupported type\r\n`;
  }
}

// ───── AOF WORKER ───────────────────────────────────────────────────────────────
const aofWorker = new Worker(`
  const fs = require('fs');
  process.on('message', cmd => {
    fs.appendFileSync('${AOF_PATH}', cmd + '\\r\\n');
  });
`, { eval: true });

// ───── RDB PRELOAD & SNAPSHOT ───────────────────────────────────────────────────
(function preload() {
  if (fs.existsSync(RDB_PATH)) {
    try {
      const obj = JSON.parse(fs.readFileSync(RDB_PATH));
      for (let k in obj) store.set(k, { type: 'string', value: obj[k] });
    } catch {}
  }
})();
function snapshot() {
  const obj = {};
  for (let [k,v] of store) if (v.type==='string') obj[k] = v.value;
  fs.writeFile(RDB_PATH, JSON.stringify(obj), ()=>{});
}
setInterval(snapshot, SNAPSHOT_MS);

// ───── COMMAND REGISTRY ────────────────────────────────────────────────────────
const commands = new Map();
function def(name, fn){ commands.set(name, fn); }

// ───── CORE KEYSPACE COMMANDS ─────────────────────────────────────────────────
def('ping',   () => 'PONG');
def('echo',   args => args.slice(1));
def('time',   () => {
  const t = nowMs();
  return [Math.floor(t/1000), (t%1000)*1000];
});
def('set',    args => {
  const [, key, val, opt, ttl] = args;
  store.set(key, { type: 'string', value: val });
  metadata.set(key, { version: (metadata.get(key)?.version||0)+1 });
  if (opt?.toLowerCase()==='px') ttlWheel.schedule(key, nowMs() + Number(ttl));
  writeAOF(RESP.encode(args));
  return 'OK';
});
def('get',    args => {
  const ent = store.get(args[1]);
  return ent && ent.type==='string' ? ent.value : null;
});
def('del',    args => {
  let cnt=0;
  for (let k of args.slice(1)) if (store.delete(k)) {
    metadata.delete(k);
    cnt++;
  }
  writeAOF(RESP.encode(args));
  return cnt;
});
def('flushall',() => {
  store.clear(); metadata.clear();
  writeAOF(RESP.encode(['flushall']));
  return 'OK';
});
def('keys',   () => Array.from(store.keys()));
def('config', args => {
  if (args[1]?.toLowerCase()==='get' && args[2]?.toLowerCase()==='maxmemory')
    return ['maxmemory', String(MAXMEMORY)];
  if (args[1]?.toLowerCase()==='set' && args[2]?.toLowerCase()==='maxmemory') {
    MAXMEMORY = Number(args[3]);
    return 'OK';
  }
  return null;
});
def('memory', args => args[1]==='usage' ? memUsage() : null);
def('info',   () => {
  const sec = new Map();
  sec.set('redis_version','4.0-genius');
  sec.set('uptime_in_seconds',((nowMs()-startTime)/1000).toFixed(0));
  sec.set('connected_clients', clients.size);
  sec.set('used_memory', memUsage());
  sec.set('keyspace_hits', cmdStats.get('get')||0);
  sec.set('keyspace_misses',(cmdStats.get('get_miss')||0));
  sec.set('db0', `keys=${store.size},expires=${[...metadata.values()].filter(m=>m.expiresAt).length}`);
  return sec;
});

// ───── DATA STRUCTURE COMMANDS ──────────────────────────────────────────────────
// Hashes
def('hset',   args => {
  const [, key, field, val] = args;
  let h = store.get(key);
  if (!h || h.type!=='hash') {
    h = new Map();
    store.set(key, { type:'hash', value: h });
  } else h = h.value;
  const added = h.has(field)?0:1;
  h.set(field, val);
  writeAOF(RESP.encode(args));
  return added;
});
def('hget',   args => {
  const h = store.get(args[1]);
  return h && h.type==='hash' ? h.value.get(args[2])||null : null;
});
def('hgetall',args => {
  const h = store.get(args[1]);
  if (!h||h.type!=='hash') return [];
  const out = [];
  for (let [f,v] of h.value) out.push(f,v);
  return out;
});
def('hdel',   args => {
  const h = store.get(args[1]);
  let cnt=0;
  if (h && h.type==='hash') {
    for (let f of args.slice(2)) if (h.value.delete(f)) cnt++;
  }
  writeAOF(RESP.encode(args));
  return cnt;
});

// Lists
def('lpush', (args) => {
  const [, key, ...vals] = args;
  let l = store.get(key);
  if (!l || l.type!=='list') {
    l = [];
    store.set(key, { type:'list', value: l });
  } else l = l.value;
  l.unshift(...vals);
  writeAOF(RESP.encode(args));
  return l.length;
});
def('rpush', args => {
  const [, key, ...vals] = args;
  let l = store.get(key);
  if (!l || l.type!=='list') {
    l = [];
    store.set(key, { type:'list', value: l });
  } else l = l.value;
  l.push(...vals);
  writeAOF(RESP.encode(args));
  return l.length;
});
def('lpop',  args => {
  const l = store.get(args[1]);
  if (l && l.type==='list') {
    const v = l.value.shift();
    writeAOF(RESP.encode(args));
    return v||null;
  }
  return null;
});
def('rpop',  args => {
  const l = store.get(args[1]);
  if (l && l.type==='list') {
    const v = l.value.pop();
    writeAOF(RESP.encode(args));
    return v||null;
  }
  return null;
});
def('lrange',(args) => {
  const [, key, start, stop] = args;
  const l = store.get(key);
  if (!l||l.type!=='list') return [];
  const s = parseInt(start), e = parseInt(stop);
  return l.value.slice(s < 0 ? l.value.length + s : s, e < 0 ? l.value.length + e + 1 : e + 1);
});

// Sets
def('sadd',  args => {
  const [, key, ...members] = args;
  let s = store.get(key);
  if (!s || s.type!=='set') {
    s = new Set();
    store.set(key,{type:'set',value:s});
  } else s = s.value;
  let added = 0;
  for (let m of members) if (!s.has(m)) { s.add(m); added++; }
  writeAOF(RESP.encode(args));
  return added;
});
def('smembers',args=>{
  const s = store.get(args[1]);
  return s && s.type==='set' ? Array.from(s.value) : [];
});
def('sismember',args=>{
  const s = store.get(args[1]);
  return s && s.type==='set' && s.value.has(args[2]) ? 1 : 0;
});
def('srem',   args=>{
  const s = store.get(args[1]);
  let rem=0;
  if (s&&s.type==='set') {
    for (let m of args.slice(2)) if (s.value.delete(m)) rem++;
  }
  writeAOF(RESP.encode(args));
  return rem;
});

// Bitmaps
def('setbit', args=>{
  const [, key, off, bit] = args;
  let b = store.get(key);
  if (!b || b.type!=='string') {
    b = '';
    store.set(key,{type:'string',value:''});
  } else b = b.value;
  const idx = parseInt(off), vb = bit==='1';
  const arr = Buffer.from(b);
  if (idx >= arr.length*8) arr.fill(0, arr.length, Math.ceil((idx+1)/8));
  const byteIdx = Math.floor(idx/8), bitIdx = idx%8;
  const old = ((arr[byteIdx]>> (7-bitIdx))&1)===1;
  if (vb) arr[byteIdx] |= (1 << (7-bitIdx));
  else arr[byteIdx] &= ~(1 << (7-bitIdx));
  const str=arr.toString();
  store.set(key,{type:'string',value:str});
  writeAOF(RESP.encode(args));
  return old?1:0;
});
def('getbit', args=>{
  const [, key, off] = args;
  const b = store.get(key);
  if (!b||b.type!=='string') return 0;
  const arr = Buffer.from(b.value);
  const idx = parseInt(off), byteIdx = Math.floor(idx/8), bitIdx = idx%8;
  if (byteIdx>=arr.length) return 0;
  return (arr[byteIdx]>>(7-bitIdx))&1;
});
def('bitcount', args=>{
  const b = store.get(args[1]);
  if (!b||b.type!=='string') return 0;
  return [...Buffer.from(b.value)].reduce((sum,byte)=>
    sum + ((byte.toString(2).match(/1/g)||[]).length), 0
  );
});

// Transactions
const txnQ = new Map();
def('multi', () => { txnQ.set(currentConn, []); return 'OK'; });
def('exec',  () => {
  const q = txnQ.get(currentConn)||[];
  txnQ.delete(currentConn);
  return q.map(cmd => dispatch(cmd, currentConn));
});

// Scripting (JS)
def('eval', args=>{
  const [, script, num] = args;
  const cnt = parseInt(num,10);
  const keys = args.slice(3, 3+cnt);
  const argv = args.slice(3+cnt);
  const sandbox = { KEYS: keys, ARGV: argv, result: null, console };
  vm.createContext(sandbox);
  new vm.Script(`result = (function(){ ${script} })();`).runInContext(sandbox);
  writeAOF(RESP.encode(args));
  return sandbox.result;
});

// Client & Slowlog
def('client', args=>{
  if (args[1]==='list') {
    return [...clients.values()].map(c=>
      `id=${c.id} addr=${c.addr} age=${Math.floor((nowMs()-c.start)/1000)}`
    );
  }
  if (args[1]==='kill') {
    const id = Number(args[2]);
    for (let [conn,info] of clients) {
      if (info.id === id) { conn.destroy(); return 1; }
    }
    return 0;
  }
});
def('slowlog', args=>{
  if (args[1]==='get') {
    const n = args[2] ? Number(args[2]) : SLOWLOG_MAX;
    return slowLog.slice(0,n).map(e=>[e.ts, e.dur, e.cmd]);
  }
  if (args[1]==='reset') {
    slowLog.length=0;
    return 'OK';
  }
});

// LRU Eviction on Memory Limit
function evictIfNeeded() {
  while (memUsage() > MAXMEMORY) {
    // simplest: remove oldest metadata entry
    const oldestKey = metadata.keys().next().value;
    if (!oldestKey) break;
    store.delete(oldestKey);
    metadata.delete(oldestKey);
  }
}

// ───── DISPATCH & SERVER ───────────────────────────────────────────────────────
let currentConn = null;

function dispatch(cmd, conn) {
  currentConn = conn;
  const name = cmd[0].toLowerCase();
  const fn   = commands.get(name);
  if (!fn) return new Error(`ERR unknown command '${name}'`);
  try { return fn(cmd); }
  catch (e) { return new Error(`ERR ${e.message}`); }
}

function handle(cmd, conn, parser) {
  const start = nowMs();
  let res;
  if (txnQ.has(conn) && cmd[0].toLowerCase()!=='exec') {
    txnQ.get(conn).push(cmd);
    res = 'QUEUED';
  } else {
    res = dispatch(cmd, conn);
  }
  const dur = nowMs() - start;
  if (dur > SLOWLOG_TH_MS) {
    slowLog.unshift({ ts: start, dur, cmd });
    if (slowLog.length > SLOWLOG_MAX) slowLog.pop();
  }
  cmdStats.set(cmd[0], (cmdStats.get(cmd[0])||0)+1);
  evictIfNeeded();
  conn.write(RESP.encode(res));
}

const server = net.createServer(conn => {
  const id = ++clientId;
  clients.set(conn, { id, addr: conn.remoteAddress, start: nowMs() });
  const parser = new RESP();
  conn.on('data', data => {
    for (let cmd of parser.ingest(data)) handle(cmd, conn, parser);
  });
  conn.on('close', () => clients.delete(conn));
});

server.listen(PORT, HOST, () => {
  console.log(`🚀 InsaneRedis 4.0 listening on ${HOST}:${PORT}`);
});
