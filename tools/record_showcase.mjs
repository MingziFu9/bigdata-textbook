// 录制首页“先玩一玩”轮播短视频：按步骤操作动画，同时画出模拟鼠标指针，逐帧截图后用 ffmpeg 合成 MP4/WebM 和封面图
// 用法：node record_showcase.mjs <任务.json> <输出目录> [只录的动画名,逗号分隔]
// 任务.json：{"base":"http://127.0.0.1:8000","items":[{"name":"sam-click","url":"sims/sam-click/main.html","vw":1100,"steps":[...]}]}
import { chromium } from 'playwright';
import fs from 'node:fs'; import path from 'node:path'; import { execFileSync } from 'node:child_process';

const [,, JOB, OUT, ONLY] = process.argv;
const job = JSON.parse(fs.readFileSync(JOB, 'utf8'));
const FPS = 20, DT = 1 / FPS;
const CURSOR = `<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 28 28"><path d="M4 2 L4 22 L9.5 17 L13 25 L16.5 23.5 L13 15.8 L20.5 15.8 Z" fill="#111" stroke="#fff" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
const ease = t => t < .5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;

async function overlay(p) {
  await p.evaluate(svg => {
    const c = document.createElement('div'); c.id = '__cur'; c.innerHTML = svg;
    Object.assign(c.style, { position: 'fixed', left: '-40px', top: '-40px', zIndex: 2147483647, pointerEvents: 'none', transform: 'translate(-4px,-2px)' });
    const r = document.createElement('div'); r.id = '__rip';
    Object.assign(r.style, { position: 'fixed', width: '34px', height: '34px', marginLeft: '-17px', marginTop: '-17px', borderRadius: '50%', background: 'rgba(83,109,254,.35)', border: '2px solid rgba(83,109,254,.9)', zIndex: 2147483646, pointerEvents: 'none', opacity: '0' });
    document.documentElement.append(c, r);
    // 隐藏滚动条，画面更干净
    const s = document.createElement('style'); s.textContent = 'html{scrollbar-width:none}html::-webkit-scrollbar{display:none}'; document.head.append(s);
  }, CURSOR);
}

async function record(b, it) {
  const vw = it.vw || 1100, vh = it.vh || Math.round(vw * 9 / 16);   // vh 比 16:9 高时，成片左右留白
  const ctx = await b.newContext({ viewport: { width: vw, height: vh }, deviceScaleFactor: it.dsf || 1.5 });
  const p = await ctx.newPage();
  await p.goto(`${job.base}/${it.url}`); await p.waitForTimeout(it.load || 1200);
  if (it.zoom) await p.evaluate(z => { document.documentElement.style.zoom = z }, it.zoom);
  await overlay(p);
  const dir = path.join(OUT, it.name); fs.rmSync(dir, { recursive: true, force: true }); fs.mkdirSync(dir, { recursive: true });
  let n = 0, list = [], x = vw * .62, y = vh * .55, poster = null;
  const frame = async (d = DT) => { const f = path.join(dir, String(n++).padStart(5, '0') + '.jpg'); await p.screenshot({ path: f, type: 'jpeg', quality: 92 }); list.push([f, d]); };
  const cur = async (nx, ny) => { x = nx; y = ny; await p.evaluate(([a, c]) => { const e = document.getElementById('__cur'); e.style.left = a + 'px'; e.style.top = c + 'px' }, [x, y]); };
  const move = async (tx, ty, sec = .55, drag = false) => {
    const k = Math.max(2, Math.round(sec * FPS)), x0 = x, y0 = y;
    for (let i = 1; i <= k; i++) { const e = ease(i / k), nx = x0 + (tx - x0) * e, ny = y0 + (ty - y0) * e; await p.mouse.move(nx, ny); await cur(nx, ny); if (drag) await p.waitForTimeout(25); await frame(); }
  };
  const ripple = async (on) => p.evaluate(([a, c, o]) => { const r = document.getElementById('__rip'); r.style.left = a + 'px'; r.style.top = c + 'px'; r.style.opacity = o ? '1' : '0' }, [x, y, on]);
  const live = async (sec) => { const t0 = Date.now(); let last = t0; while (Date.now() - t0 < sec * 1000) { await frame(0); const now = Date.now(); list[list.length - 1][1] = Math.max(DT, (now - last) / 1000); last = now; } };
  const hold = async (sec) => sec > 0 && live(sec);   // 停留时也实时截帧，动画中的过渡效果不会丢
  const scrollTo = async (ty, sec = .6) => {
    const y0 = await p.evaluate(() => scrollY), max = await p.evaluate(() => document.documentElement.scrollHeight - innerHeight);
    ty = Math.max(0, Math.min(max, ty)); if (Math.abs(ty - y0) < 4) return;
    const k = Math.max(2, Math.round(sec * FPS));
    for (let i = 1; i <= k; i++) { await p.evaluate(v => scrollTo(0, v), y0 + (ty - y0) * ease(i / k)); await frame(); }
  };
  const loc = (t) => {
    if (t.role) return p.getByRole(t.role, { name: t.name, exact: t.exact !== false }).nth(t.nth || 0);
    if (t.text) return p.getByText(t.text, { exact: !!t.exact }).nth(t.nth || 0);
    return p.locator(t.sel).nth(t.nth || 0);
  };
  const box = async (l) => {   // 目标不在画面里就先平滑滚动过去
    await l.waitFor({ state: 'visible', timeout: 4000 });
    let bb = await l.boundingBox();
    if (bb.y < 8 || bb.y + Math.min(bb.height, vh * .5) > vh - 8) { const sy = await p.evaluate(() => scrollY); await scrollTo(sy + bb.y - vh * .35); bb = await l.boundingBox(); }
    return bb;
  };
  const tgt = (s) => typeof s === 'string' ? (s.startsWith('#') || s.startsWith('.') || s.includes('[') || s.includes('>>') ? { sel: s } : { role: 'button', name: s }) : s;
  const clickAt = async (tx, ty, o = {}) => {
    await move(tx, ty, o.sec || .5); await ripple(true); await frame(.06);
    await p.mouse.click(tx, ty); await p.waitForTimeout(o.wait ?? 120); await ripple(false); await hold(o.hold ?? .7);
  };
  const OPS = {
    async hold(sec) { await hold(sec); },
    async live(sec) { await hold(sec); },
    async poster() { poster = list.length ? list[list.length - 1][0] : null; },
    async cursor(fx, fy) { await cur(vw * fx, vh * fy); },
    async move(fx, fy, sec = .6) { await move(vw * fx, vh * fy, sec); },
    async scroll(ty, sec = .8) { await scrollTo(ty, sec); },
    async click(t, o = {}) {           // 点按钮或元素，可重复 n 次
      const l = loc(tgt(t));
      for (let i = 0; i < (o.n || 1); i++) { const bb = await box(l); await clickAt(bb.x + bb.width * (o.fx ?? .5), bb.y + bb.height * (o.fy ?? .5), { ...o, sec: i ? .25 : .5, hold: i < (o.n || 1) - 1 ? (o.gap ?? .45) : o.hold }); }
    },
    async at(t, fx, fy, o = {}) { const bb = await box(loc(tgt(t))); await clickAt(bb.x + bb.width * fx, bb.y + bb.height * fy, o); },
    async slide(t, to, o = {}) {       // 拖动滑块到某个值
      const l = loc(tgt(t)); const bb = await box(l);
      const [mn, mx, v] = await l.evaluate(e => [+e.min, +e.max, +e.value]);
      const X = val => bb.x + 8 + (bb.width - 16) * (val - mn) / (mx - mn), Y = bb.y + bb.height / 2;
      await move(X(v), Y, .5); await p.mouse.down(); await ripple(true); await frame(.06);
      await move(X(to), Y, o.sec || 1.2, true); await p.mouse.up(); await ripple(false);
      await l.evaluate((e, val) => { if (Math.abs(+e.value - val) > (+e.step || 1)) { e.value = val; e.dispatchEvent(new Event('input', { bubbles: true })); e.dispatchEvent(new Event('change', { bubbles: true })); } }, to);
      await hold(o.hold ?? .7);
    },
    async select(t, label, o = {}) { const l = loc(tgt(t)); const bb = await box(l); await move(bb.x + bb.width * .5, bb.y + bb.height * .5, .5); await ripple(true); await frame(.06); await l.selectOption({ label }); await p.waitForTimeout(150); await ripple(false); await hold(o.hold ?? .7); },
    async type(t, text, o = {}) {
      const l = loc(tgt(t)); const bb = await box(l); await clickAt(bb.x + Math.min(40, bb.width / 2), bb.y + bb.height / 2, { hold: .15 });
      if (o.clear) { await l.fill(''); await frame(.1); }
      for (const ch of text) { if (ch === '\n') { await p.keyboard.press('Enter'); await hold(o.enterHold ?? .9); } else { await p.keyboard.type(ch); await frame(.06); } }
      await hold(o.hold ?? .5);
    },
    async draw(t, pts, o = {}) {       // 在画布上按住拖动，pts 是相对坐标
      const bb = await box(loc(tgt(t))); const P = ([fx, fy]) => [bb.x + bb.width * fx, bb.y + bb.height * fy];
      await move(...P(pts[0]), .5); await p.mouse.down(); await ripple(true); await frame(.05); await ripple(false);
      for (const q of pts.slice(1)) await move(...P(q), o.seg || .18, true);
      await p.mouse.up(); await hold(o.hold ?? .8);
    },
    async js(code) { await p.evaluate(code); await frame(.05); },
  };
  await cur(x, y); await hold(it.intro ?? .6);
  for (const s of it.steps) { const [op, ...a] = s; if (!OPS[op]) throw new Error('未知步骤 ' + op); await OPS[op](...a); }
  await hold(it.outro ?? .4);
  await ctx.close();
  const txt = list.map(([f, d]) => `file '${path.resolve(f)}'\nduration ${d.toFixed(3)}`).join('\n') + `\nfile '${path.resolve(list.at(-1)[0])}'\n`;
  fs.writeFileSync(path.join(dir, 'list.txt'), txt);
  const sec = list.reduce((a, [, d]) => a + d, 0);
  const base = path.join(OUT, it.name);
  const vf = 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2:white,fps=20,format=yuv420p';
  execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-f', 'concat', '-safe', '0', '-i', path.join(dir, 'list.txt'), '-vf', vf, '-c:v', 'libx264', '-preset', 'slow', '-crf', '27', '-movflags', '+faststart', '-an', base + '.mp4']);
  execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-i', base + '.mp4', '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '40', '-row-mt', '1', '-an', base + '.webm']);
  const pf = poster || list[Math.floor(list.length * .6)][0];
  execFileSync('ffmpeg', ['-y', '-loglevel', 'error', '-i', pf, '-vf', 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2:white', '-q:v', '4', base + '.jpg']);
  if (!process.env.KEEP_FRAMES) fs.rmSync(dir, { recursive: true, force: true });
  const kb = f => Math.round(fs.statSync(f).size / 1024);
  return { name: it.name, sec: +sec.toFixed(1), mp4: kb(base + '.mp4'), webm: kb(base + '.webm'), jpg: kb(base + '.jpg') };
}

const b = await chromium.launch();
const only = ONLY ? ONLY.split(',') : null, res = [];
for (const it of job.items) {
  if (only && !only.includes(it.name)) continue;
  try { const r = await record(b, it); res.push(r); console.log(JSON.stringify(r)); }
  catch (e) { console.log(JSON.stringify({ name: it.name, error: e.message.split('\n')[0] })); res.push({ name: it.name, error: e.message }); }
}
await b.close();
fs.writeFileSync(path.join(OUT, 'result.json'), JSON.stringify(res, null, 1));
