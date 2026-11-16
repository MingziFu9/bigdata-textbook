// 首页“先玩一玩”轮播：把 tools/showcase.py 生成的卡片列表变成轮播。
// 每张播放一段无声短视频（没有视频就显示封面图），点“就在这里试玩”原地加载真实动画。
// 读者还没动过时自动切换：视频播完一遍换下一张，图片停 7 秒；读者点过任何一处或转完一圈后不再自动切换。
(function () {
  function build(root) {
    if (root.dataset.ready) return;
    root.dataset.ready = "1";
    var items = [].slice.call(root.querySelectorAll(".sc-list > li")).map(function (li) {
      return {
        sim: li.dataset.sim, video: li.dataset.video, poster: li.querySelector("img").getAttribute("src"),
        title: li.dataset.title, hook: li.querySelector(".sc-hook").textContent,
        play: li.dataset.play, chap: li.querySelector("a.sc-chap")
      };
    });
    if (!items.length) return;
    var reduce = window.matchMedia && matchMedia("(prefers-reduced-motion: reduce)").matches;
    var saveData = !!(navigator.connection && navigator.connection.saveData);
    var autoVideo = !reduce && !saveData;   // 系统关了动画效果或开了省流量：仍给视频，但不自动播放，读者点 ▶ 再看
    var i = 0, auto = autoVideo, steps = 0, hover = false, live = false, timer = null, video = null;

    function el(tag, cls, html) { var e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; }
    var stage = el("div", "sc-stage"), media = el("div", "sc-media"), img = el("img");
    img.alt = ""; media.appendChild(img);
    var prev = el("button", "sc-arrow prev", "‹"), next = el("button", "sc-arrow next", "›");
    prev.type = next.type = "button"; prev.setAttribute("aria-label", "上一个"); next.setAttribute("aria-label", "下一个");
    media.appendChild(prev); media.appendChild(next);
    var cap = el("div", "sc-cap"), txt = el("div", "sc-cap-txt"), hook = el("p", "sc-hook"), desc = el("p", "sc-desc");
    txt.appendChild(hook); txt.appendChild(desc);
    var act = el("div", "sc-act"), tryBtn = el("button", "sc-try", "▶ 就在这里试玩"), chap = el("a", "sc-chap");
    tryBtn.type = "button"; act.appendChild(tryBtn); act.appendChild(chap);
    cap.appendChild(txt); cap.appendChild(act); cap.setAttribute("aria-live", "polite");
    stage.appendChild(media); stage.appendChild(cap);
    var tabs = el("div", "sc-tabs"); tabs.setAttribute("role", "tablist");
    items.forEach(function (it, k) {
      var t = el("button", "sc-tab", '<span class="no">' + (k + 1) + "</span>" + it.title + '<i class="prog"></i>');
      t.type = "button"; t.setAttribute("role", "tab");
      t.onclick = function () { stop(); go(k); };
      tabs.appendChild(t);
    });
    var list = root.querySelector(".sc-list");
    root.insertBefore(stage, list); root.insertBefore(tabs, list);
    root.classList.add("on");

    function stop() { auto = false; root.classList.remove("auto"); clearTimeout(timer); if (video) video.loop = true; }
    function schedule() {
      clearTimeout(timer);
      if (!auto || live) return;
      if (video) return;                       // 视频在 ended 事件里切换
      timer = setTimeout(function tick() { if (hover) { timer = setTimeout(tick, 400); return; } advance(); }, 7000);
    }
    function advance() {
      steps++;
      if (steps >= items.length) { stop(); go(0); return; }
      go(i + 1);
    }
    function go(k) {
      closeLive();
      i = (k + items.length) % items.length;
      var it = items[i];
      img.src = it.poster; img.alt = it.title + "截图";
      if (video) { video.remove(); video = null; }
      if (it.video) {
        video = el("video");
        video.muted = true; video.playsInline = true; video.autoplay = autoVideo; video.loop = !auto;
        video.setAttribute("muted", ""); video.setAttribute("playsinline", ""); video.preload = autoVideo ? "auto" : "metadata";
        if (!autoVideo) video.controls = true;
        video.poster = it.poster; video.setAttribute("aria-label", it.title + "操作演示（无声）");
        video.innerHTML = '<source src="' + it.video + '.webm" type="video/webm"><source src="' + it.video + '.mp4" type="video/mp4">';
        video.addEventListener("ended", function () { if (auto && !live) { if (hover) { video.play(); return; } advance(); } });
        video.addEventListener("loadedmetadata", function () { if (video) root.style.setProperty("--sc-dur", video.duration + "s"); });
        media.insertBefore(video, prev);
        if (autoVideo) {
          var v = video, pr = v.play();
          // 浏览器拦了自动播放：显示播放按钮，读者点一下就能看
          if (pr && pr.catch) pr.catch(function () { v.controls = true; stop(); });
        }
      } else {
        root.style.setProperty("--sc-dur", "7s");
      }
      hook.textContent = it.hook;
      desc.innerHTML = "<b></b>　"; desc.querySelector("b").textContent = it.title; desc.appendChild(document.createTextNode(it.play));
      if (it.chap) { chap.href = it.chap.getAttribute("href"); chap.textContent = it.chap.textContent; chap.title = it.chap.title; chap.hidden = false; } else chap.hidden = true;
      [].forEach.call(tabs.children, function (t, j) {
        t.setAttribute("aria-selected", j === i ? "true" : "false");
        var p = t.querySelector(".prog"); p.style.animation = "none"; void p.offsetWidth; p.style.animation = "";
      });
      schedule();
    }
    function openLive() {
      stop(); live = true;
      var box = el("div", "sc-live", '<button type="button" class="sc-close">返回轮播 ✕</button><iframe title="交互动画" loading="lazy"></iframe>');
      box.querySelector("iframe").src = items[i].sim;
      box.querySelector(".sc-close").onclick = function (e) { e.stopPropagation(); closeLive(); };
      media.appendChild(box); stage.classList.add("live");
      if (video) video.pause();
    }
    function closeLive() {
      var b = media.querySelector(".sc-live"); if (b) b.remove();
      stage.classList.remove("live");
      if (live && video && !video.controls) video.play();
      live = false;
    }
    tryBtn.onclick = openLive;
    media.addEventListener("click", function (e) { if (!live && !e.target.closest("button") && !(video && video.controls && e.target === video)) openLive(); });
    prev.onclick = function () { stop(); go(i - 1); };
    next.onclick = function () { stop(); go(i + 1); };
    root.addEventListener("mouseenter", function () { hover = true; root.classList.add("paused"); });
    root.addEventListener("mouseleave", function () { hover = false; root.classList.remove("paused"); });
    root.addEventListener("keydown", function (e) {
      if (e.target.closest && e.target.closest("iframe")) return;
      if (e.key === "ArrowLeft") { stop(); go(i - 1); } else if (e.key === "ArrowRight") { stop(); go(i + 1); }
    });
    var x0 = null;
    media.addEventListener("touchstart", function (e) { x0 = e.touches[0].clientX; }, { passive: true });
    media.addEventListener("touchend", function (e) {
      if (x0 === null || live) return; var dx = e.changedTouches[0].clientX - x0; x0 = null;
      if (Math.abs(dx) > 40) { stop(); go(i + (dx < 0 ? 1 : -1)); }
    });
    if (auto) root.classList.add("auto");
    go(0);
  }
  function init() { document.querySelectorAll(".showcase").forEach(build); }
  if (window.document$ && window.document$.subscribe) window.document$.subscribe(init);
  else if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
