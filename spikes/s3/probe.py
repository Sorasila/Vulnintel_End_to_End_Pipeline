"""Spike S3: ทดสอบข้อจำกัด 1 active pipeline ต่อประเภทด้วย Databricks SDK (รันบน GitHub runner).

ใช้: python spikes/s3/probe.py --case {all,parallel,seq,seq_during}
  parallel    Step 2: start_update ของ spike_s3_a และ spike_s3_b พร้อมกัน แล้วดูว่าตัวที่สอง queue/reject/รันซ้อน
  seq         Step 3: run job spike_s3_seq (run_a -> run_b) ต้องสำเร็จทั้ง 2 task
  seq_during  Step 4: start_update spike_s3_a แล้วสั่ง job spike_s3_seq ทันที ดูว่า task รอ/fail/retry
  all         รันทั้ง 3 case ตามลำดับ

ต้อง deploy bundle (spikes/s3/databricks.yml) ก่อน: หา pipeline/job จากชื่อ
auth: env DATABRICKS_HOST / DATABRICKS_AUTH_TYPE=oauth-m2m / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET
ผลพิมพ์เป็นบรรทัด "RESULT ..." (workflow ดึงขึ้น Job summary) ไม่พิมพ์ secret และ host
ทุก loop มี deadline และจบด้วยการ stop pipeline / cancel run ที่ยังค้าง (best effort)

exit 0 เมื่อสังเกตผลครบ (reject/queue ไม่ใช่ความล้ม), 1 เมื่อ step 3 ไม่ผ่าน, เกิด error หรือ timeout
"""

import argparse
import itertools
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

PIPELINE_NAMES = {"A": "spike_s3_a", "B": "spike_s3_b"}
JOB_NAME = "spike_s3_seq"

SDK_HTTP_TIMEOUT_S = 20
SDK_RETRY_TIMEOUT_S = 30
POLL_S = 5
IDLE_DEADLINE_S = 300  # รอให้ pipeline ว่างก่อนเริ่มแต่ละ case
PIPELINE_DEADLINE_S = 600
JOB_DEADLINE_S = 960  # job ตั้ง timeout_seconds 900 ไว้ใน bundle
TOTAL_BUDGET_S = 35 * 60  # ต้องน้อยกว่า timeout-minutes ของ step ใน workflow
ERR_TRUNC = 300

TERMINAL_UPDATE = {"COMPLETED", "FAILED", "CANCELED"}
TERMINAL_RUN = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
# state ที่นับว่า update "กำลังทำงาน" (ไม่ใช่รอคิว/รอ resource และยังไม่จบ) ใช้ตัดสินว่ารันซ้อนกันไหม
ACTIVE_UPDATE = {"INITIALIZING", "SETTING_UP_TABLES", "RUNNING", "RESETTING"}
PIPELINE_BUSY = {"RUNNING", "STARTING", "RECOVERING", "RESETTING", "DEPLOYING"}


# ---------- pure functions (timeline + classification) ----------


def scrub(text) -> str:
    """ตัดค่า host (มี/ไม่มี scheme) และ credential ออกจากข้อความ error ก่อนพิมพ์."""
    out = str(text)
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    secrets = [host, host.rstrip("/"), host.removeprefix("https://").removeprefix("http://").rstrip("/")]
    secrets += [os.environ.get("DATABRICKS_CLIENT_ID", ""), os.environ.get("DATABRICKS_CLIENT_SECRET", "")]
    for value in sorted({v for v in secrets if len(v) >= 4}, key=len, reverse=True):
        out = out.replace(value, "<redacted>")
    return out


def trunc(text, n: int = ERR_TRUNC) -> str:
    s = " ".join(scrub(text).split())
    return s if len(s) <= n else s[: n - 3] + "..."


def state_name(state) -> str | None:
    if state is None:
        return None
    return getattr(state, "value", str(state))


def add_state(timeline: list, t: float, state: str | None) -> None:
    """บันทึกเฉพาะตอนที่ state เปลี่ยน: timeline = [(t_วินาทีสัมพัทธ์, state), ...]."""
    if state is not None and (not timeline or timeline[-1][1] != state):
        timeline.append((round(t, 1), state))


def fmt_timeline(timeline: list) -> str:
    return " -> ".join(f"{t:g}s:{s}" for t, s in timeline) if timeline else "(none)"


def windows(timeline: list, states, end_t: float) -> list:
    """ช่วงเวลา [start, end) ที่อยู่ใน state นี้ (str หรือ set ของ state) ช่วงสุดท้ายปิดที่ end_t."""
    wanted = {states} if isinstance(states, str) else set(states)
    out = []
    for i, (t, s) in enumerate(timeline):
        if s in wanted:
            end = timeline[i + 1][0] if i + 1 < len(timeline) else end_t
            out.append((t, max(end, t)))
    return out


def overlap_seconds(wins_a: list, wins_b: list) -> float:
    return sum(max(0.0, min(a1, b1) - max(a0, b0)) for a0, a1 in wins_a for b0, b1 in wins_b)


def terminal_time(timeline: list, end_t: float) -> float:
    for t, s in timeline:
        if s in TERMINAL_UPDATE:
            return t
    return end_t


@dataclass
class UpdateTrack:
    label: str  # "A" | "B"
    pipeline_id: str
    start_error: str | None = None
    update_id: str | None = None
    t_called: float | None = None
    timeline: list = field(default_factory=list)
    end_t: float = 0.0
    done: bool = False


def classify(a: UpdateTrack, b: UpdateTrack) -> tuple[str, str]:
    """ตัดสินว่า update 2 ตัวที่สั่งพร้อมกัน: reject / รันซ้อน / รันต่อกัน / ไม่ได้รัน.

    เทียบเฉพาะช่วงที่ state อยู่ใน ACTIVE_UPDATE (RUNNING, INITIALIZING, SETTING_UP_TABLES, RESETTING)
    ทุก track ใช้ timestamp เดียวกันต่อรอบ poll (ดู observe) ซ้อนกันต้องนานอย่างน้อย POLL_S วินาทีจึงเป็น CONCURRENT
    ซ้อนกันสั้นกว่านั้น (0 < ov < POLL_S) คือเห็นแค่ว่าติดกันในรอบ poll เดียว ตัดสินไม่ได้ จึงรายงาน SERIAL_OR_ADJACENT
    """
    rejected = [t.label for t in (a, b) if t.start_error]
    if len(rejected) == 2:
        return "BOTH_REJECTED_AT_START", "start_update raised for A and B"
    if rejected:
        return f"{rejected[0]}_REJECTED_AT_START", "start_update raised for " + rejected[0]
    wa = windows(a.timeline, ACTIVE_UPDATE, a.end_t)
    wb = windows(b.timeline, ACTIVE_UPDATE, b.end_t)
    if not wa or not wb:
        never = [t.label for t, w in ((a, wa), (b, wb)) if not w]
        finals = {t.label: (t.timeline[-1][1] if t.timeline else "NONE") for t in (a, b)}
        return "NEVER_RAN_" + "_".join(never), f"never active (RUNNING/INITIALIZING/SETTING_UP_TABLES): {never}; last states={finals}"
    ov = overlap_seconds(wa, wb)
    if ov >= POLL_S - 0.5:  # เผื่อการปัดเวลา 0.1 s ใน add_state
        return "CONCURRENT", f"overlap_active_s={ov:g}"
    first, second = (a, b) if wa[0][0] <= wb[0][0] else (b, a)
    second_active = (wb if second is b else wa)[0][0]
    first_end = terminal_time(first.timeline, first.end_t)
    verdict = "SERIAL" if ov <= 0 else "SERIAL_OR_ADJACENT"
    return (
        verdict,
        f"first={first.label} ended_at={first_end:g}s overlap_active_s={ov:g} "
        f"{second.label}_first_active_at={second_active:g}s",
    )


# ---------- Databricks helpers ----------


def result(msg: str) -> None:
    print(f"RESULT {msg}", flush=True)


def lookup_ids(w: WorkspaceClient) -> tuple[dict, int]:
    pipelines = {}
    for label, name in PIPELINE_NAMES.items():
        hits = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'") if p.name == name]
        if len(hits) != 1:
            raise RuntimeError(f"expected exactly 1 pipeline named {name}, found {len(hits)} (deploy bundle first)")
        pipelines[label] = hits[0].pipeline_id
    jobs = [j for j in w.jobs.list(name=JOB_NAME) if j.settings and j.settings.name == JOB_NAME]
    if len(jobs) != 1:
        raise RuntimeError(f"expected exactly 1 job named {JOB_NAME}, found {len(jobs)} (deploy bundle first)")
    return pipelines, jobs[0].job_id


def pipeline_state(w: WorkspaceClient, pipeline_id: str) -> str:
    return state_name(w.pipelines.get(pipeline_id).state) or "UNKNOWN"


def ensure_idle(w: WorkspaceClient, pipelines: dict, deadline_s: float) -> None:
    """ให้ pipeline ทั้งสองว่างก่อนเริ่ม case (ถ้าค้างจาก run ก่อน: stop แล้วรอ)."""
    t0 = time.monotonic()
    stopped = set()
    while True:
        busy = {}
        for label, pid in pipelines.items():
            st = pipeline_state(w, pid)
            if st in PIPELINE_BUSY | {"STOPPING"}:
                busy[label] = st
                if label not in stopped and st != "STOPPING":
                    w.pipelines.stop(pid)
                    stopped.add(label)
        if not busy:
            if stopped:
                result(f"[pre] stopped leftover updates for {sorted(stopped)}; idle after {time.monotonic() - t0:.0f}s")
            return
        if time.monotonic() - t0 > deadline_s:
            raise RuntimeError(f"pipelines not idle after {deadline_s:.0f}s: {busy}")
        time.sleep(POLL_S)


def start_update(w: WorkspaceClient, tr: UpdateTrack, t0: float) -> None:
    tr.t_called = round(time.monotonic() - t0, 1)
    try:
        tr.update_id = w.pipelines.start_update(tr.pipeline_id).update_id
    except Exception as exc:  # คำต่อคำคือผลที่ต้องการ
        tr.start_error = f"{type(exc).__name__}: {trunc(exc)}"
        tr.done = True
        tr.end_t = round(time.monotonic() - t0, 1)


@dataclass
class TaskTrack:
    timeline: list = field(default_factory=list)  # "attempt0:RUNNING" ... เก็บตอนเปลี่ยน
    messages: dict = field(default_factory=dict)  # attempt -> state_message
    attempts: set = field(default_factory=set)
    attempt_runs: dict = field(default_factory=dict)  # attempt -> task run_id (ไว้ดึง error ของทุก attempt)
    attempt_results: dict = field(default_factory=dict)  # attempt -> result_state ล่าสุดที่เห็น
    last_state: dict = field(default_factory=dict)  # attempt -> state ล่าสุดที่บันทึก (get_run คืนทุก attempt จึงต้องแยกต่อ attempt)
    run_id: int | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    result_state: str | None = None


@dataclass
class RunTrack:
    run_id: int
    page_path: str
    t_called: float
    timeline: list = field(default_factory=list)
    tasks: dict = field(default_factory=dict)
    result_state: str | None = None
    state_message: str | None = None
    end_t: float = 0.0
    done: bool = False


def run_job(w: WorkspaceClient, job_id: int, t0: float, runs: list) -> RunTrack:
    """สั่ง job แล้วใส่ run ลง runs ทันที (ก่อนเรียก API อื่น) เพื่อให้ cleanup cancel ได้เสมอ."""
    t_called = round(time.monotonic() - t0, 1)
    run = w.jobs.run_now(job_id).response  # ไม่ใช้ .result(): ต้องควบคุม deadline เอง
    rt = RunTrack(run_id=run.run_id, page_path="(unknown)", t_called=t_called)
    runs.append(rt)
    try:
        rt.page_path = urlparse(w.jobs.get_run(run.run_id).run_page_url or "").path or "(no url)"  # path เท่านั้น ไม่พิมพ์ host
    except Exception as exc:
        rt.page_path = f"(get_run failed: {type(exc).__name__})"
    return rt


def poll_run(w: WorkspaceClient, rt: RunTrack, now: float) -> None:
    run = w.jobs.get_run(rt.run_id)
    lc = state_name(run.state.life_cycle_state) if run.state else None
    add_state(rt.timeline, now, lc)
    for task in run.tasks or []:
        tt = rt.tasks.setdefault(task.task_key, TaskTrack())
        attempt = task.attempt_number or 0
        st = task.state
        lc_t = state_name(st.life_cycle_state) if st else None
        rs_t = state_name(st.result_state) if st and st.result_state else None
        state_t = f"{lc_t}/{rs_t}" if lc_t and rs_t else lc_t
        if state_t and tt.last_state.get(attempt) != state_t:
            tt.last_state[attempt] = state_t
            tt.timeline.append((round(now, 1), f"attempt{attempt}:{state_t}"))
        tt.attempts.add(attempt)
        if task.run_id:
            tt.attempt_runs[attempt] = task.run_id
        if rs_t:
            tt.attempt_results[attempt] = rs_t
        if st and st.state_message:
            tt.messages[attempt] = trunc(st.state_message)
        if attempt >= max(tt.attempts):  # สรุปของ task ใช้ attempt ล่าสุดเท่านั้น
            tt.run_id, tt.start_ms, tt.end_ms, tt.result_state = task.run_id, task.start_time, task.end_time, rs_t
    if lc in TERMINAL_RUN:
        rt.done, rt.end_t = True, round(now, 1)
        rt.result_state = state_name(run.state.result_state) if run.state.result_state else None
        rt.state_message = trunc(run.state.state_message or "")


def poll_update(w: WorkspaceClient, tr: UpdateTrack, now: float, errors: set) -> None:
    try:
        state = state_name(w.pipelines.get_update(tr.pipeline_id, tr.update_id).update.state)
    except Exception as exc:  # get_update อาจ 404 ชั่วครู่หลัง start: บันทึกข้อความไม่ซ้ำแล้วลองต่อ
        msg = f"{tr.label}: get_update {type(exc).__name__}: {trunc(exc, 150)}"
        if msg not in errors:
            errors.add(msg)
            result(f"[poll] t={now:.0f}s {msg}")
        return
    add_state(tr.timeline, now, state)
    if state in TERMINAL_UPDATE:
        tr.done, tr.end_t = True, round(now, 1)


def observe(w, t0, deadline_s, updates=(), run: RunTrack | None = None) -> bool:
    """poll update/run จนทุกตัวจบหรือครบ deadline คืน True ถ้าจบครบ."""
    errors: set = set()
    end = time.monotonic() + deadline_s
    while True:
        now = time.monotonic() - t0  # timestamp เดียวต่อรอบ: ทุก track ใช้ร่วมกัน ไม่ให้เหลื่อมกันภายในรอบ
        for tr in updates:
            if not tr.done:
                poll_update(w, tr, now, errors)
        if run is not None and not run.done:
            poll_run(w, run, now)
        pending = [tr for tr in updates if not tr.done] + ([run] if run is not None and not run.done else [])
        if not pending:
            return True
        if time.monotonic() >= end:
            now = round(time.monotonic() - t0, 1)
            for item in pending:
                item.end_t = now
            return False
        time.sleep(POLL_S)


def update_errors(w: WorkspaceClient, tr: UpdateTrack) -> list[str]:
    """ERROR events ของ update นี้ (ถ้าดึงได้)."""
    try:
        events = w.pipelines.list_pipeline_events(
            tr.pipeline_id, filter="level='ERROR'", max_results=10, order_by=["timestamp desc"]
        )
        out = []
        for ev in itertools.islice(events, 10):
            ev_update = getattr(ev.origin, "update_id", None) if ev.origin else None
            if ev_update in (None, tr.update_id):
                out.append(trunc(ev.message or ev.error or ""))
        return out[:3]
    except Exception as exc:
        return [f"(list_pipeline_events failed: {type(exc).__name__}: {trunc(exc, 150)})"]


def report_update(w, tag: str, tr: UpdateTrack) -> None:
    result(f"[{tag}] {tr.label} pipeline={PIPELINE_NAMES[tr.label]} start_called_at={tr.t_called}s update_id={tr.update_id}")
    if tr.start_error:
        result(f"[{tag}] {tr.label} start_update RAISED {tr.start_error}")
        return
    final = tr.timeline[-1][1] if tr.timeline else "NONE"
    result(f"[{tag}] {tr.label} timeline={fmt_timeline(tr.timeline)}")
    result(f"[{tag}] {tr.label} final={final} ended_at={tr.end_t:g}s terminal={tr.done}")
    if final != "COMPLETED":
        for msg in update_errors(w, tr):
            result(f'[{tag}] {tr.label} error_event="{msg}"')


def report_run(w, tag: str, rt: RunTrack) -> None:
    result(f"[{tag}] job run_id={rt.run_id} page={rt.page_path} called_at={rt.t_called}s")
    result(f"[{tag}] job timeline={fmt_timeline(rt.timeline)}")
    result(f"[{tag}] job result_state={rt.result_state} ended_at={rt.end_t:g}s terminal={rt.done} message=\"{rt.state_message}\"")
    for key, tt in rt.tasks.items():
        dur = f"{(tt.end_ms - tt.start_ms) / 1000:.0f}s" if tt.start_ms and tt.end_ms else "n/a"
        result(f"[{tag}] task={key} result={tt.result_state} attempts={sorted(tt.attempts)} last_attempt_duration={dur}")
        result(f"[{tag}] task={key} timeline={fmt_timeline(tt.timeline)}")
        for attempt, msg in sorted(tt.messages.items()):
            result(f'[{tag}] task={key} attempt{attempt} state_message="{msg}"')
        # ทุก attempt ที่ไม่สำเร็จ (attempt ก่อนสุดท้ายล้มแน่นอน เพราะถูก retry) ดึง error แม้ retry จะช่วยให้ job ผ่าน
        last = max(tt.attempts, default=0)
        for attempt, run_id in sorted(tt.attempt_runs.items()):
            outcome = tt.attempt_results.get(attempt)
            if attempt == last and outcome in (None, "SUCCESS"):
                continue
            try:
                err = w.jobs.get_run_output(run_id).error
            except Exception as exc:
                err = f"(get_run_output failed: {type(exc).__name__}: {trunc(exc, 150)})"
            result(f'[{tag}] task={key} attempt{attempt} result={outcome} output_error="{trunc(err or "")}"')


def report_updates_list(w, tag: str, label: str, pipeline_id: str, t_ref_ms: int) -> None:
    """update ล่าสุดทั้งหมดของ pipeline (รวมที่ job สร้างเอง) เพื่อนับว่า task สร้าง update กี่ครั้ง."""
    try:
        ups = w.pipelines.list_updates(pipeline_id, max_results=6).updates or []
        for u in ups:
            rel = f"{(u.creation_time - t_ref_ms) / 1000:+.0f}s" if u.creation_time else "n/a"
            result(f"[{tag}] {label} update={u.update_id} state={state_name(u.state)} cause={state_name(u.cause)} created={rel}")
    except Exception as exc:
        result(f"[{tag}] {label} list_updates failed: {type(exc).__name__}: {trunc(exc, 150)}")


def cleanup(w, pipelines: dict, runs: list) -> None:
    """best effort: อย่าให้ update/run ที่ค้างยึด pipeline slot หลัง probe จบหรือถูกฆ่า."""
    for rt in runs:
        try:
            lc = state_name(w.jobs.get_run(rt.run_id).state.life_cycle_state)
            if lc not in TERMINAL_RUN:
                w.jobs.cancel_run(rt.run_id)
                result(f"[cleanup] cancelled job run {rt.run_id} (was {lc})")
        except Exception as exc:
            result(f"[cleanup] cancel run {rt.run_id} failed: {type(exc).__name__}: {trunc(exc, 150)}")
    for label, pid in pipelines.items():
        try:
            st = pipeline_state(w, pid)
            if st in PIPELINE_BUSY:
                w.pipelines.stop(pid)
                result(f"[cleanup] stopped pipeline {label} (was {st})")
        except Exception as exc:
            result(f"[cleanup] stop pipeline {label} failed: {type(exc).__name__}: {trunc(exc, 150)}")


# ---------- cases ----------


class Ctx:
    def __init__(self, w, pipelines, job_id):
        self.w, self.pipelines, self.job_id = w, pipelines, job_id
        self.runs: list[RunTrack] = []
        self.t_end = time.monotonic() + TOTAL_BUDGET_S

    def cap(self, limit: float) -> float:
        return max(0.0, min(limit, self.t_end - time.monotonic()))


def case_parallel(ctx: Ctx) -> bool:
    """Step 2: start_update A กับ B ห่างกันไม่เกิน ~1 วินาที."""
    w = ctx.w
    ensure_idle(w, ctx.pipelines, ctx.cap(IDLE_DEADLINE_S))
    tracks = [UpdateTrack(label, pid) for label, pid in ctx.pipelines.items()]
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda tr: start_update(w, tr, t0), tracks))
    finished = observe(w, t0, ctx.cap(PIPELINE_DEADLINE_S), updates=tracks)
    for tr in tracks:
        report_update(w, "step2", tr)
    verdict, detail = classify(*tracks)
    result(f"[step2] verdict={verdict} {detail}")
    if not finished:
        result("[step2] TIMEOUT: updates not terminal before deadline (will be stopped by cleanup)")
    return finished


def case_seq(ctx: Ctx) -> bool:
    """Step 3: job spike_s3_seq ต้องสำเร็จทั้ง 2 task."""
    w = ctx.w
    ensure_idle(w, ctx.pipelines, ctx.cap(IDLE_DEADLINE_S))
    t0 = time.monotonic()
    rt = run_job(w, ctx.job_id, t0, ctx.runs)
    finished = observe(w, t0, ctx.cap(JOB_DEADLINE_S), run=rt)
    report_run(w, "step3", rt)
    ok = finished and rt.result_state == "SUCCESS" and all(t.result_state == "SUCCESS" for t in rt.tasks.values()) and len(rt.tasks) == 2
    result(f"[step3] seq_ok={ok} tasks={ {k: t.result_state for k, t in rt.tasks.items()} }")
    return ok


def case_seq_during(ctx: Ctx) -> bool:
    """Step 4: start_update A แล้วสั่ง job ทันที (ห่างไม่เกิน ~2 วินาที)."""
    w = ctx.w
    ensure_idle(w, ctx.pipelines, ctx.cap(IDLE_DEADLINE_S))
    t0 = time.monotonic()
    t_ref_ms = int(time.time() * 1000)
    a = UpdateTrack("A", ctx.pipelines["A"])
    start_update(w, a, t0)
    rt = run_job(w, ctx.job_id, t0, ctx.runs)
    result(f"[step4] gap_between_start_update_and_run_now={rt.t_called - (a.t_called or 0):.1f}s")
    finished = observe(w, t0, ctx.cap(JOB_DEADLINE_S), updates=[a], run=rt)
    if finished is False and not a.done:
        result("[step4] TIMEOUT before job/standalone update finished")
    report_update(w, "step4", a)
    report_run(w, "step4", rt)
    report_updates_list(w, "step4", "A", ctx.pipelines["A"], t_ref_ms)
    attempts = max((len(t.attempts) for t in rt.tasks.values()), default=0)
    first = next(iter(rt.tasks.values()), None)
    result(f"[step4] job_result={rt.result_state} max_task_attempts={attempts} first_task_result={first.result_state if first else None}")
    return finished


CASES = {"parallel": case_parallel, "seq": case_seq, "seq_during": case_seq_during}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="all", choices=["all", *CASES])
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))  # ให้ finally (cleanup) ทำงานเมื่อ workflow ถูกยกเลิก
    w = WorkspaceClient(
        config=Config(http_timeout_seconds=SDK_HTTP_TIMEOUT_S, retry_timeout_seconds=SDK_RETRY_TIMEOUT_S)
    )
    pipelines, job_id = lookup_ids(w)
    result(f"[s3] case={args.case} pipelines_found={sorted(pipelines)} job_found=True")

    ctx = Ctx(w, pipelines, job_id)
    ok = True
    try:
        for name in (CASES if args.case == "all" else [args.case]):
            if ctx.cap(1) <= 0:
                result(f"[s3] total budget exhausted: skip {name}")
                ok = False
                continue
            try:
                ok = CASES[name](ctx) and ok
            except Exception as exc:
                result(f"[{name}] ERROR {type(exc).__name__}: {trunc(exc)}")
                ok = False
    finally:
        cleanup(w, pipelines, ctx.runs)
    result(f"[s3] done ok={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
