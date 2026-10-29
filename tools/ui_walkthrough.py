"""Drive the BankDW app like a user: one flow per BR, count the interactions, time each answer,
and take high-resolution screenshots for the report and the slides.

    python tools/ui_walkthrough.py [--out report/figures] [--url http://localhost:8501]

Needs Playwright (pip install playwright) and a local Google Chrome (channel="chrome").
"Time to answer" = from the user's last interaction to the moment Streamlit finished re-running the
script and the result is on screen. BR04 writes one status change (acted_by = 'ui_walkthrough');
remove it afterwards (see the printed SQL). Results: <out>/ui_walkthrough.json
"""
import argparse
import json
import re
import time

from playwright.sync_api import sync_playwright

p = argparse.ArgumentParser()
p.add_argument("--out", default="report/figures")
p.add_argument("--url", default="http://localhost:8501")
args = p.parse_args()

RESULTS = []


def settle(page, started=None, timeout=60):
    """Wait until Streamlit has finished re-running; return seconds since `started`.
    Switching tabs does not re-run the script (every tab is already rendered): the answer is on
    screen at once, reported as 0.0."""
    app = page.locator('[data-testid="stApp"]')
    deadline, reran = time.perf_counter() + 2, False
    while time.perf_counter() < deadline:                      # wait for the rerun to start
        if app.get_attribute("data-test-script-state") == "running":
            reran = True
            break
        time.sleep(0.02)
    end = time.perf_counter() + timeout
    while reran and time.perf_counter() < end:                  # ... and to finish
        if app.get_attribute("data-test-script-state") != "running":
            break
        time.sleep(0.02)
    elapsed = time.perf_counter() - started if started is not None else None
    page.wait_for_timeout(300)                                  # let charts and tables paint
    if started is None:
        return None
    return round(elapsed, 2) if reran else 0.0


def record(br, flow, interactions, seconds, result):
    RESULTS.append({"br": br, "flow": flow, "interactions": interactions, "seconds": seconds, "result": result})
    print(f"{br:<5} {interactions} thao tác  {seconds:>5} s  {flow} -> {result}", flush=True)


def tab(page, name):
    page.locator('[role="tab"]:visible').filter(has_text=re.compile(name)).first.click()


def shot(page, name):
    page.screenshot(path=f"{args.out}/{name}.png")


with sync_playwright() as pw:
    browser = pw.chromium.launch(channel="chrome", headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
    page.goto(args.url)
    page.get_by_text("BR01 · Tổng quan dòng tiền").wait_for(timeout=60000)
    settle(page)

    # BR01: the Overview tab opens by default; one interaction = choose the period
    box = page.locator('[data-testid="stDateInput"]').filter(has_text="Kỳ báo cáo").locator("input")
    box.click()
    box.fill("1997/01/01 – 1997/12/31")
    t = time.perf_counter()
    box.press("Enter")
    s = settle(page, t)
    page.keyboard.press("Escape")
    record("BR01", "chọn kỳ 1997", 1, s, page.locator('[data-testid="stMetricValue"]:visible').first.inner_text())
    box.click()
    box.fill("1998/01/01 – 1998/12/31")
    box.press("Enter")
    settle(page)
    page.keyboard.press("Escape")
    page.mouse.click(1300, 120)
    shot(page, "app_overview")

    # BR02: Analysis tab (BR02 sub-tab is the default), type the account
    tab(page, "Phân tích")
    settle(page)
    acc = page.locator('[data-testid="stNumberInput"]').filter(has_text="Mã tài khoản").locator("input")
    acc.fill("2378")
    t = time.perf_counter()
    acc.press("Enter")
    s = settle(page, t)
    record("BR02", "tab Phân tích → nhập tài khoản 2378", 2, s,
           page.get_by_text(re.compile(r"^Tài khoản 2378")).filter(visible=True).first.inner_text()[:60])
    acc.fill("11")
    acc.press("Enter")
    settle(page)
    shot(page, "app_lookup")

    # BR03: alerts sub-tab, select the newest alert, read reason and evidence
    tab(page, "Cảnh báo")
    settle(page)
    grid = page.locator('[data-testid="stDataFrame"]:visible').first
    grid.scroll_into_view_if_needed()
    bb = grid.bounding_box()
    t = time.perf_counter()
    page.mouse.click(bb["x"] + 18, bb["y"] + 35 + 17)            # selection column, first data row
    s = settle(page, t)
    title = page.get_by_text(re.compile(r"^Cảnh báo #\d+")).filter(visible=True).first
    title.wait_for(timeout=10000)
    record("BR03", "tab Phân tích → Cảnh báo → chọn cảnh báo", 3, s, title.inner_text())
    title.scroll_into_view_if_needed()
    page.mouse.wheel(0, -120)
    page.wait_for_timeout(300)
    shot(page, "app_alert")

    # BR04: change status with a note (written to ops; removed afterwards)
    form = page.locator('[data-testid="stForm"]:visible').first
    form.scroll_into_view_if_needed()
    form.get_by_role("combobox").first.click()
    page.get_by_role("option", name=re.compile("Đang xem xét")).click()
    form.locator('[data-testid="stTextInput"]').filter(has_text="Ghi chú kết quả").locator("input").fill("Kiểm tra với khách hàng (đo thao tác)")
    form.locator('[data-testid="stTextInput"]').filter(has_text="Người xử lý").locator("input").fill("ui_walkthrough")
    t = time.perf_counter()
    form.get_by_role("button", name="Ghi nhận").click()
    s = settle(page, t)
    record("BR04", "chọn trạng thái → ghi chú → Ghi nhận (sau BR03)", 3, s, "đã ghi nhận")

    # BR06: Operations tab opens on the pipeline monitor
    tab(page, "Vận hành")
    t = time.perf_counter()
    s = settle(page, t)
    record("BR06", "tab Vận hành", 1, s, "danh sách lần chạy")
    shot(page, "app_runs")

    # BR05
    t = time.perf_counter()
    tab(page, "BR05")
    s = settle(page, t)
    record("BR05", "tab Vận hành → BR05", 2, s, "bản ghi bị tách theo lô và quy tắc")
    shot(page, "app_rejects")

    # BR07: backup catalog and operations (a backup itself is timed in ops.backup_operation)
    t = time.perf_counter()
    tab(page, "BR07")
    s = settle(page, t)
    record("BR07", "tab Vận hành → BR07", 2, s, "danh mục bản sao lưu, lịch sử thao tác")
    shot(page, "app_backup")

    # BR08: old and new period by tier, versions of the demo client
    t = time.perf_counter()
    tab(page, "BR08")
    s = settle(page, t)
    record("BR08", "tab Vận hành → BR08", 2, s, "báo cáo theo hạng kỳ cũ / kỳ mới, phiên bản khách 24")
    shot(page, "app_scd2")

    # Scenario runner (N01-N05): preview and run KB03 again (a re-run: adds nothing)
    tab(page, "Chạy kịch bản")
    settle(page)
    shot(page, "app_scenario_preview")
    t = time.perf_counter()
    page.get_by_role("button", name=re.compile("Chạy KB03")).click()
    page.get_by_text(re.compile("Kịch bản KB03: xong")).wait_for(timeout=180000)
    s = round(time.perf_counter() - t, 2)                       # the app polls the runner until the job ends
    settle(page)
    record("KB03", "tab Vận hành → Chạy kịch bản → (KB03 mặc định) → Chạy", 3, s, "chạy lại lô KB03_FRAUD")
    page.get_by_text(re.compile("Kịch bản KB03: xong")).scroll_into_view_if_needed()
    shot(page, "app_scenario_run")
    browser.close()

json.dump(RESULTS, open(f"{args.out}/ui_walkthrough.json", "w"), ensure_ascii=False, indent=1)
print("cleanup: DELETE FROM ops.alert_action WHERE acted_by = 'ui_walkthrough'; "
      "and reset that alert's status to NEW")
