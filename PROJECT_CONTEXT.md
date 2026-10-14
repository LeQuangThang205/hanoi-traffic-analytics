# Hanoi Traffic Analytics — Project Context

> Source of truth cho toàn bộ project. Mọi AI agent và developer phải đọc file này trước khi code.
> Nhiệm vụ hiện tại: CHỈ tạo/cập nhật tài liệu quản lý. Không triển khai application.

---

## 1. Mục tiêu dự án

Xây dựng hệ thống thu thập và phân tích dữ liệu giao thông kết hợp thời tiết tại Hà Nội, sử dụng Apache Spark làm processing engine chính.

Hệ thống làm được 5 việc chính:

1. Lấy dữ liệu giao thông thật từ TomTom Traffic API.
2. Lấy dữ liệu thời tiết thật từ Open-Meteo API.
3. Cho phép thêm/xóa tuyến đường (quản lý `roads.csv`, thao tác được trên giao diện).
4. Xử lý và phân tích dữ liệu bằng PySpark / Apache Spark.
5. Hiển thị kết quả bằng Streamlit Dashboard (metrics, map, charts, table, filters).

Nguyên tắc: làm vừa đủ nhưng hoàn chỉnh, ưu tiên đơn giản — dễ hiểu — dễ chạy — dễ demo — dễ giải thích với giảng viên. Không làm theo hướng production nặng.

Tên ngắn khi demo: **Hanoi Traffic Analytics**

---

## 2. Kiến trúc pipeline

### 2.1. Sơ đồ tổng thể

```text
USER
 │
 ▼
STREAMLIT DASHBOARD (app.py)
 │   ├─ Quản lý tuyến (roads.csv)
 │   └─ Xem phân tích (processed/)
 │
 ▼
roads.csv
 │
 ▼
Python Data Collector (collector.py)
 │   ├─ traffic_api.py  → TomTom Traffic API
 │   └─ weather_api.py  → Open-Meteo API
 │
 ▼
data/traffic_data.csv (raw, append theo thời gian)
 │
 ▼
PySpark (spark_analysis.py)
 │   ├─ cleaning / casting
 │   ├─ congestion calculation
 │   ├─ time extraction / classification
 │   └─ aggregation
 │
 ├──────────┼──────────┐
 ▼          ▼          ▼
road_summary  hourly_summary  weather_summary
 (processed/)
 │
 ▼
STREAMLIT DASHBOARD
 ├─ Metrics
 ├─ Map (Folium)
 ├─ Charts (Plotly)
 └─ Table + Filters
```

### 2.2. Luồng One-click Update (Phase 9)

```text
Collector → Spark → Processed Data → Dashboard Refresh
```

### 2.3. Cấu trúc file chính (tham khảo)

```text
hanoi-traffic-analytics/
 ├─ app.py
 ├─ collector.py
 ├─ spark_analysis.py
 ├─ config.py
 ├─ requirements.txt
 ├─ README.md
 ├─ .env
 ├─ .env.example
 ├─ data/
 │   ├─ roads.csv
 │   └─ traffic_data.csv
 ├─ processed/
 │   ├─ road_summary.csv
 │   ├─ hourly_summary.csv
 │   └─ weather_summary.csv
 └─ utils/
     ├─ traffic_api.py
     ├─ weather_api.py
     └─ road_manager.py
```

> Không nhất thiết tách `utils/` ngay từ đầu. Có thể làm 3 file chính trước (`collector.py`, `spark_analysis.py`, `app.py`) rồi refactor sau.

---

## 3. Technology stack (CỐ ĐỊNH)

### 3.1. Stack bắt buộc

| Thành phần | Công nghệ |
|---|---|
| Ngôn ngữ | Python |
| Traffic API | TomTom Traffic API |
| Weather API | Open-Meteo API |
| Big Data Processing | PySpark / Apache Spark (Local Mode) |
| Lưu danh sách tuyến | CSV (`roads.csv`) |
| Lưu dữ liệu thu thập | CSV (`traffic_data.csv`) |
| Dashboard | Streamlit |
| Charts | Plotly |
| Map | Folium / streamlit-folium |
| Config secret | python-dotenv (`.env`) |
| IDE | VS Code |
| Runtime Spark | Java JDK |

### 3.2. Không thuộc MVP — CẤM tự ý thêm

- Kafka
- Hadoop / HDFS
- Docker
- Kubernetes
- PostgreSQL / MySQL (mọi database)
- React
- Spring Boot
- Machine Learning
- Spark Cluster / Cloud Infrastructure

Không được thêm các công nghệ trên nếu chưa được người dùng chấp thuận bằng văn bản rõ ràng.

---

## 4. Phạm vi MVP

### 4.1. In-scope (phải có)

- Thu thập traffic thật (TomTom): `currentSpeed`, `freeFlowSpeed`, `currentTravelTime`.
- Thu thập weather thật (Open-Meteo): `temperature`, `rain`, `humidity`, `wind_speed`.
- Quản lý tuyến đường: thêm / xóa / liệt kê trong `roads.csv` (tối thiểu 5 tuyến Hà Nội).
- Spark ETL: cleaning, casting, tính congestion, tách thời gian, phân loại, aggregation.
- Spark Analytics: 5 bài toán phân tích chính (mục 5).
- Dashboard Streamlit: metrics, map marker màu theo congestion + popup, 4 charts, bảng dữ liệu + filter, sidebar quản lý tuyến + nút cập nhật.
- One-click update: Collector → Spark → Dashboard refresh.
- README + backup dataset + chuẩn bị demo.

### 4.2. Out-of-scope (không làm trong MVP)

- Mọi mục trong 3.2.
- Dự báo / ML / dự đoán ùn tắc.
- Auth, phân quyền, multi-user.
- Realtime streaming, message queue.
- Triển khai cloud / CI-CD production.

---

## 5. Năm bài toán phân tích chính

Công thức chuẩn toàn project:

```text
congestion = (1 - current_speed / free_flow_speed) * 100
```

Phân loại:

- `< 20%`: Thông thoáng
- `20–40%`: Đông
- `40–60%`: Ùn tắc
- `> 60%`: Ùn tắc nghiêm trọng

5 bài toán:

1. **Tốc độ trung bình theo tuyến đường.**
2. **Mức ùn tắc trung bình theo tuyến đường.**
3. **So sánh mức ùn tắc giữa các tuyến.**
4. **Phân tích ùn tắc theo giờ (hourly).**
5. **So sánh giao thông khi có mưa và không mưa (weather impact).**

Output Spark tối thiểu:

- `processed/road_summary.csv`
- `processed/hourly_summary.csv`
- `processed/weather_summary.csv`

---

## 6. Roadmap Phase 0 → Phase 10 + Acceptance Criteria

### Phase 0 — Environment (HOÀN THÀNH)

Nội dung:

- Cài Python, Java JDK, PySpark, Streamlit, Plotly, Requests, Folium.
- Chạy Spark Local Mode thành công.

Acceptance criteria:

- [x] `python --version` chạy được.
- [x] `java -version` chạy được.
- [x] `test_spark.py` tạo DataFrame và tính congestion thành công, in `Spark version`.
- [x] Không yêu cầu cluster/HDFS.

### Phase 1 — Project Foundation (HOÀN THÀNH)

Nội dung:

- Cấu trúc thư mục, `config.py`, `.env`, `.env.example`, `.gitignore`, `data/roads.csv`, `requirements.txt`.

Acceptance criteria:

- [x] Có thư mục `data/`, `processed/` (hoặc tạo khi chạy).
- [x] `config.py` đọc secret từ `.env` qua `python-dotenv`, không hard-code key.
- [x] Có `.env.example` không chứa secret thật; `.env` nằm trong `.gitignore`.
- [x] `data/roads.csv` có tối thiểu 5 tuyến Hà Nội với cột `road_name,lat,lon`.
- [x] `requirements.txt` liệt kê đúng stack cố định.
- [x] Chạy `python -c "import config"` (hoặc tương đương) không lỗi import.

### Phase 2 — TomTom Traffic (HOÀN THÀNH)

Nội dung:

- Kết nối TomTom Traffic API, lấy 1 tuyến rồi mở rộng nhiều tuyến, lưu CSV.

Acceptance criteria:

- [x] Lấy được `currentSpeed`, `freeFlowSpeed`, `currentTravelTime` cho 1 tọa độ test.
- [x] Lấy được cho toàn bộ tuyến trong `roads.csv`.
- [x] Dữ liệu append vào `data/traffic_data.csv` kèm `timestamp`.
- [x] Key TomTom nằm trong `.env`, không hard-code.
- [x] Lỗi API/network được xử lý, không crash collector.

### Phase 3 — Weather Integration (HOÀN THÀNH)

Nội dung:

- Tích hợp Open-Meteo, ghép traffic + weather theo `(lat, lon, timestamp)`.

Acceptance criteria:

- [x] Với input `lat, lon` lấy được `temperature, rain, humidity, wind_speed`.
- [x] Mỗi bản ghi traffic có weather tương ứng (thực hiện bằng temporal join contract: `road_name` + nearest weather timestamp trong 60 phút; raw datasets giữ riêng, Spark Phase 5 join).
- [x] Không cần key (Open-Meteo free) nhưng phải xử lý lỗi request.
- [x] Schema `traffic_data.csv` ổn định cho Phase 4+.

### Phase 4 — Historical Data Collection (HOÀN THÀNH)

Nội dung:

- Thu thập nhiều thời điểm, xây dựng dataset lịch sử.

Acceptance criteria:

- [x] `traffic_data.csv` có dữ liệu nhiều khung giờ/ngày khác nhau.
- [x] Không trùng lặp/ghi đè; timestamp chuẩn parse được bằng Spark.
- [ ] Có backup dataset dùng cho demo khi mất mạng/API (chuyển sang Phase 10 Testing & Demo — phase này đã yêu cầu backup dataset).
- [ ] Tài liệu ngắn ghi cách chạy collector để lấy thêm dữ liệu (chuyển sang Phase 10 cùng README demo).

### Phase 5 — Spark ETL (HOÀN THÀNH — final verification 5.8 PASS)

Nội dung (bắt buộc dùng PySpark):

- Cleaning, casting kiểu, tính `congestion`, tách `hour/day`, phân loại trạng thái, aggregation.

Acceptance criteria:

- [ ] `spark_analysis.py` chạy `local[*]` thành công, không dùng Pandas thay Spark cho ETL.
- [ ] Xử lý null / sai kiểu / chia cho 0 (`free_flow_speed`).
- [ ] Sinh đủ 3 file `processed/road_summary.csv`, `hourly_summary.csv`, `weather_summary.csv`.
- [ ] Chạy lại nhiều lần cho kết quả nhất quán (idempotent ở mức aggregation).

### Phase 6 — Spark Analytics (HOÀN THÀNH — final verification 6.4 PASS)

Nội dung:

- Thực hiện đủ 5 bài toán phân tích chính bằng Spark.

Acceptance criteria:

- [ ] Mỗi bài toán trong mục 5 có output tương ứng trong `processed/`.
- [ ] Kết quả khớp công thức congestion và ngưỡng phân loại.
- [ ] Có thể giải thích truy vấn/transform cho giảng viên.
- [ ] Không thay Spark bằng Pandas cho analytics.

### Phase 7 — Streamlit Dashboard (CURRENT_PHASE — ĐANG THỰC HIỆN)

Nội dung:

- Metrics, map, 4 charts, table, filters, sidebar.

Acceptance criteria:

- [ ] `streamlit run app.py` chạy được.
- [ ] Map Hà Nội có marker màu xanh/vàng/cam/đỏ + popup (speed, free speed, congestion, weather).
- [ ] 4 charts: speed theo thời gian (line), ùn tắc từng tuyến (bar), ùn tắc theo giờ (bar/line), mưa vs không mưa (bar).
- [ ] Bảng dữ liệu có filter tuyến + trạng thái.
- [ ] Sidebar có checklist tuyến, nút thêm tuyến, nút cập nhật dữ liệu.
- [ ] Chỉ dùng Pandas ở presentation layer nếu cần, Spark vẫn là engine xử lý.

### Phase 8 — Road Management (HOÀN THÀNH)

Nội dung:

- Thêm/xóa tuyến, quản lý `roads.csv` từ code và từ UI.

Acceptance criteria:

- [x] Thêm tuyến mới bằng `(road_name, lat, lon)` hợp lệ; từ chối trùng tên/tọa độ sai.
- [x] Xóa tuyến khỏi `roads.csv` và dashboard cập nhật.
- [ ] Tuyến mới xuất hiện trên map sau khi cập nhật dữ liệu (cần chạy pipeline lần đầu ở Phase 9; Dai Co Viet đã được thêm vào `roads.csv` và dashboard hiển thị coverage 5/6).
- [x] Không phá dữ liệu lịch sử khi sửa `roads.csv`.

Tiến độ:

- [x] **Bước 8.1 — Safe Road Management Core: COMPLETE** (verification 91 checks PASS, 0 API, 0 Spark; `utils/road_manager.py` giữ nguyên, không sửa code).
  - `roads.csv` vẫn là source of truth của MVP.
  - Validation (tên/tọa độ/finite-number) PASS; duplicate protection (case-insensitive + trim) PASS.
  - `add_road` / `remove_road` PASS (giữ thứ tự, header 1 lần, không tmp sót).
  - Last-road protection (`LastRoadError`) PASS; malformed CSV fail rõ ràng, không rewrite.
  - Atomic replacement (temp sibling + `os.replace` + cleanup) PASS.
  - Test dùng CSV tạm (`tempfile`); production `roads.csv` byte-identical PRE==POST.
- [x] **Bước 8.2 — Streamlit Road Management UI: COMPLETE** (AppTest headless PASS, smoke test 200, 0 API, 0 Spark; chỉ sửa `app.py`, `utils/road_manager.py` giữ nguyên).
  - Sidebar đọc `roads.csv` trực tiếp qua `road_manager.load_roads()` (không dùng `road_summary.csv`).
  - Add/Remove gọi `road_manager`; validation/error feedback (`ValidationError`/`DuplicateRoadError`/`LastRoadError`), không traceback.
  - Last-road protection ở cả UI (disable) và core; historical data không cascade delete.
  - Add/remove không tự gọi collector/Spark; tuyến mới chỉ có processed data sau lần cập nhật tiếp theo.
  - Processed dashboard có thể tạm thời khác monitored roads (coverage `x/y` hiển thị nhẹ, 6 KPI không đổi).
  - Automated tests chỉ dùng CSV tạm/AppTest read-only; production CSV byte-identical PRE==POST.
- [x] **Bước 8.2 UX patch — Add Road dùng catalog dropdown: COMPLETE** (catalog 16 checks + AppTest PASS, smoke 200, 0 API, 0 Spark; sửa `config.py` + `app.py`, `utils/road_manager.py` giữ nguyên).
  - `HANOI_ROAD_CATALOG` trong `config.py`: 13 entries (5 production giữ chính xác tọa độ `roads.csv` + 8 tuyến mới lấy điểm đại diện trên trục đường từ OpenStreetMap, làm tròn 4 decimals; Chua Boc/Nguyen Van Cu loại vì không xác minh được road point).
  - Người dùng chỉ chọn tên tuyến; lat/lon tự lấy từ catalog; không nhập tay, không geocoding runtime/API.
  - Dropdown Add chỉ hiện tuyến chưa theo dõi (trim + case-insensitive, tương thích road_manager); hết tuyến → thông báo thân thiện, không selectbox rỗng.
  - Luồng: dropdown → catalog → `road_manager.add_road()` → `roads.csv` (schema không đổi); tuyến xóa có thể thêm lại sau refresh.
  - Remove UI, 6 KPI, map, 5 charts, filters, historical no-cascade giữ nguyên.
- [x] **Phase 8 closeout PASS** (0 API, 0 Spark; user e2e đã thêm Dai Co Viet qua dropdown → `roads.csv` 6 tuyến, coverage dashboard 5/6; automated tests chỉ dùng CSV tạm, production PRE==POST trong closeout).
  - Phase 8.1 Safe Road Management Core PASS; Phase 8.2 Streamlit Road Management UI + catalog dropdown PASS.
  - `roads.csv` vẫn là source of truth của MVP; Add/Remove không tự gọi API/Spark; historical data không cascade delete.
  - Phase 9 sẽ triển khai one-click data update (lúc đó tuyến mới có processed data và marker trên map).

### Phase 9 — One-click Update (HOÀN THÀNH)

Nội dung:

- Một nút thao tác chạy full luồng Collector → Spark → Dashboard refresh.

Acceptance criteria:

- [x] Bấm `Cập nhật dữ liệu` / `Analyze Data` kích hoạt collector rồi Spark job.
- [x] Dashboard refresh hiển thị dữ liệu mới không cần restart thủ công.
- [x] Lỗi ở một bước được báo rõ, không treo UI.
- [x] Thời gian chạy chấp nhận được cho demo trên laptop.

Tiến độ:

- [x] **Bước 9.1 — Data Update Orchestrator: COMPLETE** (mock verification 18 checks PASS, 0 real API, 0 real Spark; NEW `data_update.py`, không sửa collector/Spark/app).
  - `run_data_update()` tái sử dụng `collector.run_collection_cycle()` + `run_spark_etl()`/`run_road/hourly/weather_analytics()`; synchronous, không background job; app.py vẫn presentation-only.
  - Result dict: success, started/finished UTC, duration, collection counters, spark_success/stages, processed_files, error.
  - Collector best-effort được phản ánh trung thực; Spark gate: cần >= 1 saved observation mới, fatal collection → Spark 0 lần; Spark fail → raw giữ nguyên, không rollback.
  - Concurrency guard: process-local non-blocking lock, call thứ hai bị từ chối ngay.
  - Real first update (Dai Co Viet acceptance) CHƯA chạy; production CSV unchanged trong mock tests.
  - Phase 9.2 sẽ nối Streamlit UI sau khi real pipeline được kiểm chứng.
- [x] **Bước 9.1C — Weather schema constant shadowing fix: COMPLETE** (static + contract + mock regression PASS, 0 real API, 0 real Spark; chỉ sửa `spark_analysis.py`).
  - First production collection succeeded 6/6 traffic + 6/6 weather; raw append retained (traffic 21 / weather 16 rows).
  - Spark failed do duplicate `WEATHER_REQUIRED_COLUMNS` global (Phase 6.3 ghi đè raw contract Phase 5.3 tại runtime).
  - Analytics constant renamed to `WEATHER_ANALYTICS_REQUIRED_COLUMNS` (1 def + 2 usages + docstring); raw contract + mọi logic/schema giữ nguyên.
  - No API/Spark production rerun during fix verification; processed data still awaiting controlled Spark recovery/update.
- [x] **Bước 9.1D — Controlled Spark Recovery: COMPLETE** (4 stages PASS 1 lần mỗi stage, 0 API; chỉ đọc raw hiện có, không sửa code).
  - Reused 21 traffic + 16 weather raw rows; ETL PASS (matched 21/21, combined 21 rows/25 cols), road/hourly/weather analytics PASS.
  - Dai Co Viet processed end-to-end: combined 1 row (22/29 km/h, congestion ~24.14 Đông, matched diff ~5.05 min), road_summary 6 tuyến.
  - Reconciliation: combined 21 = road_sum 21 = hourly_sum 21 = weather_sum 21; raw CSV byte-identical PRE==POST.
  - Dashboard data coverage = 6/6 (AppTest read-only PASS); deferred Phase 8 acceptance "newly added road becomes analyzable after next pipeline processing" = VERIFIED.
- [x] **Bước 9.2 — Streamlit One-click Data Update: COMPLETE** (mocked AppTest 28 checks PASS, smoke 200, 0 real API, 0 real Spark; chỉ sửa `app.py`).
  - Sidebar section "Cập nhật dữ liệu" sau Road Management; button gọi `data_update.run_data_update()` đúng 1 lần trong spinner, chỉ khi user bấm (render/filter/rerun không tự gọi).
  - Structured result handling: full success (flash + rerun), partial (warning flash + rerun), failure (inline error + facts, không rerun/traceback), already-running (warning riêng).
  - Flash/rerun pattern tách biệt road_flash; error sanitize `key=***`; production CSV byte-identical trong UI verification.
- [x] **Phase 9 closeout PASS** (user browser E2E PASS; backend + UI đã verify, không chạy thêm production trong closeout).
  - User bấm "Cập nhật dữ liệu" trên browser 2 lần thành công: traffic 21 → 33 (+12), weather 16 → 28 (+12), combined 21 → 33, road_summary 6 tuyến, hourly 2 → 3 rows; roads.csv giữ 6 tuyến.
  - Full luồng Streamlit → orchestrator → Collector → TomTom/Open-Meteo → raw append → Spark ETL/join/analytics → processed → rerun refresh đã chứng minh end-to-end.
  - Static 5 files PASS; AppTest sanity read-only PASS (Dai Co Viet, update button, 5 charts, 2 filters, 1 table, road mgmt).

### Phase 10 — Testing & Demo

Nội dung:

- Error handling, backup dataset, README, chuẩn bị demo.

Acceptance criteria:

- [ ] Có xử lý lỗi API/Spark/file cơ bản trên toàn luồng.
- [ ] Có backup dataset chạy offline được.
- [ ] `README.md` ghi cách cài, cấu hình `.env`, chạy collector/Spark/dashboard.
- [ ] Kịch bản demo 8 bước chạy end-to-end (mở dashboard → xem map → cập nhật → Spark → refresh → thêm tuyến → cập nhật → marker mới).

Tiến độ:

- [x] **Phase 10.1 — Full System Verification & Offline Demo Safety: COMPLETE** (0 TomTom, 0 Open-Meteo, 0 real Spark; production CSV PRE==POST; chỉ sửa `PROJECT_CONTEXT.md`).
  - Static verification: py_compile 10 files PASS (`app`, `data_update`, `collector`, `historical_collector`, `spark_analysis`, `config`, `test_spark`, `traffic_api`, `weather_api`, `road_manager`).
  - Import safety: `config`/`collector`/`spark_analysis`/`data_update`/`app` import không side-effect (không API/Spark/append/Streamlit auto-run); CSV unchanged.
  - Production baseline: roads 6 / traffic 33 / weather 28 / combined 33 / road_summary 6 / hourly 3 / weather 2; 7 contracts header PASS (3/9/12/25/11/12/7 cols).
  - Raw quality: traffic 0/33 invalid, weather 0/28 invalid (read-only, không sửa rows).
  - Processed quality: combined congestion formula mismatch 0, level invalid 0, diff invalid 0; matched 33 / unmatched 0 (all <=60 min).
  - Aggregate reconciliation: combined 33 == road_sum 33 == hourly_sum 33; weather_sum 33 == matched 33.
  - Road coverage: monitored 6 / analyzed 6 = 6/6; Dai Co Viet có trong roads/traffic/weather/combined/road_summary.
  - Dashboard read-only regression: AppTest 0 exception; update button đúng 1; 2 multiselects (Dai Co Viet trong filter); 1 detail dataframe; map 6 markers + 5 Plotly charts via helpers (plotly calls 5); không click update production.
  - Update UI mock regression: no-click 0 calls; full/partial/failure/already-running mỗi case đúng 1 call, 0 exception, 0 API/Spark thật.
  - Road manager regression (temp CSV): load/add/duplicate/remove/last-road/invalid/malformed/atomic 8/8 PASS; production `roads.csv` unchanged.
  - Collector mock regression (temp files): full/partial/fatal/append PASS; không production raw mutation.
  - Orchestrator mock regression: full/partial/fatal/zero-saved/spark-fail/concurrency 6/6 PASS; không real Spark/API.
  - Error-handling audit: 9 scenarios HANDLED (missing key, network failure, one-road fail, all-fail zero-saved skip Spark, malformed roads, Spark fail raw-kept, processed missing guard, schema drift guard, already-running lock + key redaction).
  - Offline dashboard confirmed: startup chỉ đọc `data/processed` snapshot (KPI/map/charts/filters/table đủ); không bắt buộc API; AppTest + smoke health `ok` PASS.
  - Production CSV PRE==POST byte-for-byte (7 SHA256 unchanged).
  - Backup status: chưa có `data/demo_backup/`; đề xuất backup 7 CSV (~15.9KB), restore bằng copy an toàn, không chứa `.env`/key/venv/logs.
  - README gaps: overview/setup/structure partial nhưng outdated (ghi Phase 1); thiếu architecture/stack/run-dashboard/pipeline/Spark-role/API-sources/screenshots/limitations/offline-demo.
  - Demo readiness: bước 3–8 READY (dashboard/KPI/map/charts/road-mgmt/update/Spark/mới-refresh đã chứng minh Phase 9 E2E); bước 1–2 NEEDS WORK ở README (cần viết lại Phase 10.2+, không code).
- [x] **Phase 10.2 — Local Data Lake + Parquet Storage: COMPLETE** (0 TomTom, 0 Open-Meteo; 2 production Spark ETL runs trên raw hiện có, lần 2 fix timestamp; production CSV PRE==POST).
  - Native Spark `df.write.parquet` unsupported trên Windows hiện tại (`HADOOP_HOME/winutils` unset, đã verify Phase 10.2) — không cài winutils/HDFS/Docker/WSL.
  - Approved fallback: PyArrow driver-side Parquet serialization (`pyarrow==25.0.1` thêm vào `requirements.txt`). Spark vẫn là ETL engine duy nhất (validate/transform/join/orderBy); PyArrow chỉ serialize snapshot MVP nhỏ đã order; không ETL thứ hai, không `pandas.to_parquet`.
  - `config.py`: `DATA_LAKE_DIR`/`CURATED_DIR`/`COMBINED_PARQUET_PATH` = `data/lake/curated/combined/combined_data.parquet`. `spark_analysis.py`: `export_combined_parquet()` (exact 25-col check, Spark orderBy, typed Arrow schema, temp sibling + read-back validate + `os.replace`) + `run_spark_etl()` export CSV trước rồi Parquet (thêm `parquet_path` param, meta có `parquet_path`/`parquet_rows`). `data_update.py`/`app.py` không đổi (orchestrator gọi `run_spark_etl`; dashboard vẫn CSV).
  - Bug tìm được nhờ reconciliation: Spark collect naive datetime theo local tz (+7h) — fix bằng `date_format` UTC của Spark trước collect; Parquet hiện reconcile 0 mismatch.
  - Parquet production: 33 rows / 25 cols typed (timestamp tz-aware, double, int32, bool) / 9590B vs CSV 8738B; coverage 6/6 = CSV (Dai Co Viet có); failure test (mock write lỗi) giữ dest + dọn temp PASS; không secret.
  - Dashboard regression: AppTest 0 exception, update button 1, 2 multiselects (Dai Co Viet), 1 table, 5 charts; update mock no-click 0 / click 1 call. Orchestrator mock full/fatal PASS.
  - Limitation: driver-side fallback phù hợp dataset nhỏ local; large-scale/cluster nên dùng native distributed Parquet writer + object storage.
- [x] **Phase 10.3 — ML Dataset Readiness & Collection Strategy: COMPLETE** (audit/design read-only, 0 TomTom, 0 Open-Meteo, 0 Spark production; 7 CSV + Parquet PRE==POST; không tạo `ml_features`/`train`/`test`/`model.pkl`/`mlruns`).
  - ML source: `data/lake/curated/combined/combined_data.parquet` (33 rows / 25 cols typed / 0 nulls / matched 33/33); CSV combined chỉ là dashboard snapshot.
  - Task: regression target `congestion_percent`; `congestion_level` suy ra sau prediction bằng thresholds hiện tại (<20/20–40/40–60/>60); không classification riêng.
  - Leakage audit 25 cols: TARGET = `congestion_percent`; LEAKAGE/DO-NOT-USE = `current_speed` (trong công thức target), `current_travel_time`, `confidence`, `congestion_level` (suy từ target); CONDITIONAL = `free_flow_speed`/`free_flow_travel_time` (baseline gần như hằng số theo tuyến, biết trước được — khuyến nghị chỉ dùng `free_flow_speed`, bỏ kia cho đỡ dư thừa); SAFE = `road_name`, `local_hour`, `day_of_week`/`is_weekend` (derive), `temperature`, `humidity`, `precipitation`, `rain`, `wind_speed`, `weather_code`; IDENTIFIER/METADATA/EXCLUDE = `timestamp`/`event_timestamp`/`weather_timestamp`/`weather_event_timestamp` (chỉ split thời gian), `lat`/`lon`/`weather_lat`/`weather_lon` (proxy tuyến, dư thừa), `weather_time_diff_minutes` (join metadata, không có lúc inference).
  - Target: min 0.0 / max 45.71 / mean 30.35 / median 34.21 / std 11.65; Thông thoáng 8, Đông 18, Ùn tắc 7, Nghiêm trọng 0 (thiếu class trên cùng).
  - Temporal: 2 ngày calendar (23/09 Wed, 29/09 Tue) / 3 giờ Hanoi (11:16, 12:12, 13:5 bản ghi) — cụm theo batch, ngoài 2 khung thu thập chuẩn; weekday 33 / weekend 0.
  - Road: 6 tuyến nhưng Dai Co Viet mới 3 obs (các tuyến khác 6); mean tuyến 13.8–38.7 (mất cân bằng target theo tuyến).
  - Weather: Mưa 2 / Không mưa 31; code {3:22, 1:5, 95:3, 51:2, 0:1} — gap mưa rõ rệt. Duplicate: 0 exact, 0 trùng (road+timestamp); nhưng obs đi theo batch đồng thời 5–6 tuyến/cycle nên tương quan thời gian — split phải giữ nguyên batch.
  - Sufficiency: NOT READY FOR MEANINGFUL ML (2 ngày, 3 giờ, 0 weekend, 2 mẫu mưa, 0 mẫu nghiêm trọng, Dai Co Viet 3 obs).
  - Collection target thực tế: 3,000–5,000 obs (practical prototype, không phải ngưỡng khoa học universal).
  - Rate từ `config.py` (6 tuyến, 15 phút, 06–10h + 16–20h = 32 cycles/ngày): 6 obs/cycle → ~192 combined/ngày (100% success). Time-to-target: ~1,000 ≈ 6 ngày; ~3,000 ≈ 16 ngày; ~5,000 ≈ 26 ngày (thực tế lâu hơn do fail từng phần). Demand: TomTom 192 + Open-Meteo 192 = 384 req/ngày (≈5,760/30 ngày, trong budget 20,000/safety 16,000).
  - Strategy: thu đủ sáng/trưa/chiều/tối + weekday/weekend + nhiều ngày + ngày mưa/không mưa; diversity > spam count. `historical_collector.py`: READY (bounded max_cycles, slot semantics, skip missed, injectable — không sửa). Nút Streamlit chỉ cho manual/demo, không click hàng trăm lần làm dataset.
   - ML schema (logical, chưa tạo file): FEATURES = road_name(onehot, không int ordinal), local_hour, day_of_week, is_weekend, temperature, humidity, precipitation, rain, wind_speed, weather_code, free_flow_speed; TARGET = congestion_percent; METADATA = event_timestamp (split), weather_matched (lọc). Split: sort theo event_timestamp, cũ train / mới test (+validation giữa nếu đủ lớn), giữ batch đồng thời cùng fold. Unmatched: exclude khỏi training prototype (không fill 0 câm).
   - Models (design): DummyRegressor baseline, Ridge/Linear, RandomForest; không DL/XGBoost. Metrics: MAE (chính, dễ giải thích), RMSE, R². MLflow planned (type/params/train-test period/features/MAE/RMSE/R²/artifact) — chưa cài/chạy.
- [x] **Phase 10.4 — Real Data Collection Campaign Preparation: COMPLETE** (0 TomTom, 0 Open-Meteo, 0 Spark production; 8 production artifacts PRE==POST; không synthetic/backfill/duplicate-fabrication; không scikit-learn/MLflow/model.pkl/prediction UI).
  - CAMPAIGN READY (chưa DATASET READY): collection target 3k–5k combined vẫn chưa achieved; phase này chỉ chuẩn bị hệ thống.
  - Selected schedule OPTION A: `COLLECTION_WINDOWS = (("06:00","22:00"),)` liên tục mỗi 5 phút (`COLLECTION_INTERVAL_MINUTES = 5`; Asia/Ho_Chi_Minh, start-inclusive/end-exclusive; cycles 06:00..21:55). Single-window đơn giản nhất (1 tuple, không gap logic), tự phủ trưa + tối (gap của config 10.3 cũ 06–10 + 16–20). Constants giữ trong `config.py` + planning constants `CAMPAIGN_TARGET_MIN/MAX_OBS = 3,000/5,000`, `CAMPAIGN_PROCESSING_CADENCE = "batch-on-demand"`. Manual Streamlit update + dashboard giữ nguyên.
  - Collection rate (6 roads, 5 phút): 192 cycles/ngày → ~1,152 combined observations/ngày (100% success). Internal API demand (KHÔNG kết luận quota/budget, chưa verify quota chính thức hiện tại): TomTom ~1,152 + Open-Meteo ~1,152 = ~2,304 requests/ngày; TomTom ~34,560/30 ngày. Phân biệt rõ combined observations/ngày (1,152) vs total API requests/ngày (2,304).
  - ETA từ current 33 combined (ideal 1,152/ngày): 1,000 ≈ 0.8 ngày (~1 ngày); 3,000 ≈ 2.6 ngày (~3 ngày); 5,000 ≈ 4.3 ngày (~5 ngày). Practical ETA (70% yield ≈ 806/ngày, allowance fail/offline/missed): 1,000 ≈ 1.2 ngày (~2 ngày); 3,000 ≈ 3.7 ngày (~4 ngày); 5,000 ≈ 6.2 ngày (~7 ngày).
  - `historical_collector.py` là orchestration/scheduling layer (tái dùng `collector.run_collection_cycle()`, không duplicate API logic): `run_campaign()` bounded max_cycles (> 0, không infinite), slot >= now + strict-after sau mỗi cycle (missed slots skip, không backfill), fatal cycle → error entry + sang slot tiếp (không retry spam). `campaign_progress()` read-only bằng stdlib csv (traffic/weather rows, date range, days, roads, local hours, weekday/weekend, rain/no-rain; không Pandas/Spark). CLI `python historical_collector.py --max-cycles N` độc lập Streamlit; `--progress` chỉ đọc tiến độ. Mỗi cycle in: Hanoi now, cycle i/N, slot, roads_loaded, traffic collected/failed/saved, weather collected/failed/saved, totals rows, next_slot; không log API key.
  - Resume/restart: append-only raw CSV, không truncate/overwrite; restart dùng cùng command, tiếp tục slot tương lai. Duplicate: timestamp thu thập là thời điểm gọi API (UTC giây) nên restart không tái tạo cùng slot; nguy cơ exact (road+timestamp) trùng không đáng kể → giữ raw append-only semantics, không thêm dedup logic (tránh overengineer/risk).
  - Failure/laptop behavior: per-road fail = best-effort trong summary (không chết campaign); cả cycle fail → report + chờ slot tiếp. Laptop sleep/off → không dữ liệu cho khoảng đó, mở lại tiếp tục slot tương lai, không giả lập/backfill. Ctrl+C dừng sạch (giữ raw đã append, không temp file, báo stopped). Raw vs Spark: collection mỗi slot + Spark batch/on-demand (cuối ngày/khi cần/trước ML), không Spark 192 lần/ngày; manual Streamlit update giữ nguyên.
  - Checkpoints ~1k/3k/5k audit road balance, hour coverage, weekday/weekend, rain coverage, target distribution (không chỉ row count). Stop khi ~3k–5k + nhiều ngày + weekday/weekend + nhiều windows + target/weather diversity; nếu không mưa thì ghi limitation, không fabricate.
  - Mock verification 27 checks PASS (temp CSV/mock cycle, sleep inject no-op, không real API): schedule 192 cycles/5 phút, next-slot/missed-slot, normal/partial/full-fail, stop-restart append, bounded validation, Ctrl+C stopped, progress helper, CLI --progress/--max-cycles-0, no-secret. Production 8 SHA256 PRE==POST.
   - Start: `.venv\Scripts\Activate.ps1` rồi `python historical_collector.py --max-cycles 192` (≈ 1 ngày). Stop: Ctrl+C. Restart: cùng command (append, không truncate). KHÔNG chạy production campaign trong Phase 10.4.
- [x] **Process-only CLI (`data_update.py --process-only`): COMPLETE** (mock 34 checks + 1 production run, 0 TomTom, 0 Open-Meteo; raw PRE==POST; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
  - `run_process_only()` tái dùng `_default_spark_pipeline()` (run_spark_etl + road/hourly/weather analytics), không duplicate ETL, không gọi collection/API (verified: collection calls = 0, requests.get = 0, AST body không chứa TomTom/Open-Meteo/requests).
  - CLI `python data_update.py --process-only` (argparse); `--help` không khởi động Spark; `import data_update` không side-effect; `run_data_update()` và nút Streamlit "Cập nhật dữ liệu" giữ nguyên.
  - Production run 01/10/2026: traffic input 147 / weather input 70 (dedup từ 142 raw) / combined 147 (matched 147, unmatched 0) / road 6 / hourly 6 / weather 2 / Parquet 147 rows; duration ~30s. Raw roads/traffic/weather byte-identical PRE==POST. Reconciliation: combined 147 = road_sum = hourly_sum; weather_sum = matched; Parquet↔CSV rows/coverage bằng nhau, key mismatches 0. Dashboard AppTest read-only: 0 exception, 6 roads (Dai Co Viet), 5 charts, 2 multiselects, 1 dataframe, update button nguyên vẹn, snapshot mới 147 obs.
- **Daily workflow (collection campaign):**
  - A. Thu dữ liệu: `python historical_collector.py --max-cycles 192`
  - B. Dừng khi muốn: `Ctrl+C` (giữ raw đã append, restart cùng command)
  - C. Process raw hiện có, không gọi API: `python data_update.py --process-only`
  - D. Dashboard: `streamlit run app.py`
  - Dashboard chỉ đọc processed snapshot (`data/processed/*.csv`), không đọc raw trực tiếp.
- [x] **Campaign auto-process (operational enhancement): COMPLETE** (mock 39 checks + import-safety, 0 real API, 0 real Spark, 0 real sleep; production artifacts PRE==POST byte-identical; không tạo phase mới, Phase 10 vẫn IN PROGRESS, Phase 10.4 vẫn COMPLETE/CAMPAIGN READY).
  - `config.py`: `AUTO_PROCESS_ENABLED = True`, `AUTO_PROCESS_EVERY_CYCLES = 12` (single source; 12 cycles × 5 phút ≈ 60 phút; không hardcode rải rác).
  - `historical_collector.run_campaign()` tái dùng `data_update.run_process_only()` (lazy import, không duplicate Spark pipeline; collection cycle vẫn gọi API đúng như trước, process thêm 0 API call). Attempt-based deterministic (summary hay error đều đếm): sau mỗi 12 attempts → 1 periodic process tuần tự (raw đã đóng/flush; không song song; không Spark mỗi 5 phút).
  - Failure isolation: process fail → báo lỗi sanitize, raw giữ nguyên (không rollback/xóa), campaign tiếp tục slot tương lai, không retry ngay; boundary/final kế tiếp thử lại.
  - Scheduling contract giữ nguyên: slot tiếp = `_next_strict(target, now() mới sau process)` nên slot lỡ trong lúc Spark chạy (~30s+) bị skip, không backfill.
  - Final process: kết thúc bình thường + đã có ≥1 periodic + còn cycles mới → 1 final (vd 17 cycles = process ở 12 + final); campaign ngắn hơn 1 interval (vd 11) thì không final; Ctrl+C thì không final Spark (stop nhanh/an toàn; limitation: không kill Spark phức tạp, current process cleanup rồi stop).
  - CLI: mặc định `python historical_collector.py --max-cycles 192` (auto-process ON); tắt bằng `--no-auto-process`. Status output: cycle info + `Auto-process starting...` + `success combined_rows/duration` hoặc `FAILED ... Raw data preserved. Campaign continues.` + next slot; không log secret (key=***).
  - `app.py` không sửa; dashboard đọc processed snapshot mới sau mỗi auto-process khi Streamlit rerun/refresh.
  - Manual fallback giữ nguyên: `python data_update.py --process-only` (khi auto-process fail hoặc Ctrl+C trước boundary).
- **Daily command (mới):** `python historical_collector.py --max-cycles 192` (collection mỗi 5 phút 06:00–22:00 + auto Spark process mỗi 12 cycles ≈ 1 giờ + final khi xong) → `streamlit run app.py`.
- [x] **Continuous collection mode (operational enhancement): COMPLETE** (mock 31 checks + finite regression + import-safety, 0 TomTom, 0 Open-Meteo, 0 real Spark, 0 real sleep; production artifacts PRE==POST byte-identical; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
  - CLI: `python historical_collector.py --continuous` (service chạy liên tục qua nhiều ngày, không max_cycles; mutually exclusive với `--max-cycles` → truyền cả hai bị argparse reject). Biến thể `python historical_collector.py --continuous --no-auto-process`. `--progress` giữ behavior cũ. Finite `python historical_collector.py --max-cycles N` giữ nguyên semantics (max cycles, periodic auto-process, final, Ctrl+C, missed-slot skip, no backfill).
  - Sampling window giữ nguyên 06:00–22:00 Asia/Ho_Chi_Minh (slots 06:00..21:55, start-inclusive/end-exclusive). Service có thể chạy 24/7 nhưng NGOÀI window 22:00–06:00 KHÔNG gọi API; scheduler chờ tới next valid slot bằng `_wait_until()` chunked 60s (không busy-loop, không wake vài giây, không backfill slot lỡ, không sleep 8 tiếng bằng 1 primitive uninterruptible; Ctrl+C phản hồi trên Windows). Mỗi overnight gap chỉ in 1 dòng `Outside collection window. Next collection slot: ...`.
  - Day transition: không restart process lúc sang ngày, không reset raw files (raw CSV append-only). Ví dụ 21:55 collect → overnight wait → hôm sau 06:00 collect tiếp.
  - Auto-process tái dùng `data_update.run_process_only()` (0 API call thêm): mỗi 12 collection ATTEMPTS thực tế trong valid window (summary hay error đều đếm) → 1 periodic process tuần tự. Overnight idle không đếm thành cycles, không Spark chỉ vì clock trôi qua. Counter attempts tiếp tục qua ngày trong cùng process (vd 7 cycles ngày 1 + 5 cycles ngày 2 → đủ 12 → process); không reset lúc 00:00.
  - Restart semantics (limitation, documented trong docstring): stop rồi start lại `--continuous` thì counter session mới bắt đầu lại từ 0 (không reconstruct từ raw CSV, không state DB/checkpoint file). Raw cũ giữ nguyên. Fallback luôn có: `python data_update.py --process-only`.
  - Error recovery: collection cycle lỗi → ghi error entry, raw đã ghi giữ nguyên, không crash service, slot kế tiếp thử lại. Auto-process/Spark lỗi → sanitize (key=***), báo `Raw data preserved. Campaign continues.`, service tiếp tục, không retry ngay; boundary kế tiếp thử lại. Fatal programming/config error (scheduling) không swallow vô hạn → propagate. Không framework retry phức tạp.
  - Ctrl+C: thoát sạch, giữ raw đã append, không truncate/temp sót, KHÔNG bắt buộc final Spark, báo stopped + hướng dẫn manual process-only. Muốn snapshot mới nhất sau Ctrl+C: `python data_update.py --process-only`.
  - Status output khi start: `Mode: CONTINUOUS / Collection interval: 5 minutes / Collection window: 06:00–22:00 Asia/Ho_Chi_Minh / Auto-process: ON|OFF / Auto-process every: 12 cycles`. Trong window mỗi cycle in `[Hanoi now] Cycle N traffic success=... weather success=... next slot=...`. Không spam, không log API key.
  - `app.py` không sửa; dashboard đọc processed snapshot mới sau mỗi auto-process khi Streamlit rerun/refresh.
  - Next operational step: Windows Task Scheduler startup automation (CHƯA làm ở task này; KHÔNG đánh dấu complete; KHÔNG tạo Phase 11; Phase 10 vẫn IN PROGRESS).
- **Continuous command:** `python historical_collector.py --continuous` (KHÔNG chạy production ở task này; Task Scheduler cấu hình ở bước sau).
- [x] **Near-realtime pipeline + dashboard auto-refresh: COMPLETE** (mock 53 checks: collector 22 + dashboard-refresh 15 + AppTest isolation/regression 16; 0 TomTom, 0 Open-Meteo, 0 real Spark, 0 real sleep; production artifacts PRE==POST byte-identical; không tạo phase mới, Phase 10 vẫn IN PROGRESS, không Phase 11, không ML, không cloud).
  - Collection: mỗi 5 phút trong window 06:00–22:00 Asia/Ho_Chi_Minh (slots 06:00..21:55, giữ nguyên).
  - Auto-process: `AUTO_PROCESS_EVERY_CYCLES = 1` (đổi từ 12) → mỗi collection attempt trong valid window đều collection → raw persistence hoàn tất → `run_process_only()` → Spark ETL + analytics → processed snapshot mới (near-realtime batch 5 phút, KHÔNG streaming). `CAMPAIGN_PROCESSING_CADENCE = "per-cycle"`. Tắt bằng `--no-auto-process` (cycles vẫn chạy, process calls = 0).
  - Scheduler/missed-slot: slot tiếp luôn tính bằng `_next_strict(target, now() mới SAU khi process xong)` nên giữ wall-clock slots (vd Spark 30s: 14:05 → 14:10) và skip slot lỡ (vd Spark tới 14:11: 14:10 skip → 14:15); không backfill/catch-up, không overlapping Spark jobs (tuyệt đối tuần tự, 1 job tại 1 thời điểm). Overnight 21:55 collect+process → 22:00–06:00 0 API/0 Spark → 06:00 collect+process.
  - Process failure: raw giữ nguyên, log sanitize (key=***), processed cũ không corrupt, service tiếp tục, cycle kế attempt lại; không retry trong cùng cycle, không crash service.
  - Dashboard: read-only auto-refresh khoảng 30 giây bằng native `st.fragment(run_every=30)` cho tick `_auto_refresh_tick()` (chỉ stat 4 files, không đọc nội dung) + full app rerun khi file signature đổi. Fragment toàn-dashboard KHÔNG dùng vì mọi section phụ thuộc cùng snapshot. Không dependency mới, không JavaScript hack. Header hiển thị `🟢 Tự động làm mới 30 giây` + `Cập nhật dữ liệu gần nhất: DD/MM/YYYY HH:mm` lấy từ DATA (latest observation, không phải giờ rerun).
  - Cache invalidation: `snapshot_signature()` (mtime_ns + size) làm cache key cho `_load_snapshots_cached()`; file mới → đọc mới, không stale (phát hiện và fix bug: param prefix `_` bị Streamlit loại khỏi cache key). Dashboard vẫn chỉ đọc processed snapshot, không đọc raw.
  - Snapshot consistency: Spark replace từng file atomically (temp + os.replace) nên không bao giờ đọc file viết dở; `load_dashboard_data_safe()` fallback snapshot trước + caption nhỏ nếu đọc lỗi, không crash/traceback lớn, không transaction/DB phức tạp.
  - Filter state: `detail_date`/`detail_roads`/`detail_levels` có explicit key nên auto-refresh giữ selection; user xem ngày cũ không bị ép về ngày mới. Nút `🔄 Cập nhật dữ liệu` giữ nguyên backend (chỉ chạy khi user click; auto-refresh không trigger).
  - Normal operation: `python historical_collector.py --continuous` + `streamlit run app.py` → không cần click update, không cần F5.
- [x] **Production dashboard UX + coherent snapshot publishing: COMPLETE** (mock 12 snapshot checks A–G + AppTest UI/isolation 57 checks PASS; 0 TomTom, 0 Open-Meteo, 0 real Spark, 0 production writes; production data + parquet PRE==POST byte-identical, không publish fake production marker; không tạo phase mới, Phase 10 vẫn IN PROGRESS, không Phase 11, không ML, không cloud).
  - Root cause warning 274/267: dashboard refresh rơi vào giữa lúc pipeline replace từng file riêng lẻ (mỗi file atomic, nhưng 4 files không đồng bộ) → transient mixed-version snapshot. Fix gốc bằng coherent publishing, không hide warning bằng CSS, không xóa validation backend.
  - Publishing: `data_update._default_spark_pipeline()` sau khi FULL pipeline (combined + 3 summaries + parquet) thành công mới gọi `publish_snapshot_marker(metas)` CUỐI CÙNG (temp sibling + os.replace, atomic) → `data/processed/snapshot_ready.json` {version ISO microseconds, published_at, combined/road/hourly/weather rows + sums, parquet/input rows}. Một hook duy nhất bao cả `run_process_only()` và manual `run_data_update()`; fail giữa chừng → không publish, marker cũ giữ nguyên. Result dicts thêm key `snapshot_marker` (tương thích ngược).
  - Dashboard watch marker version (`current_snapshot_key()`: marker → `("marker", version)`, bootstrap không marker → file-sig cũ) thay vì mtime từng file lẻ; chỉ load lại khi marker đổi nên không bao giờ đọc mixed-version. `load_coherent_snapshot()` validate schema/counts/marker, fail → giữ last-known-good + caption nhỏ "Đang đồng bộ dữ liệu mới...", diagnostic chi tiết ra terminal (`[dashboard] ...`), không warning banner; chỉ báo lỗi ngắn khi chưa từng có snapshot usable. Reconciliation warnings trong `calculate_overview_metrics` giữ ở backend, main chỉ print terminal.
  - UX production monitoring (dark, `assets/dashboard.css` tách khỏi `app.py`, không framework/CDN/JS hack): single hero `🚦 Hanoi Traffic Analytics` + subtitle "Theo dõi giao thông và thời tiết tại Hà Nội" (xóa title duplicate + đoạn Spark); header phải `● ĐANG THEO DÕI` + `Cập nhật: HH:MM` (từ DATA) + `Tự động · 30s`; sidebar control panel (`● Hệ thống hoạt động`, N Quan sát/Tuyến/Cập nhật, QUẢN LÝ TUYẾN, `TomTom · Open-Meteo`); 4 KPI uppercase (TUYẾN ĐƯỜNG/TỐC ĐỘ TRUNG BÌNH/QUAN SÁT/ÙN TẮC TRUNG BÌNH) radius 16px + hover translateY(-2px); grid 60/40 map+charts, row 65/35 latest+weather; detail gom dưới "Phân tích chi tiết" (filters + table + expander bổ sung giữ nguyên); nút cập nhật thành secondary (auto-refresh không trigger, backend giữ nguyên); smooth scroll + scrollbar dark mảnh + transitions ~200ms, không animation lòe loẹt.
  - Near-realtime cadence giữ nguyên: API 5 phút → Spark per cycle → dashboard ~30 giây. Không đổi congestion/weather-join/Spark/analytics formulas.
- [x] **Dashboard detail table single-day filter (UI/read-only, `app.py` only): COMPLETE** (AppTest + helper regression 26 checks PASS, 0 real API/Spark; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
- [x] **Dashboard redesigned to card/grid layout inspired by modern traffic dashboard (UI/layout refactor, `app.py` only): COMPLETE** (AppTest + helper regression 36 checks PASS, requests.get/post blocked = 0 calls, 0 real API/Spark; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
  - Header + KPI + map/charts + latest traffic + weather + daily detail. Dark navy theme (wide layout, card 12px radius, subtle borders; không framework/JS mới). Header: 🚦 Hanoi Traffic Analytics + subtitle + "Cập nhật lần cuối" + nút 🔄 giữ nguyên backend Phase 9.2 (chuyển từ sidebar lên header). Sidebar gọn: title, snapshot info, Quản lý tuyến đường, nguồn TomTom/Open-Meteo (không key); KPI luôn global, không filter nào đổi KPI.
  - 4 KPI cards từ metrics hiện có (monitored roads, avg speed km/h, observations, avg congestion %; không fake delta). Grid 60/40: Folium map (latest actual observations, legend 🟢🟡🟠🔴, popup đầy đủ, cao 650) + hourly avg_speed line (cùng hourly_summary artifact, presentation-only) + road congestion hbar. Hàng 2 (65/35): latest traffic table (1 obs/tuyến, 6 cột) + weather card từ matched mới nhất (temp/humidity/rain/wind/code + note mô tả phi causal; không gọi Open-Meteo). Detail theo ngày giữ nguyên phía dưới. Còn lại (hourly congestion, road speed, weather bars, insights) vào expander "Phân tích bổ sung" — không mất functionality Phase 7–10.
  - Dashboard detail table now supports single-day filtering, defaulting to latest available observation date. `Ngày xem` selectbox (options = ngày thực sự có trong combined data, newest → oldest, hiển thị DD/MM/YYYY); lọc date → roads → statuses chỉ áp dụng cho bảng chi tiết; KPI/bản đồ/5 charts giữ nguyên full snapshot. Text `Hiển thị X / Y quan sát trong ngày DD/MM/YYYY`; empty → message thân thiện, không crash. Không đụng API/collector/Spark/CSV/Parquet/road-management/update-pipeline.
- [x] **Versioned processed snapshot + atomic pointer (fix mixed-version overwrite): COMPLETE** (mock 41 versioned checks A–H + 14 pipeline orchestration checks PASS; 0 TomTom, 0 Open-Meteo, 0 real Spark; production CSV/Parquet PRE==POST byte-identical, chưa publish versioned production pointer; không tạo phase mới, Phase 10 vẫn IN PROGRESS, không Phase 11, không ML, không cloud).
  - Root cause đã xác nhận: `snapshot_ready.json` cũ (407) + pipeline cycle mới replace `combined_data.csv` thành 414 trước khi summaries + marker mới hoàn tất → dashboard phát hiện mismatch đúng nhưng sau Streamlit restart RAM last-known-good mất và snapshot vật lý 407 đã bị overwrite → "Chưa có dữ liệu hiển thị". Fix bằng versioned snapshots, không nới validation, không bỏ marker check, không hide error, không đọc mixed files.
  - Pipeline (`data_update._default_spark_pipeline`) build snapshot mới trong `data/processed/snapshots/<version>/` riêng (version UTC `YYYYMMDDTHHMMSS_microsecondsZ`, sortable, Windows-safe không ":"); trong lúc build `current_snapshot.json` vẫn trỏ A, dashboard chỉ đọc A. Analytics đọc staging combined qua `combined_path` mới (backward compat `None` → legacy). Sau FULL pipeline + validate counts PASS (combined == road_sum == hourly_sum; weather_sum == matched; 4 files tồn tại) mới publish pointer atomically (temp sibling + `os.replace`) A → B. Build fail → xóa staging incomplete, pointer giữ A. Invalid counts → không publish, pointer giữ A.
  - Pointer `data/processed/current_snapshot.json` chứa version, published_at, relative `snapshots/<version>`, combined_rows, road/hourly/weather observation sums (+ rows chi tiết); không absolute Windows path. Dashboard authoritative read path: `current_snapshot.json` → `snapshots/<version>/` (4 CSV trong cùng directory, validate vs pointer). Auto-refresh chỉ watch pointer/version (`("versioned", version)`); pointer không đổi → không đổi snapshot; pointer đổi → load version mới. RAM fallback vẫn giữ nhưng correctness không phụ thuộc RAM (restart vẫn đọc disk pointer A).
  - Bootstrap/migration: chưa có pointer → dashboard dùng legacy coherent snapshot nếu validate được (marker cũ hoặc internal reconciliation); lần process-only/continuous thành công tiếp theo tạo versioned snapshot đầu tiên + pointer; không copy legacy mixed vào versioned storage. Legacy `data/processed/*.csv` + `snapshot_ready.json` giữ làm compat (refresh best-effort sau publish, không authoritative).
  - Parquet canonical (`data/lake/curated/combined/combined_data.parquet`) giữ ngoài versioned atomic scope (vẫn overwrite per-file atomic trong ETL; dashboard không đọc Parquet) — đã document trong `data_update.py`.
  - Retention: giữ latest 3 snapshots sau successful publish; không bao giờ xóa directory mà pointer đang trỏ; cleanup fail không fail pipeline.
  - Near-realtime giữ nguyên: API 5 phút, `AUTO_PROCESS_EVERY_CYCLES = 1`, Spark per-cycle tuần tự, dashboard ~30s read-only (tick chỉ `current_snapshot_key`, 0 API, 0 Spark), missed slot skip, no backfill, manual update button, process-only, continuous mode.
- [x] **Dashboard dark hiện đại + 8 biểu đồ trực quan (UI/presentation only): COMPLETE** (mock 15 helper checks + AppTest smoke/interaction PASS, 0 TomTom, 0 Open-Meteo, 0 real Spark; production CSV/pointer PRE==POST byte-identical trong lúc verify; chỉ sửa `app.py`, `assets/dashboard.css`, `tests/test_dashboard_viz.py` (mới); không tạo phase mới, Phase 10 vẫn IN PROGRESS, không Phase 11, không ML, không cloud).
  - Giao diện Modern Dark (bg #0B1020 / sidebar #101827 / card #151F32 / border #27344A / text #F1F5F9 / muted #94A3B8 / accent #38BDF8·#818CF8; trạng thái #22C55E/#EAB308/#F97316/#EF4444 đồng nhất KPI/bản đồ/biểu đồ/chú giải/bảng). Header "Phân tích giao thông Hà Nội" + phụ đề hệ thống giám sát; trạng thái dữ liệu trung thực (Dữ liệu sẵn sàng / Đang đồng bộ / Dữ liệu đã cũ / Chưa có dữ liệu — không suy diễn collector đang chạy); KPI 4 cards chiều cao đồng nhất + ghi rõ phạm vi toàn bộ snapshot; sidebar gọn (tiêu đề tiếng Việt, trạng thái, tổng lượt ghi nhận/số tuyến/cập nhật gần nhất, quản lý tuyến bullet từng dòng có dấu, footer "Nguồn dữ liệu: TomTom · Open-Meteo"); bảng chi tiết + nút tải CSV đã lọc.
  - 8 biểu đồ Plotly tiếng Việt (tiêu đề/tooltip/trục/đơn vị), tổ chức 3 tabs (Tổng quan: tốc độ theo giờ theo tuyến + ùn tắc theo tuyến; Chuyên sâu: heatmap tuyến×giờ ô trống NaN + xu hướng multi-tuyến + phân bố stacked 100% + so sánh Thứ Hai–Chủ Nhật; Thời tiết: mưa–ùn tắc + nhiệt độ–tốc độ, mô tả phi causal). Mỗi biểu đồ empty-state riêng, lỗi một biểu đồ không giết dashboard; không tiêu đề lặp Streamlit/Plotly; không hardcode số liệu; không nối khoảng thiếu gây hiểu nhầm; không hồi quy/dự đoán.
  - Bản đồ: GIỚI HẠN đã xác minh — CartoDB dark_matter trên folium 0.20 yêu cầu API key ("CartoDB tiles now require an API key"), trái quy tắc không thêm key → fallback OpenStreetMap mặc định (sáng dịu, không key, attribution tự động), không dùng URL tile lách key; marker tại tọa độ đại diện (không vẽ road geometry giả), popup/tooltip/chú giải tiếng Việt, caption ghi rõ thời điểm quan sát gần nhất (không phải realtime).
  - Bộ lọc toàn cục (tuyến + ngày, mặc định toàn bộ dữ liệu thực tế) cho 8 biểu đồ; KPI luôn global; bảng chi tiết giữ bộ lọc riêng (ngày/tuyến/trạng thái); chú thích phạm vi mỗi khu vực. road_name gốc giữ nguyên không dấu; ánh xạ có dấu chỉ ở presentation (Nguyễn Trãi/Trường Chinh/Giải Phóng/Cầu Giấy/Xuân Thủy/Đại Cồ Việt/Kim Mã). Định dạng Việt (24,2 · 36,9% · 1.351 · DD/MM/YYYY HH:MM, Asia/Ho_Chi_Minh). Ngưỡng ùn tắc, công thức congestion, Spark ETL, collector, versioned snapshot, pointer format, cache invalidation giữ nguyên.
- [x] **UI cleanup & polish (presentation only): COMPLETE** (mock 17 helper checks + AppTest smoke/interaction PASS, 0 TomTom, 0 Open-Meteo, 0 real Spark; chỉ sửa `app.py`, `assets/dashboard.css`, `tests/test_dashboard_viz.py`; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
  - Xóa các chú thích kỹ thuật dài trên dashboard (phạm vi KPI/snapshot, vị trí đại diện, giới hạn dark tile, ngưỡng, nội suy, realtime...); giới hạn dữ liệu quan trọng chuyển vào expander "Thông tin phương pháp và giới hạn dữ liệu" (thu gọn, cuối trang). Giữ tiêu đề, nhãn, đơn vị, tooltip, thời điểm cập nhật, lỗi quan trọng, cảnh báo dữ liệu cũ, empty state, attribution OSM; không còn chữ "Phase"/snapshot-internals trong UI.
  - Bộ lọc gọn: multiselect mặc định trống = tất cả (placeholder "Tất cả tuyến đường" / "Toàn bộ thời gian có dữ liệu" / "Tất cả trạng thái"), hết chip đỏ khi không lọc; helper mới `resolve_all_or_selected()` (pure); logic tổng hợp giữ nguyên. Sidebar: danh sách tuyến vào expander "Danh sách tuyến đang theo dõi". Tiêu đề biểu đồ rút gọn (bỏ tiền tố số), spacing section-header 1.5→1rem, xóa CSS class không dùng. Tile provider, 8 biểu đồ, 3 tabs, KPI, bản đồ/marker, thêm/xóa tuyến, versioned snapshot, auto-refresh giữ nguyên.
- [x] **Tìm kiếm + thêm tuyến đường Hà Nội (Nominatim, UI/presentation + utils): COMPLETE** (mock 10 search checks + 17 viz regression + AppTest smoke/search-flow PASS, 0 Nominatim/TomTom/Open-Meteo that, 0 real Spark, production CSV PRE==POST trong luc verify; mới `utils/geocoding.py` + `tests/test_road_search.py`, sửa `app.py`; `config.py`/`collector.py`/`road_manager.py`/schema giữ nguyên; không tạo phase mới, Phase 10 vẫn IN PROGRESS).
  - Nguyên nhân không dấu: `roads.csv` + catalog lưu `road_name` ASCII; map hiển thị cũ chỉ 7 tuyến. Giữ khóa nội bộ ASCII cho tuyến hiện có (tương thích raw/processed/Spark join/snapshot); mở rộng map hiển thị đủ 12 tuyến (Láng Hạ, Nguyễn Chí Thanh, Trần Duy Hưng, Lê Văn Lương, Phạm Hùng...). Tuyến mới lưu tên có dấu do Nominatim cung cấp (chưa có lịch sử nên không xung đột khóa); không đổi hàng loạt tên lịch sử.
  - Geocoding Nominatim công khai (không key): chỉ gọi khi nhấn "Tìm tuyến đường" (không autocomplete), User-Agent định danh, rate-limit client 1.1s/call, cache process 100 query (key bỏ dấu), timeout 10s, viewbox+bouned Hà Nội + lọc client (address Hà Nội/bbox, class=highway, tọa độ hợp lệ làm tròn 4 decimals). Kết quả trùng tên hiện radio kèm địa chỉ để chọn; trùng gần đúng (bỏ dấu/tiền tố) bị từ chối thân thiện.
  - Thêm tuyến: qua `road_manager.add_road` (validation/duplicate/atomic giữ nguyên), không gọi TomTom ngay; `collector.load_roads()` đọc tươi `roads.csv` mỗi cycle nên nhận tuyến mới ở chu kỳ tiếp theo (đã kiểm chứng trên CSV tạm). UI dark tiếng Việt: giữ thêm nhanh catalog + form tìm kiếm, spinner, thông báo tìm thấy/không thấy/lỗi, xác nhận thành công; giữ xóa tuyến; ghi rõ theo dõi tọa độ đại diện.

---

## 7. Decision Log

- Dùng Spark Local Mode vì project môn học chạy trên laptop.
- PySpark là processing engine chính cho cleaning/transformation/aggregation/analytics.
- Pandas chỉ dùng ở presentation layer hoặc thao tác nhỏ, không thay Spark cho ETL/analytics.
- CSV làm storage cho MVP (`roads.csv`, `traffic_data.csv`, `processed/*.csv`).
- Không dùng database trong MVP.
- Không dùng Kafka/Hadoop/Docker/K8s trong MVP.
- Machine Learning không thuộc MVP.
- Secret (TomTom key, mọi credential) nằm trong `.env`, không hard-code, không commit `.env`.
- Làm 3 file chính trước (`collector.py`, `spark_analysis.py`, `app.py`), refactor `utils/` sau nếu cần.
- `PROJECT_CONTEXT.md` là source of truth; mâu thuẫn phải báo người dùng, không tự quyết.
- Phase 2 TomTom Traffic Collector completed (2.1 config → 2.7 final verification PASS).
- Raw production traffic dataset (`data/traffic_data.csv`) contains 10 real observations, schema 9 fields; must be preserved.
- Phase 3 will integrate Open-Meteo weather data.
- Temporal join contract traffic ↔ weather (Buoc 3.5): logical key = road_name + nearest timestamp within 60 minutes (WEATHER_JOIN_TOLERANCE_MINUTES = 60); tie → prefer past; no match → weather unavailable (no fake data); khong join bang lat/lon equality; reference helper find_nearest_weather_record() trong utils/weather_api.py, Spark ETL Phase 5 phai implement equivalent semantics.
- Phase 3 Weather Integration completed (3.1 client → 3.7 final verification PASS).
- Raw datasets remain separate: traffic_data.csv = TomTom raw observations; weather_data.csv = Open-Meteo raw observations.
- Spark will implement equivalent integration semantics in Phase 5.
- Controlled historical scheduler (Buoc 4.4): module historical_collector.py dung configured 15-min slots + active windows; max_cycles bat buoc (> 0), KHONG infinite mode; slot semantics start-inclusive/end-exclusive; next >= now, sau cycle dung strict-after (khong duplicate); missed slots skipped, khong backfill; fatal cycle error -> ghi error, count attempt, sang slot tiep (khong retry ngay); scheduling dung Asia/Ho_Chi_Minh timezone-aware datetimes (reject naive).
- Historical collection cadence (Buoc 4.3): interval 15 min, windows 06:00–10:00 + 16:00–20:00 Asia/Ho_Chi_Minh (start inclusive, end exclusive); 5 roads → 32 cycles/day (16+16); estimate 160 TomTom + 160 Open-Meteo requests/day, 4,800/30 days; configured budget 20,000, safety limit 16,000 (80%); pure helper estimate_collection_budget() trong config.py, KHONG scheduler/quota tracker o buoc nay.
- Phase 4 Final Verification PASS (Buoc 4.6: 53 checks, 0 API, raw datasets byte-exact unchanged).
- Production raw state at Phase 4 close: traffic_data.csv = 15 observations / 5 roads; weather_data.csv = 10 observations / 5 roads.
- Cross-source diagnostic at close: 15/15 traffic observations matched weather within locked 60-minute temporal contract.
- Phase 5 raw input contract: data/traffic_data.csv + data/weather_data.csv la RAW INPUT chi duoc READ; Spark KHONG duoc rewrite/normalize/dedup in-place, append derived columns (congestion), merge weather vao raw traffic, hay xoa observations; moi output dan xuat vao data/processed/.
- Buoc 5.1 SparkSession + explicit raw schemas PASS (local[*], Spark 4.2.0; traffic 9 fields + weather 12 fields, timestamp StringType; counts 15/10 khop raw; 0 nulls).
- Buoc 5.2 traffic validation + timestamp normalization PASS: validate_and_normalize_traffic() fail-fast (khong sua/drop rows), event_timestamp TimestampType (session tz UTC, Asia/Ho_Chi_Minh chi dung cho scheduling); production 15/15 valid, 0 parse failures.
- Buoc 5.3 weather validation + timestamp normalization PASS: validate_and_normalize_weather() fail-fast (raw timestamp bat buoc timezone-aware, weather_code chi NOT NULL); production 10/10 valid; traffic regression 15/15 OK.
- Buoc 5.4 traffic derived fields: derive_traffic_fields() pure Spark (column expressions, khong UDF/IO) — congestion_percent dung locked raw formula (1 - current_speed / free_flow_speed) * 100 KHONG clamp (am la hop le, DoubleType full precision); congestion_level theo locked boundaries (when <20 / <=40 / <=60 / otherwise, labels co dau); event_timestamp canonical UTC (session tz UTC giu nguyen), local_hour = hour(from_utc_timestamp(event_timestamp, Asia/Ho_Chi_Minh)); output 13 cot (9 raw + event_timestamp + 3 derived), khong drop row; KHONG weather dedup/join/analytics/export.
- Buoc 5.4 floating-point verification note (Option B): congestion formula remains unclamped Double arithmetic; formula verification uses floating-point tolerance (vd 80/100 ≈ 20, khong doi exact 20.0); classification thresholds tested independently using explicit congestion values via classify_congestion() helper (extraction, khong doi semantics); no epsilon/rounding/clamping introduced.
- Buoc 5.5 weather dedup PASS: deduplicate_weather() key = (road_name, event_timestamp) (shared timestamps across roads la hop le); conflicting duplicates chon canonical row theo fixed ASC order vi raw khong co ingestion-order metadata; production 10 -> 10 (0 duplicate groups); raw CSV non-destructive.
- Buoc 5.6 temporal join PASS: traffic-weather join theo same road + nearest weather (<=60 min inclusive, equal-distance tie prefers past, deterministic final ordering); unmatched traffic preserved (LEFT); toi da 1 weather/observation; production 15 -> 15 (matched 15/15, diff 4.4-44.65 min); exact duplicate traffic fails fast vi raw schema lacks ingestion identity.
- Buoc 5.7 processed export PASS (runtime verified 5.7b): base snapshot la data/processed/combined_data.csv (15 rows, deterministic Spark orderBy); Spark thuc hien moi ETL/transform/join/orderBy + UTC timestamp format; Windows local thieu Hadoop/winutils nen snapshot MVP nho duoc serialize driver-side bang Python stdlib csv (khong .write.csv, khong Pandas, non-scalable, chi cho demo mon hoc); snapshot replacement nguyen tu qua temp sibling + os.replace; raw inputs immutable.
- Phase 5 closeout PASS (Buoc 5.8 final audit, 0 Spark rerun, 0 API): 22 checks PASS — code compile/import safe, raw schemas 9/12 hop le va byte-exact, combined_data.csv 25 cot / 15 rows / matched 15/15, congestion formula + classification + local_hour + UTC instants + diff + deterministic order + road coverage 5/5 deu PASS, khong lan Phase 6; output contract Phase 5 la row-level combined_data.csv (thay the phac thao 3-file cu).
- Buoc 6.1 road-level analytics PASS: Phase 6 tieu thu combined_data.csv lam ETL boundary (khong doc raw, khong rerun join); aggregation + ordering DO SPARK (groupBy/count/avg/min/max/sum-when, khong UDF/Pandas/round); avg_congestion_percent la descriptive comparison metric (khong claim causality); output data/processed/road_summary.csv (5 roads, sum obs 15/15, top Truong Chinh ~39.64); single-file serialization tai dung approved MVP driver-side temp+os.replace.
- Buoc 6.2 hourly analytics PASS: dung Phase 5 local_hour (Asia/Ho_Chi_Minh, khong recompute timezone, khong aggregate UTC hour); fixed time-window boundaries (00-05 Dem, 06-10 Sang cao diem, 11-15 Trua, 16-20 Chieu cao diem, 21-23 Toi); aggregation + ordering DO SPARK (groupBy local_hour, khong UDF/Pandas/round, fail-fast hour/level invalid); output data/processed/hourly_summary.csv (2 observed hours 11/13, sum obs 15/15, top hour 11 Trua ~35.19); driver serialization chi final MVP output.
- Buoc 6.3 rain-vs-no-rain PASS: chi dung matched weather observations (unmatched excluded, never No-rain); rain > 0 = Mua, rain == 0 = Khong mua (khong threshold, khong weather_code/precipitation override); descriptive association only (khong causality); aggregation DO SPARK (filter/groupBy, khong UDF/Pandas/round, fail-fast rain/match invalid); output data/processed/weather_summary.csv (Khong mua 13 obs avg ~26.15/~35.57, Mua 2 obs avg 37.5/~20.43); final serialization approved driver-side MVP.
- Phase 6 closeout PASS (Buoc 6.4 final audit, 0 Spark rerun, 0 API): static + 3 artifact audits + independent reconciliations (road/hourly/weather) deu PASS — combined 15 == road_sum 15 == hourly_sum 15, weather_sum 15 == matched 15, roads 5/5/5 consistent, ca 5 locked analyses co output, descriptive-only, processed dir dung 4 files, moi hash unchanged, khong lan Phase 7.
- Buoc 7.1 dashboard foundation PASS: Streamlit/pandas chi o presentation layer, tieu thu 4 processed snapshots (khong Spark/API, khong ghi/sua CSV); page wide + cached loader + file/schema/empty guards (error + stop, khong tu tao artifact); sidebar thong tin + snapshot freshness Asia/Ho_Chi_Minh (khong claim live); smoke test server 200 + AppTest headless khong exception; moi hash unchanged.
- Buoc 7.2 overview KPI PASS: 6 KPI tu processed snapshot (Tuyen duong / Quan sat / Toc do TB / Un tac TB / Ghep thoi tiet / Du lieu moi nhat dd/mm/YYYY HH:MM); pandas aggregation presentation-level (khong recreate Spark analytics); boolean weather_matched chuan hoa strict (string "False" khong truthy); reconciliation canh bao thay vi crash; highlight tuyen un tac nhat + dong weather mo ta collected-data (khong delta, khong live claim); moi hash unchanged.
- Buoc 7.3 Folium map PASS: 1 latest processed observation/road (deterministic event+secondary ordering, khong dua input order); center tu mean toa do markers hien co (zoom 12); mau marker theo Phase 5 congestion_level (xanh/cam/do/do dam); popup escape HTML, gio Hanoi dd/mm/YYYY HH:MM, weather chi khi matched; legend toi gian; map la snapshot (khong live); production 5 markers hop le; moi hash unchanged.
- Buoc 7.4 Plotly charts PASS: 5 charts (2 road hbar + 1 hourly line+markers + 2 weather bars) tieu thu truc tiep Spark summaries (khong re-aggregate combined, khong raw/Spark/API); ordering deterministic (road avg DESC+name, hour ASC, weather semantic); khong tao gio/condition thieu; km/h va % tren truc rieng; weather descriptive non-causal; helpers pure co validation + khong mutate source; dung width='stretch' (khong deprecated use_container_width); moi hash unchanged.
- Buoc 7.5 detail table + filters PASS: 2 multiselect (tuyen alpha, trang thai semantic hien co) chi loc bang chi tiet (exact AND, copy, default all); KPI/map/charts giu snapshot day du (AppTest: loc Cau Giay -> 3 rows, KPI khong doi; empty -> thong bao than thien); bang 8 cot, event DESC + road ASC, gio Hanoi dd/mm/YYYY, weather unmatched hien "Khong co du lieu" (khong thanh 0/khong mua); st.dataframe read-only (khong editor/download); moi hash unchanged.
- Buoc 7.6 final closeout PASS: Phase 7 acceptance passed; dashboard remains processed-snapshot presentation layer; 6 KPI + Folium map + 5 Plotly charts + isolated detail filters/table verified; Spark executions = 0; TomTom requests = 0; Open-Meteo requests = 0; source/processed artifacts remained byte-identical; temporary audit artifacts removed.
- MySQL được chọn làm mục tiêu persistence cho phiên bản nâng cấp sau khi CSV MVP hoàn thành ổn định. Phase 8 hiện tại vẫn sử dụng `roads.csv`; chưa cài database, chưa tạo schema, chưa thêm connector.
- [Phase 10.2] Local File-based Data Lake supersedes planned MySQL direction cho MVP mở rộng (giữ dòng MySQL trên làm lịch sử): Raw = CSV observations; Curated = CSV compatibility (`data/processed/`) + Parquet analytical (`data/lake/curated/combined/`, PyArrow driver-side fallback vì native Spark writer thiếu HADOOP_HOME/winutils); ML zone reserved. Không DB server/HDFS/S3/Delta/Iceberg.
- Phase 8 closeout PASS: 8.1 core (91 checks) + 8.2 UI + catalog dropdown (13 entries) + remove regression + AppTest + smoke 200, TomTom = 0, Open-Meteo = 0, Spark = 0; user e2e thêm Dai Co Viet qua UI (roads.csv 5 → 6, dashboard coverage 5/6); production automated-test immutability giữ trong closeout; chuyển CURRENT_PHASE = 9.
- Phase 9 closeout PASS: orchestrator + controlled collection/recovery + shadowing fix + one-click UI + user browser E2E (2 updates: traffic 21 → 33, weather 16 → 28, combined 33, hourly 3 rows; roads 6 giữ nguyên); chuyển CURRENT_PHASE = 10 (phase cuối, không có Phase 11).
- Coherent snapshot publishing: dashboard chỉ nhận snapshot mới khi `data/processed/snapshot_ready.json` (publish atomic cuối pipeline) đổi version; transient mixed-version giữ last-known-good, diagnostics ra terminal, không warning backend lên normal UI. Dashboard UX theo phong cách production monitoring (single hero, control-panel sidebar, KPI hover, smooth scroll), cadence near-realtime giữ nguyên.
- Versioned processed snapshot architecture (fix overwrite bug): dashboard authoritative read path là `data/processed/current_snapshot.json` → `data/processed/snapshots/<version>/` (4 CSV cùng version, validate vs pointer); atomic pointer publication guarantees old coherent snapshot remains available while new Spark batch is processing. Legacy `data/processed/*.csv` + `snapshot_ready.json` retained only for bootstrap/compatibility (not authoritative). Phase 10 remains IN PROGRESS.

---

## 8. Trạng thái hiện tại

```text
CURRENT_PHASE = 10
LAST_COMPLETED_PHASE = 9
NEXT_PHASE = —
```

- Phase 0: HOÀN THÀNH.
- Phase 1 (Project Foundation): HOÀN THÀNH — đã được người dùng xác nhận.
- Phase 2 (TomTom Traffic): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 3 (Weather Integration): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 4 (Historical Data Collection): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 5 (Spark ETL): HOÀN THÀNH — final verification 5.8 PASS (22 checks, 0 Spark rerun, 0 API).
- Phase 6 (Spark Analytics): HOÀN THÀNH — final verification 6.4 PASS (static + reconciliations, 0 Spark rerun, 0 API).
- Phase 7 (Streamlit Dashboard): HOÀN THÀNH — final verification 7.6 PASS (audit + closeout).
- Phase 8 (Road Management): HOÀN THÀNH — closeout PASS (8.1 core + 8.2 UI + catalog dropdown, user e2e add Dai Co Viet, 0 API, 0 Spark).
- Phase 9 (One-click Update): HOÀN THÀNH — closeout PASS (backend + UI + user browser E2E 2 lần update thành công).
- Phase 10 (Testing & Demo Preparation): CURRENT / IN PROGRESS.
- Phase 10 là phase cuối cùng của roadmap (0 → 10); không có Phase 11.
- Không tự chuyển `CURRENT_PHASE`; việc chuyển phase cần người dùng xác nhận.
