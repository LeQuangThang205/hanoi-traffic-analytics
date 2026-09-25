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

### Phase 5 — Spark ETL (CURRENT_PHASE — ĐANG THỰC HIỆN)

Nội dung (bắt buộc dùng PySpark):

- Cleaning, casting kiểu, tính `congestion`, tách `hour/day`, phân loại trạng thái, aggregation.

Acceptance criteria:

- [ ] `spark_analysis.py` chạy `local[*]` thành công, không dùng Pandas thay Spark cho ETL.
- [ ] Xử lý null / sai kiểu / chia cho 0 (`free_flow_speed`).
- [ ] Sinh đủ 3 file `processed/road_summary.csv`, `hourly_summary.csv`, `weather_summary.csv`.
- [ ] Chạy lại nhiều lần cho kết quả nhất quán (idempotent ở mức aggregation).

### Phase 6 — Spark Analytics

Nội dung:

- Thực hiện đủ 5 bài toán phân tích chính bằng Spark.

Acceptance criteria:

- [ ] Mỗi bài toán trong mục 5 có output tương ứng trong `processed/`.
- [ ] Kết quả khớp công thức congestion và ngưỡng phân loại.
- [ ] Có thể giải thích truy vấn/transform cho giảng viên.
- [ ] Không thay Spark bằng Pandas cho analytics.

### Phase 7 — Streamlit Dashboard

Nội dung:

- Metrics, map, 4 charts, table, filters, sidebar.

Acceptance criteria:

- [ ] `streamlit run app.py` chạy được.
- [ ] Map Hà Nội có marker màu xanh/vàng/cam/đỏ + popup (speed, free speed, congestion, weather).
- [ ] 4 charts: speed theo thời gian (line), ùn tắc từng tuyến (bar), ùn tắc theo giờ (bar/line), mưa vs không mưa (bar).
- [ ] Bảng dữ liệu có filter tuyến + trạng thái.
- [ ] Sidebar có checklist tuyến, nút thêm tuyến, nút cập nhật dữ liệu.
- [ ] Chỉ dùng Pandas ở presentation layer nếu cần, Spark vẫn là engine xử lý.

### Phase 8 — Road Management

Nội dung:

- Thêm/xóa tuyến, quản lý `roads.csv` từ code và từ UI.

Acceptance criteria:

- [ ] Thêm tuyến mới bằng `(road_name, lat, lon)` hợp lệ; từ chối trùng tên/tọa độ sai.
- [ ] Xóa tuyến khỏi `roads.csv` và dashboard cập nhật.
- [ ] Tuyến mới xuất hiện trên map sau khi cập nhật dữ liệu.
- [ ] Không phá dữ liệu lịch sử khi sửa `roads.csv`.

### Phase 9 — One-click Update

Nội dung:

- Một nút thao tác chạy full luồng Collector → Spark → Dashboard refresh.

Acceptance criteria:

- [ ] Bấm `Cập nhật dữ liệu` / `Analyze Data` kích hoạt collector rồi Spark job.
- [ ] Dashboard refresh hiển thị dữ liệu mới không cần restart thủ công.
- [ ] Lỗi ở một bước được báo rõ, không treo UI.
- [ ] Thời gian chạy chấp nhận được cho demo trên laptop.

### Phase 10 — Testing & Demo

Nội dung:

- Error handling, backup dataset, README, chuẩn bị demo.

Acceptance criteria:

- [ ] Có xử lý lỗi API/Spark/file cơ bản trên toàn luồng.
- [ ] Có backup dataset chạy offline được.
- [ ] `README.md` ghi cách cài, cấu hình `.env`, chạy collector/Spark/dashboard.
- [ ] Kịch bản demo 8 bước chạy end-to-end (mở dashboard → xem map → cập nhật → Spark → refresh → thêm tuyến → cập nhật → marker mới).

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

---

## 8. Trạng thái hiện tại

```text
CURRENT_PHASE = 5
LAST_COMPLETED_PHASE = 4
NEXT_PHASE = 6
```

- Phase 0: HOÀN THÀNH.
- Phase 1 (Project Foundation): HOÀN THÀNH — đã được người dùng xác nhận.
- Phase 2 (TomTom Traffic): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 3 (Weather Integration): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 4 (Historical Data Collection): HOÀN THÀNH — đã được người dùng xác nhận (final verification PASS).
- Phase 5 (Spark ETL): ĐANG THỰC HIỆN.
- Phase 6–10: CHƯA LÀM.
- Không được triển khai Phase 6 trước khi Phase 5 được người dùng xác nhận hoàn thành.
- Không tự chuyển `CURRENT_PHASE`; việc chuyển phase cần người dùng xác nhận.
