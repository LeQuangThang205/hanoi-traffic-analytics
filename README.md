# Hanoi Traffic Analytics

**Hệ thống thu thập, xử lý và trực quan hóa dữ liệu giao thông kết hợp thời tiết tại Hà Nội**

> **Phiên bản tài liệu:** Phase 10 — Sẵn sàng Demo  
> **Kiến trúc:** Python Collector · Apache Spark (PySpark Local Mode) · CSV · Streamlit · Plotly · Folium · Docker

---

## Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Cấu trúc thư mục cốt lõi](#3-cấu-trúc-thư-mục-cốt-lõi)
4. [Yêu cầu hệ thống](#4-yêu-cầu-hệ-thống)
5. [Hướng dẫn khởi chạy bằng Docker](#5-hướng-dẫn-khởi-chạy-bằng-docker)
6. [Dừng, giám sát và xử lý sự cố](#6-dừng-giám-sát-và-xử-lý-sự-cố)
7. [Phạm vi và giới hạn](#7-phạm-vi-và-giới-hạn)
8. [Trạng thái dự án và tài liệu tham chiếu](#8-trạng-thái-dự-án-và-tài-liệu-tham-chiếu)

---

## 1. Tổng quan dự án

**Hanoi Traffic Analytics** là hệ thống thu thập, xử lý và trực quan hóa dữ liệu giao thông kết hợp thời tiết tại Hà Nội. Hệ thống sử dụng **Apache Spark (PySpark Local Mode)** cho xử lý lô (batch ETL) và **Streamlit** cho lớp trình bày, kết hợp **Plotly** để vẽ biểu đồ và **Folium** để hiển thị bản đồ.

Hệ thống thực hiện các chức năng chính:

- Thu thập dữ liệu giao thông thực tế từ **TomTom Traffic Flow API**.
- Thu thập dữ liệu thời tiết thực tế từ **Open-Meteo API**.
- Quản lý danh sách tuyến đường đang theo dõi trong `data/roads.csv`.
- Xử lý, làm sạch, tính toán và tổng hợp dữ liệu bằng **PySpark**.
- Trực quan hóa kết quả trên **Streamlit Dashboard**: KPI, bản đồ, biểu đồ, bảng dữ liệu và bộ lọc.

**Định hướng kỹ thuật của phiên bản demo:**

- Lưu trữ bằng **CSV** theo mô hình snapshot có phiên bản.
- Xử lý **batch** bằng PySpark; không dùng streaming.
- Ghép dữ liệu thời tiết gần nhất trong khoảng **60 phút**.
- **Không có mô hình Học máy (ML)** và không dự báo tương lai.
- Dashboard chỉ đọc dữ liệu đã xử lý; không gọi API và không chạy Spark khi hiển thị.

---

## 2. Kiến trúc hệ thống

### 2.1. Luồng dữ liệu

```text
┌──────────────────────────────┐
│ TomTom Traffic Flow API      │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│ Open-Meteo API               │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Python Collector             │
│ (historical_collector.py)    │
└──────────────┬───────────────┘
               │ append CSV
               ▼
┌──────────────────────────────────────────────┐
│ Raw CSV                                      │
│ data/traffic_data.csv                        │
│ data/weather_data.csv                        │
└──────────────┬───────────────────────────────┘
               │ read-only
               ▼
┌──────────────────────────────────────────────┐
│ PySpark Batch ETL                            │
│ (spark_analysis.py, local[*])                │
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ Versioned Processed CSV Snapshots            │
│ data/processed/snapshots/<version>/          │
│   ├─ combined_data.csv                       │
│   ├─ road_summary.csv                        │
│   ├─ hourly_summary.csv                      │
│   └─ weather_summary.csv                     │
│ data/processed/current_snapshot.json         │
│   (pointer tới snapshot đang hoạt động)      │
└──────────────┬───────────────────────────────┘
               │ read-only
               ▼
┌──────────────────────────────────────────────┐
│ Streamlit Dashboard                          │
│ Plotly (biểu đồ) · Folium (bản đồ)           │
└──────────────────────────────────────────────┘
```

### 2.2. Vai trò các lớp

| Lớp | Thành phần chính | Trách nhiệm |
|---|---|---|
| Thu thập | Python Collector | Gọi TomTom và Open-Meteo theo chu kỳ, ghi dữ liệu thô vào CSV. |
| Xử lý | PySpark Local Mode | Làm sạch, chuẩn hóa kiểu, tính mức ùn tắc, ghép thời tiết, tổng hợp phân tích. |
| Lưu trữ | CSV + versioned snapshots | Giữ dữ liệu thô; xuất snapshot CSV đã xử lý; công bố pointer nguyên tử. |
| Trình bày | Streamlit + Plotly + Folium | Hiển thị KPI, bản đồ, biểu đồ, bảng và bộ lọc từ snapshot CSV. |

### 2.3. Nguyên tắc thiết kế

- **Lưu trữ CSV:** Dữ liệu thô và snapshot phục vụ dashboard đều là CSV. Dashboard chỉ đọc CSV; không dùng Parquet trong luồng hiển thị.
- **Xử lý batch:** PySpark chạy ở chế độ `local[*]`, xử lý theo từng chu kỳ thu thập; không dùng Spark Streaming.
- **Ghép thời tiết 60 phút:** Mỗi quan sát giao thông được ghép với bản ghi thời tiết gần nhất của cùng tuyến trong khoảng 60 phút. Không nội suy và không tạo dữ liệu thời tiết giả; quan sát không khớp được giữ nguyên với trạng thái thiếu thời tiết.
- **Không có ML:** Hệ thống không huấn luyện mô hình, không dự báo ùn tắc; các kết quả chỉ mang tính mô tả dữ liệu đã thu thập.
- **Dashboard read-only:** Khi render, dashboard không gọi API và không chạy Spark. Dữ liệu chỉ thay đổi khi collector hoặc thao tác cập nhật được kích hoạt.

### 2.4. Công thức và phân loại ùn tắc

Mức ùn tắc được tính từ dữ liệu TomTom:

```text
congestion_percent = (1 - current_speed / free_flow_speed) × 100
```

| Mức ùn tắc | Nhãn hiển thị |
|---|---|
| `< 20%` | Thông thoáng |
| `20% – 40%` | Đông |
| `40% – 60%` | Ùn tắc |
| `> 60%` | Ùn tắc nghiêm trọng |

Các phân tích chính gồm: tốc độ trung bình theo tuyến, mức ùn tắc trung bình theo tuyến, so sánh giữa các tuyến, phân tích theo giờ và so sánh khi có mưa với không mưa.

---

## 3. Cấu trúc thư mục cốt lõi

```text
hanoi-traffic-analytics/
├─ app.py                         # Streamlit Dashboard
├─ historical_collector.py        # Bộ lập lịch thu thập dữ liệu
├─ spark_analysis.py              # PySpark ETL + phân tích
├─ data/
│  ├─ roads.csv                   # Danh sách tuyến đang theo dõi
│  ├─ traffic_data.csv            # Raw traffic CSV (TomTom)
│  ├─ weather_data.csv            # Raw weather CSV (Open-Meteo)
│  ├─ processed/
│  │  ├─ current_snapshot.json    # Pointer tới snapshot đang hoạt động
│  │  └─ snapshots/
│  │     └─ <version>/
│  │        ├─ combined_data.csv
│  │        ├─ road_summary.csv
│  │        ├─ hourly_summary.csv
│  │        └─ weather_summary.csv
│  └─ demo_backup/                # Vị trí lưu bản backup CSV cho demo offline
└─ ...
```

**Quy ước về lớp dữ liệu thô:** `data/raw/` là tên gọi logic cho lớp dữ liệu thô. Trong cấu trúc hiện tại, hai file raw nằm trực tiếp dưới `data/` là `traffic_data.csv` và `weather_data.csv`.

**Về `data/demo_backup/`:** Đây là vị trí dành cho bản backup CSV phục vụ demo offline (nếu đã tạo). Nguồn đọc chính của dashboard là snapshot đang hoạt động trong `data/processed/` (thông qua `current_snapshot.json`).

**Các thành phần hỗ trợ:** `collector.py` (một chu kỳ thu thập), `data_update.py` (điều phối pipeline và chế độ `--process-only`), `config.py` (đường dẫn và cấu hình thu thập), `utils/` (API client, quản lý tuyến), `assets/dashboard.css` (giao diện), `tests/` (kiểm thử), cùng `Dockerfile`, `docker-compose.yml` và `run-docker.ps1` cho đóng gói Docker.

---

## 4. Yêu cầu hệ thống

- **Docker Desktop** (bắt buộc).
  - Trên Windows, bật **WSL2 backend** trong Docker Desktop.
  - Docker Engine phải ở trạng thái **running** trước khi khởi chạy.
- **Không cần cài Python, Java/JDK, PySpark hay Streamlit trên máy host.** Toàn bộ runtime đã được đóng gói trong Docker image (Python 3.11 + OpenJDK 17 + các thư viện của dự án).
- **Chế độ Full System:** cần `TOMTOM_API_KEY` trong file `.env` và kết nối Internet để gọi API.
- **Chế độ Offline Demo:** không gọi API; chỉ đọc bộ snapshot CSV có sẵn. Sau khi Docker image đã được build, chế độ này không phụ thuộc vào API.

### Cấu hình `.env` (chỉ cần cho Full System)

```powershell
Copy-Item .env.example .env
```

Mở `.env` và điền khóa TomTom:

```text
TOMTOM_API_KEY=<khóa TomTom của bạn>
```

Không commit file `.env`. Open-Meteo không yêu cầu API key.

---

## 5. Hướng dẫn khởi chạy bằng Docker

### 5.1. Khởi chạy

Mở **PowerShell** tại thư mục gốc của dự án và chạy:

```powershell
cd D:\hanoi-traffic-analytics
.\run-docker.ps1
```

Nếu PowerShell chặn script, cho phép chạy trong phiên hiện tại:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\run-docker.ps1
```

Script sẽ:

1. Kiểm tra Docker Desktop và Docker Compose v2.
2. Hỏi lựa chọn chế độ chạy (1 hoặc 2).
3. Dọn container cũ bằng `docker compose down`.
4. Build image và khởi chạy các container tương ứng.
5. Chờ dashboard sẵn sàng (tối đa 45 giây) rồi mở trình duyệt tại `http://localhost:8501/`.

### 5.2. Hai chế độ chạy

| Lựa chọn | Chế độ | Thành phần | Gọi API | Khuyến nghị |
|---|---|---|---|---|
| **1** | **Full System** | Dashboard + Collector | Có | Khi cần thu thập dữ liệu mới |
| **2** | **Offline Demo** | Chỉ Dashboard | Không | Khi báo cáo, thuyết trình |

#### Chế độ 1 — Full System

Chế độ này khởi chạy cả **Collector nền** và **Dashboard**:

- Collector tự động gọi **TomTom Traffic API** và **Open-Meteo API** theo chu kỳ **5 phút/lần**, trong khung thời gian thu thập 06:00–22:00 (Asia/Ho_Chi_Minh).
- Script khởi chạy collector với `--max-cycles 100`, tức mỗi lần chạy thực hiện tối đa 100 chu kỳ thu thập.
- Dữ liệu mới được ghi vào CSV thô, sau đó PySpark xử lý và công bố snapshot mới cho dashboard.

> ⚠️ **Cảnh báo tiêu tốn quota API:** Mỗi chu kỳ collector gọi TomTom cho từng tuyến đang theo dõi và gọi Open-Meteo cho dữ liệu thời tiết. Chỉ chạy Full System khi thực sự cần thu thập dữ liệu; dừng hệ thống bằng `docker compose down` sau khi hoàn tất để tránh tiêu tốn quota ngoài ý muốn.

#### Chế độ 2 — Offline Demo (khuyên dùng khi báo cáo)

Chế độ này **chỉ khởi chạy Streamlit Dashboard**:

- Không khởi chạy collector; không gọi TomTom hay Open-Meteo.
- Dashboard đọc dữ liệu từ bộ **snapshot CSV có sẵn** trong `data/processed/snapshots/` thông qua `current_snapshot.json`.
- Bộ snapshot demo hiện có **hơn 3.600 quan sát**, đủ để trình diễn KPI, bản đồ, biểu đồ, bảng và bộ lọc.
- Đây là chế độ an toàn tuyệt đối khi thuyết trình vì không phát sinh lưu lượng API.

> ✅ **Lưu ý khi báo cáo:** Không bấm nút **“Cập nhật dữ liệu”** trên dashboard trong chế độ Offline Demo. Nút này được thiết kế cho luồng cập nhật dữ liệu và có thể kích hoạt collector/Spark nếu runtime có cấu hình API.

### 5.3. Địa chỉ truy cập

Sau khi script báo thành công:

```text
http://localhost:8501/
```

---

## 6. Dừng, giám sát và xử lý sự cố

### 6.1. Dừng hệ thống

```powershell
docker compose down
```

Lệnh này dừng và xóa container nhưng **không xóa dữ liệu** trong thư mục `data/` (dữ liệu được mount từ máy host).

### 6.2. Xem log và trạng thái

```powershell
docker compose ps
docker compose logs -f dashboard
docker compose logs -f collector
```

### 6.3. Một số lỗi thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| Không tìm thấy lệnh `docker` | Cài Docker Desktop và mở lại PowerShell. |
| Docker Engine chưa sẵn sàng | Mở Docker Desktop, chờ trạng thái **Engine running**, rồi chạy lại script. |
| PowerShell chặn `run-docker.ps1` | Chạy `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` trong phiên hiện tại. |
| Dashboard chưa sẵn sàng sau 45 giây | Kiểm tra `docker compose logs dashboard` và `docker compose ps`. |
| Full System không thu thập được dữ liệu | Kiểm tra `TOMTOM_API_KEY` trong `.env` và kết nối Internet. |
| Cổng `8501` đang bị chiếm | Chạy `docker compose down` để dừng container cũ, sau đó chạy lại script. |

---

## 7. Phạm vi và giới hạn

- **Batch, không streaming:** PySpark xử lý dữ liệu theo từng chu kỳ thu thập; hệ thống không dùng streaming, message queue hay xử lý sự kiện thời gian thực.
- **CSV là định dạng lưu trữ của luồng demo:** Dữ liệu thô và snapshot dashboard là CSV. Dashboard không đọc Parquet.
- **Không có ML:** Không huấn luyện mô hình, không dự báo hay phân loại tương lai.
- **Ghép thời tiết có giới hạn:** Chỉ ghép bản ghi thời tiết gần nhất trong khoảng 60 phút; quan sát không khớp không được gán giá trị thời tiết giả.
- **Dữ liệu là ảnh chụp tại thời điểm thu thập:** Dashboard hiển thị snapshot đã xử lý, không phải trạng thái giao thông trực tiếp theo thời gian thực.
- **Bản đồ hiển thị tọa độ đại diện của tuyến:** Marker được đặt tại tọa độ đại diện trong `roads.csv`, không vẽ hình học đầy đủ của con đường.

---

## 8. Trạng thái dự án và tài liệu tham chiếu

### 8.1. Trạng thái

- **Phase 10 — Testing & Demo (Sẵn sàng Demo).**
- Dashboard đọc snapshot CSV và hoạt động độc lập với API khi render.
- Bộ snapshot demo có hơn 3.600 quan sát, phục vụ trình diễn offline.
- Hệ thống không có mô hình Học máy (ML).

### 8.2. Tài liệu tham chiếu

- `PROJECT_CONTEXT.md` — kiến trúc, phạm vi, roadmap và trạng thái kỹ thuật của dự án.
- `AGENTS.md` — quy tắc phát triển và làm việc với dự án.
- `tests/` — các bộ kiểm thử của dashboard, quản lý tuyến và tìm kiếm tuyến đường.

---

*Tài liệu được cập nhật cho phiên bản Phase 10 — Sẵn sàng Demo của Hanoi Traffic Analytics.*
