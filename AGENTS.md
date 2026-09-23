# AI Development Rules — Hanoi Traffic Analytics

Tài liệu này quy định cách AI coding agent làm việc với project.
Mọi rule là bắt buộc. Vi phạm rule phải DỪNG và báo người dùng.

---

## Rule 1 — Luôn đọc PROJECT_CONTEXT.md trước khi code

Trước bất kỳ thay đổi nào:

1. Đọc `PROJECT_CONTEXT.md`.
2. Xác định `CURRENT_PHASE` và `LAST_COMPLETED_PHASE`.
3. Kiểm tra repository hiện tại (file/thư mục, dependency).
4. Chỉ sau đó mới được sửa code.

Không được bắt đầu coding nếu chưa hiểu phase hiện tại.

---

## Rule 2 — Kiểm tra repository trước khi sửa

- Đọc code hiện tại liên quan trước khi sửa.
- Hiểu dependency và ảnh hưởng.
- Không giả định file không tồn tại; phải kiểm tra thực tế.

---

## Rule 3 — Chỉ làm CURRENT_PHASE

Chỉ triển khai chức năng thuộc `CURRENT_PHASE`.

Không:

- làm trước phase sau;
- tạo code "để dùng sau";
- tự mở rộng scope;
- triển khai bonus feature.

Ví dụ: nếu `CURRENT_PHASE = 1` thì chỉ làm Project Foundation.
Không được tự làm TomTom, Open-Meteo, Dashboard, ML, Database.

---

## Rule 4 — Không tự làm phase tiếp theo

Khi hoàn thành tiêu chí kỹ thuật của phase hiện tại:

1. Chạy verification.
2. Báo cáo kết quả.
3. DỪNG.

Chờ người dùng xác nhận mới được sang phase kế tiếp.

---

## Rule 5 — Không tự thay architecture/stack

Technology stack và architecture trong `PROJECT_CONTEXT.md` là cố định.

Không tự ý:

- đổi CSV sang database;
- đổi Streamlit sang React;
- thêm Kafka;
- thêm Hadoop;
- thêm Docker;
- thêm framework/infra khác.

Nếu cho rằng cần thay đổi: DỪNG và giải thích đề xuất.
Chỉ thực hiện sau khi người dùng chấp thuận.

---

## Rule 6 — Không over-engineering

Đây là project môn học.

Ưu tiên:

- đơn giản;
- dễ hiểu;
- dễ chạy;
- dễ demo;
- dễ giải thích với giảng viên.

Không tạo abstraction, architecture hoặc infrastructure không cần thiết.

---

## Rule 7 — PySpark là processing engine chính

Các bước:

- cleaning;
- transformation;
- aggregation;
- analytics;

phải ưu tiên PySpark.

---

## Rule 8 — Pandas không được thay thế Spark cho ETL/analytics

Không được thay toàn bộ processing bằng Pandas.

Pandas chỉ được sử dụng khi hợp lý ở presentation layer
(đọc file nhỏ để vẽ Streamlit/Plotly) hoặc thao tác nhỏ
không thay thế vai trò của Spark.

---

## Rule 9 — Không hard-code secret

Không hard-code:

- API key;
- token;
- credential.

Các giá trị bí mật phải nằm trong `.env`.

`.env` không được commit Git (phải nằm trong `.gitignore`).

Có thể cung cấp `.env.example` nhưng không chứa secret thật.

---

## Rule 10 — Không phá chức năng đang chạy

Trước khi sửa:

- đọc code hiện tại;
- hiểu dependency;
- giữ backward compatibility nếu có thể.

Ưu tiên thay đổi nhỏ nhất để hoàn thành yêu cầu.

Không rewrite toàn project nếu không cần thiết.

---

## Rule 11 — Luôn verification sau khi sửa

Sau mỗi thay đổi:

1. Chạy kiểm tra phù hợp (syntax/import/unit/run thử).
2. Kiểm tra lỗi syntax/import.
3. Kiểm tra chức năng vừa triển khai.
4. Không tuyên bố thành công nếu verification thất bại.

Nếu test thất bại:

- tìm nguyên nhân;
- sửa;
- chạy lại.

---

## Rule 12 — Báo rõ lỗi nếu có

Nếu gặp lỗi không thể giải quyết:

Không giả định rằng chức năng hoạt động.

Phải báo:

- lỗi gì;
- xảy ra ở đâu;
- nguyên nhân có khả năng;
- phần nào đã hoàn thành;
- phần nào chưa hoàn thành.

Không che giấu lỗi.

---

## Rule 13 — Báo cáo file tạo/sửa và command kiểm thử

Cuối mỗi task phải báo ngắn gọn:

### Đã làm

- ...

### File đã tạo

- ...

### File đã sửa

- ...

### Verification

- command đã chạy
- kết quả

### Trạng thái roadmap

- Phase hiện tại
- hoàn thành / chưa hoàn thành

### Bước tiếp theo

- chỉ nêu phase kế tiếp;
- không tự triển khai.

---

## Rule 14 — Không tự chuyển CURRENT_PHASE

AI có thể báo: "Phase X đã đáp ứng các tiêu chí kỹ thuật."

Nhưng không được tự thay `CURRENT_PHASE = X + 1`.

Chỉ người dùng quyết định chuyển sang phase tiếp theo.

---

## Rule 15 — Việc chuyển phase cần người dùng xác nhận

- Việc chuyển phase cần người dùng xác nhận rõ ràng.
- Sau khi được xác nhận, tài liệu `PROJECT_CONTEXT.md` phải được cập nhật (`CURRENT_PHASE`, `LAST_COMPLETED_PHASE`, trạng thái phase) để tránh lệch ở các session sau.
- Nếu prompt hiện tại và implementation cũ mâu thuẫn với `PROJECT_CONTEXT.md`, phải báo mâu thuẫn cho người dùng, không tự quyết. Yêu cầu mới rõ ràng của người dùng có quyền thay đổi context, nhưng sau đó context phải được cập nhật.
