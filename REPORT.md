# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nông Ngọc Dương &nbsp;·&nbsp; **MSSV:** 2A202601296 &nbsp;·&nbsp; **Lớp:** AICB-P2T2 &nbsp;·&nbsp; **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lần chạy liên tiếp</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 40.8s
  run 2/3 … 39.2s
  run 3/3 … 38.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần `make pipeline` (hoặc mỗi lần Airflow Clear Task), số hàng `gold_training_set` tiếp tục tăng — từ 12.480 lên 38.750 sau ba lần chạy — mặc dù dữ liệu nguồn không thay đổi. Không có cảnh báo lỗi nào xuất hiện. |
| **Nguyên nhân** | Model được khai báo `materialized = 'incremental'` nhưng **thiếu `unique_key`**. Khi không có `unique_key`, dbt không biết hàng nào là "cùng một thực thể" và mặc định dùng chiến lược `append` — mỗi lần chạy chèn thêm toàn bộ partition ngày đó vào bảng, thay vì thay thế. Dữ liệu nguồn là CDC có bản ghi `op='u'` (cập nhật), nên cùng một `ticket_id` có thể xuất hiện nhiều lần trong cùng một lượt chạy, rồi lại được append thêm lần nữa ở lượt sau. Bên cạnh đó, DAG Airflow cấu hình `catchup=True` (mặc định) và thiếu `max_active_runs=1` — khi người trực bấm **Clear Task** sau sự cố mạng, Airflow khởi lại task đó đồng thời với các task định kỳ vẫn đang chạy, tạo ra nhiều lượt `INSERT` song song vào cùng một bảng. Kết quả: **mọi cơ chế retry ở tầng Airflow đều biến thành cơ chế nhân bản dữ liệu**, vì bản thân phép ghi không idempotent. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: bổ sung `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng (không ổn định) · Sau: 12.480 hàng · Checksum 3 lượt: `8dd7c98653` (đồng nhất) · `gold_training_set: 1 hàng / 1 ticket ✓` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu khoảng 5% so với đối chiếu thủ công. Kỳ lạ là chỉ thiếu ở những ngày đã chạy xong từ lâu; ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** *(đo từ `EPOCH(_ingested_at - event_time) / 86400.0` trên toàn bộ 129.462 bản ghi `bronze_events`)* |
| **Lookback đã chọn** | **3 ngày** — lấy ceiling của P99 (2,73 ngày → 3 ngày) để đảm bảo bắt được ≥ 99% bản ghi về muộn, đồng thời không quét lại quá nhiều dữ liệu cũ. |
| **Nguyên nhân** | Điều kiện lọc incremental ban đầu là `WHERE event_date >= max(event_date)` — tức là chỉ tính lại từ ngày mới nhất đang có trong bảng đích. Một bản ghi có `event_date = 08-12` nhưng `_ingested_at = 08-15` (tới kho muộn 3 ngày) sẽ không thoả điều kiện lọc khi chạy ngày 08-15 (`max(event_date)` đang là 08-14 hoặc 08-15). Bản ghi đó không được đưa vào `gold_feature_daily` ở bất kỳ lượt chạy nào — **dữ liệu về muộn bị mất vĩnh viễn**. Khoảng 5,05% bản ghi (6.539 / 129.462) tới kho muộn hơn 1 ngày, đủ giải thích mức thiếu quan sát được. |
| **Cách khắc phục** | Mở rộng window lookback: `WHERE event_date >= (SELECT max(event_date) FROM {{ this }}) - interval 3 day`. Bổ sung `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để bản ghi được tính lại thay thế bản cũ, không cộng dồn. |
| **Bằng chứng** | Trước: 8.645 hàng (ổn định nhưng sai) · Sau: 9.100 hàng · Checksum 3 lượt: `3db448685c` (đồng nhất) |

**Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**

> P99 (2,73 ngày) là điểm cân bằng giữa **coverage** và **chi phí vận hành**. Nếu dùng `max` (2,94 ngày) thì lookback phải là 3 ngày — trùng với ceiling của P99, chi phí tương đương trong trường hợp này. Tuy nhiên về nguyên tắc: `max` bị kéo bởi một outlier cực đoan duy nhất; nếu có một bản ghi về muộn 30 ngày thì lookback sẽ phải là 30 ngày, quét lại 30 × lượng dữ liệu so với chạy P99 = 3 ngày. Mỗi ngày lookback thêm = thêm một ngày dữ liệu phải đọc và ghi đè ở **mọi lượt chạy trong tương lai** — chi phí này tích lũy vô thời hạn. P99 chấp nhận bỏ qua 1% outlier để giữ chi phí vận hành bền vững.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Team backend đổi cột `priority` từ số nguyên sang chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`) từ ngày 08-10, có thông báo trên Slack. Pipeline không dừng. Nhưng model phân loại từ hôm đó dự đoán kém hẳn. |
| **Nguyên nhân** | Không có **data contract** được enforce ở tầng Silver. `silver_tickets` dùng `try_cast(priority_raw as integer)` — biểu thức này âm thầm biến nhãn chuỗi hợp lệ (`'urgent'` → NULL) thay vì báo lỗi. NULL bị loại khỏi bảng, model classifier nhận thiếu nhãn và bị lệch phân bố. Pipeline "không dừng" chính là triệu chứng của việc lỗi bị nuốt im — không có cảnh báo nào nổi lên. Từ 08-10 đến ngày quan sát, ~50% bản ghi `priority` đến dưới dạng chuỗi và bị mất. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** — `'1'`, `'2'`, `'3'`, `'4'` (13.846 bản ghi): đúng contract cũ → cast thẳng sang integer, giữ nguyên. **Nhóm 2** — `'urgent'`, `'high'`, `'medium'`, `'low'` (7.142 bản ghi): backend đổi format từ 08-10, ý nghĩa không đổi → quy đổi sang số (urgent=1, high=2, medium=3, low=4). **Nhóm 3** — `'P1'`, `'P2'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, NULL (312 bản ghi): dữ liệu hỏng thật sự → trả về NULL → đẩy sang `quarantine_tickets`. |
| **Cách khắc phục** | (1) Macro `normalize_priority` trong `dbt/macros/normalize_priority.sql` xử lý đúng ba nhóm. (2) `silver_tickets.sql`: lọc `WHERE normalize_priority(priority_raw) IS NOT NULL` *trước* khi xếp hạng `row_number()` — loại bản ghi hỏng, không loại ticket. (3) `quarantine_tickets.sql`: nhặt bản ghi có `normalize_priority IS NULL`. (4) `schema.yml`: bật `contract: enforced: true`, thêm test `not_null` và `accepted_values: [1,2,3,4]` cho cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass · `silver_tickets.priority ∈ 1..4, không NULL ✓ sạch` · `silver_tickets` giữ đủ 12.480 ticket |

**Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao không để pipeline dừng?**

> **Không chặn ở Bronze** vì Bronze là lớp "raw as-is" — nó phản ánh đúng những gì nguồn gửi, kể cả dữ liệu hỏng. Loại bỏ ở Bronze đồng nghĩa với mất dấu vết gốc, không replay được nếu logic nghiệp vụ thay đổi. **Chặn ở Silver** là đúng vị trí: đây là nơi áp dụng business rule (priority phải là 1..4), và bản ghi hỏng được tách ra `quarantine_tickets` thay vì xoá thẳng.
>
> **Không để pipeline dừng** vì 312 bản ghi hỏng không có quyền chặn 129.462 event và 31.200 chunk hoàn toàn bình thường phục vụ người dùng cuối. Dừng pipeline = toàn bộ hệ thống ngừng hoạt động vì lỗi của một phần nhỏ dữ liệu. Quarantine là pattern đúng: tách lỗi ra một hàng đợi có cấu trúc để người trực xử lý sau, pipeline chạy tiếp với phần dữ liệu tốt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra `config()` của mọi model incremental: `unique_key` và `incremental_strategy` có được khai báo rõ ràng không? Chạy pipeline hai lần liên tiếp rồi so sánh số hàng — nếu tăng thì ghi là không idempotent. |
| 2 | Đo phân bố độ trễ giữa `event_time` và `_ingested_at` ngay từ đầu. Nếu P99 > 0 ngày, window lookback mặc định (`max(date)`) là sai. |
| 3 | Tìm tất cả `try_cast` hoặc implicit cast trong pipeline — đây là nơi lỗi schema bị nuốt im lặng nhất. Bật `contract: enforced` và viết test miền giá trị cho mọi cột nghiệp vụ quan trọng. |
