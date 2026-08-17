# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Phó Hiếu Anh  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

Ba nhiệm vụ chính đạt đủ, kèm cả hai bài mở rộng trong `EXTRA.md`.

---

## 0 · Kết quả `make verify`

<details>
<summary><code>make verify</code> — output ba lượt chạy (bấm để mở)</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 576.8s
  run 2/3 … 14.0s
  run 3/3 … 13.4s

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
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

<details>
<summary><code>make explain</code> — chấm bài mở rộng A</summary>

```
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000           9,324     ≤ 500,000   ✓
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000              14        ít hơn   ✓
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —             4.4   (tham khảo)

  => giảm 536.3× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)
```

</details>

<details>
<summary><code>make crash-test</code> — chấm bài mở rộng B</summary>

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố
  [consumer] đã ghi 20,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  B. chạy và bị giết ở lô 7
  [consumer] 💥 tiến trình bị giết ở lô 7
     -> tiến trình thoát với mã 137
     -> offset đã commit: 3,000

  C. khởi động lại, chạy nốt
  [consumer] đã ghi 17,000 message
     -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** · `dbt test` 11/11 pass · bài mở rộng A 536,3× · bài mở rộng B ĐẠT.

Đã chạy thêm `python tools/verify.py --runs 5`: checksum lượt 4 và lượt 5 vẫn
trùng khít ba lượt đầu (`gold_training_set` = `8dd7c98653` ở cả 5 lượt).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Phiếu #1041: sau khi bấm Clear Task cho chạy lại, `gold_training_set` phình từ 12.480 lên 38.750 hàng, mỗi lượt chạy lại lại tăng thêm, và không phát sinh lỗi nào. |
| **Nguyên nhân** | Xem phân tích cơ chế bên dưới. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 13.790 hàng sau 1 lượt / 38.750 sau 3 lượt · sau: **12.480** hàng ở cả 5 lượt · checksum 3 lượt: `8dd7c98653` · `8dd7c98653` · `8dd7c98653` |

**Nguyên nhân (cơ chế).** Model khai `materialized='incremental'` nhưng không khai
`unique_key`. Thiếu `unique_key`, dbt không có căn cứ nào để biết "hàng nào là cùng
một hàng", nên câu lệnh nó sinh ra từ lượt chạy thứ hai trở đi là `INSERT` thuần —
đọc được nguyên văn trong `dbt/target/run/lab17/models/gold/gold_training_set.sql`:

```sql
insert into "warehouse"."main"."gold_training_set" (...)
( select ... from "gold_training_set__dbt_tmp20260817160126929284" )
```

Đó là phép ghi **cộng dồn**, không phải phép ghi **thay thế**. Hệ quả tổng quát:
nội dung bảng phụ thuộc vào *số lần pipeline đã chạy* chứ không chỉ phụ thuộc dữ
liệu nguồn. Khi phép ghi không idempotent thì **mọi** cơ chế an toàn ở tầng trên —
retry của scheduler, Clear Task của Airflow, backfill một ngày — đều đổi vai thành
cơ chế nhân bản. Người trực làm đúng quy trình vẫn tạo ra lỗi dữ liệu.

Đặc thù của bảng này làm lỗi phát tác **ngay trong một lượt chạy duy nhất**, không
cần ai chạy lại. Grain là *entity* (1 hàng / 1 ticket) nhưng mệnh đề lọc là
*partition ngày* theo `_ingested_at`. Nguồn CDC có 1.310 bản ghi `op='u'`; một
ticket tạo ngày D1 rồi sửa ngày D2 sẽ có bản ghi mới nhất mang `_ingested_at` = D2,
nên nó lọt qua mệnh đề `WHERE` **hai lần** trong cùng một lượt phát lại 14 ngày —
một lần ở partition D1, một lần ở partition D2. Đo được đúng như vậy:

| | số hàng | chênh |
|---|---|---|
| `silver_tickets` (nguồn) | 12.480 hàng / 12.480 ticket | grain đúng |
| `gold_training_set` sau 1 lượt | 13.790 | +1.310 = đúng số bản ghi `op='u'` |
| `gold_training_set` sau 3 lượt | 38.750 | +24.960, tức **mỗi lượt chạy lại cộng đúng 12.480** |

(Hai con số 13.790 và 38.750 là đo trực tiếp; 12.480/lượt là hiệu số suy ra từ chúng.)

Nguồn giữ đúng 1 hàng / 1 ticket mà đích thì không, nên lỗi nằm ở **cách model được
materialize**, không nằm ở dữ liệu.

Vì sao chọn `merge` theo khoá chứ không `delete+insert` theo partition ngày: partition
của một ticket **thay đổi** khi ticket được update. Xoá partition ngày D2 rồi ghi lại
không chạm tới hàng cũ đang nằm ở partition D1 — hàng lặp vẫn còn. Chỉ phép upsert
theo natural key của grain (`ticket_id`) mới hội tụ.

Hai tham số DAG: `catchup=True` khiến Airflow tự xếp hàng 14 lần chạy bù cho quá khứ
mỗi khi DAG được bật lại, và `max_active_runs` bỏ trống cho phép nhiều run ghi đồng
thời vào cùng một bảng. Cần nói rõ: hai tham số này chỉ **giảm tần suất kích hoạt**
lỗi, chúng không phải root cause — sửa DAG mà không sửa model thì `make verify` vẫn
đỏ, vì bản thân một lượt chạy đã tự sinh 1.310 hàng lặp.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1043: `gold_feature_daily` có 8.645 / 9.100 hàng (thiếu 5,0%), và chỉ thiếu ở những ngày đã chạy xong từ lâu; ngày mới thì đủ. Cột `ỔN ĐỊNH` vẫn ✓. |
| **P99 độ trễ đo được** | **2,7258 ngày** (≈ 65,4 giờ) |
| **Lookback đã chọn** | **3 ngày** — vì `ceil(P99) = ceil(2,73) = 3`, và 3 ngày cũng phủ luôn `max = 2,9447` ngày; khớp với đo thực tế là bản ghi muộn nhất tới kho vào `event_date + 3 ngày`. |
| **Nguyên nhân** | Xem phân tích cơ chế bên dưới. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi mệnh đề lọc thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, đồng thời thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'`. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: **9.100** hàng, checksum `3db448685c` giống nhau cả 3 lượt |

**Phân bố độ trễ đo trên `bronze_events`** (`_ingested_at − event_time`):

| P50 | P95 | **P99** | max | tỷ lệ tới kho muộn > 1 ngày |
|---|---|---|---|---|
| 0,1281 ngày | 1,8137 ngày | **2,7258 ngày** | 2,9447 ngày | 5,051 % |

Phân bố có **hai cụm tách biệt**, không phải một đuôi trơn: cụm đúng hạn 0–6 giờ
(122.923 bản ghi) và cụm về muộn 43–71 giờ (7.760 bản ghi). Giữa 6 giờ và 43 giờ
không có bản ghi nào. Đó là dấu hiệu của hai đường nạp dữ liệu khác nhau, không phải
của nhiễu ngẫu nhiên.

**Nguyên nhân (cơ chế).** Mệnh đề incremental dùng chính `event_date` làm watermark:

```sql
where event_date > (select max(event_date) from {{ this }})
```

Watermark này đo theo thời điểm sự kiện **xảy ra**, trong khi pipeline lại nạp dữ
liệu theo thời điểm nó **tới kho**. Hai trục thời gian đó không trùng nhau, và độ
lệch giữa chúng chính là số đo ở bảng trên. Một event xảy ra 08-12 nhưng tới kho
08-15: tại lượt chạy 08-15, watermark trong bảng đích đã là 08-14, nên `event_date`
= 08-12 của nó **không** lớn hơn watermark và bị `WHERE` loại. Điểm chí tử là
watermark chỉ **tăng đơn điệu**: hôm sau nó còn lớn hơn nữa, nên bản ghi đó không
bao giờ được lượt chạy nào xét lại — nó bị loại **vĩnh viễn**, im lặng, không log,
không lỗi.

Đây cũng là lý do bảng "ổn định mà vẫn sai": chạy lại bao nhiêu lần cũng ra đúng cùng
một kết quả thiếu. **Tính ổn định và tính đúng là hai đại lượng độc lập** — một phép
biến đổi tất định vẫn tất định khi nó bỏ sót dữ liệu.

455 cặp `(ngày, khách)` mất hoàn toàn là những cặp mà **toàn bộ** event của cặp đó
đều tới muộn (nên hàng chưa bao giờ được tạo). Các cặp có cả event đúng hạn lẫn event
muộn thì vẫn tồn tại nhưng mang **giá trị tổng hợp thiếu** — `n_events`, `p95_latency_ms`
bị tính hụt. Số hàng không phản ánh được phần sai này; đó là lý do `make verify` so
checksum trên chính các cột tổng hợp chứ không chỉ so `count(*)`.

Đo xác nhận: 455 cặp thiếu nằm trọn trong 08-03…08-13, không có cặp nào ở ba ngày mới
nhất 08-14…08-16; độ trễ nhỏ nhất của chúng là 43 giờ, và thời điểm tới kho muộn nhất
là `event_date + 3 ngày`.

Đổi `>` thành `>=` **không đủ**: toán tử đó chỉ nới window thêm đúng một ngày, trong
khi cụm dữ liệu muộn nằm ở 43–71 giờ, tức 2–3 ngày.

**Ràng buộc đi kèm.** Window rộng hơn nghĩa là cùng một `(event_date, customer_id)`
được tính lại ở nhiều lượt chạy. Nếu chỉ nới window mà vẫn `insert`, bảng này lặp lại
đúng lỗi của nhiệm vụ 1 trên một grain khác — nên grain hai cột phải được khai
`unique_key = ['event_date', 'customer_id']` với `merge`, để lần tính sau **thay thế**
lần tính trước. Nhờ vậy các giá trị tổng hợp bị tính hụt trước đó cũng được ghi đè
bằng bản tính đầy đủ.

> **Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**
>
> Chi phí của lookback không trả một lần mà trả ở **mọi lượt chạy về sau**: window 3
> ngày nghĩa là mỗi đêm tính lại ~4 ngày dữ liệu thay vì 1 ngày, vĩnh viễn. Vì vậy mỗi
> ngày lùi thêm là một khoản chi thường xuyên, không phải một khoản đầu tư một lần.
>
> `max` là một thống kê không ổn định: đúng một bản ghi bệnh lý — một consumer treo,
> một lần replay thủ công — đủ để đẩy nó lên vô hạn, và window sẽ bị neo theo trường
> hợp tệ nhất từng xảy ra chứ theo hành vi thường ngày của hệ thống. P99 là ngưỡng có
> ý nghĩa vận hành: nó nói "99% dữ liệu về trong bao lâu", tức phần đuôi còn lại là
> việc của **cảnh báo**, không phải việc của window. Cách làm đúng là lookback theo
> percentile + một cảnh báo cho bản ghi tới ngoài window, để 1% kia được xử lý có ý
> thức thay vì bị lặng lẽ bỏ hoặc bị trả giá mỗi đêm.
>
> Cần nói cho trung thực: trên chính bộ dữ liệu này `ceil(P99) = ceil(max) = 3`, nên
> hai cách chọn ra cùng một con số. Lập luận trên nói về *căn cứ*, không phải về chênh
> lệch kết quả ở lần này.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Phiếu #1047: team backend đổi kiểu `priority` từ số sang chuỗi ngày 08-10, có thông báo. Pipeline không dừng, `dbt test` vẫn 9/9 pass, nhưng model phân loại từ hôm đó dự đoán kém hẳn. |
| **Nguyên nhân** | Xem phân tích cơ chế bên dưới. |
| **Cách khắc phục** | 4 chỗ: (a) `dbt/macros/normalize_priority.sql` — khối `CASE` xử lý ba nhóm; (b) `dbt/models/silver/silver_tickets.sql` — **lọc trước, xếp hạng sau**; (c) `dbt/models/silver/quarantine_tickets.sql` — `where {{ normalize_priority('priority_raw') }} is null`; (d) `dbt/models/silver/schema.yml` — `contract.enforced: true` + `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng grain: 1 hàng / 1 bản ghi CDC) · `dbt test` **11/11** pass (bản gốc 9) · `silver_tickets.priority` 0 NULL, miền [1, 4] · `silver_tickets` vẫn đủ **12.480** ticket |

**Nguyên nhân (cơ chế).** Silver chuẩn hoá bằng `try_cast(priority_raw as integer)`.
Phép ép kiểu này sai **theo hai hướng ngược nhau cùng lúc**:

1. nó biến mọi **nhãn chữ hợp lệ** (`urgent`/`high`/`medium`/`low`) thành `NULL` —
   6.488 hàng, tức vứt bỏ dữ liệu hoàn toàn tốt chỉ vì nguồn đổi cách biểu diễn;
2. nó lại **nhận** `'0'`, `'5'`, `'-1'` vì chúng đúng là số — 118 hàng — dù contract
   quy định `priority ∈ 1..4`.

Cộng lại: 6.606 / 12.480 hàng sai (**53%**).

Nhưng cơ chế đáng học không phải phép cast, mà là **vì sao không ai biết**. Cột
`priority` lúc đó có `contract.enforced: false`, tức không ai kiểm tra **kiểu**; và
không có test `accepted_values`, tức không ai kiểm tra **miền giá trị**. Không có
mệnh đề nào trong pipeline phát biểu điều kỳ vọng về cột này, nên khi nguồn đổi hợp
đồng, hệ thống không có chỗ nào để mà thất bại. Nó tiếp tục chạy, `dbt test` tiếp tục
báo 9/9 pass, và đưa xuống mô hình phân loại một cột nhãn rỗng hơn nửa. Đây là dạng
lỗi tốn kém nhất trong vận hành dữ liệu: không phải pipeline **đổ**, mà pipeline
**vẫn xanh** trong khi dữ liệu đã sụp. Thứ hỏng trước tiên là phép đo, không phải
dữ liệu.

Cần cả hai lớp, vì chúng ràng buộc hai thứ khác nhau: `contract` ràng buộc **kiểu**,
nhưng một mình nó vẫn cho `priority = 99` đi qua — 99 đúng là integer. Miền giá trị
là việc của **test**.

**Ba nhóm giá trị `priority` và cách xử lý từng nhóm**

| Nhóm | Giá trị | Số hàng | Bản chất | Xử lý |
|---|---|---|---|---|
| 1 | `'1' '2' '3' '4'` | 6.846 | Đúng contract gốc | Giữ nguyên (kèm điều kiện `between 1 and 4`) |
| 2 | `'urgent' 'high' 'medium' 'low'` | 7.142 | **Schema evolution** — nguồn đổi cách biểu diễn từ 08-10, ý nghĩa không đổi | **Quy về số**: urgent→1, high→2, medium→3, low→4 |
| 3 | `'P1' 'P2' 'unknown' '0' '5' '-1' '' NULL` | **312** | Dữ liệu lỗi thật | Macro trả `NULL` → vào `quarantine_tickets` |

Tổng 6.846 + 7.142 + 312 = 14.300 = đúng tổng số bản ghi CDC.

Tiêu chí phân biệt nhóm 2 với nhóm 3: *giá trị này có mang đúng thông tin của contract
cũ, chỉ khác cách biểu diễn hay không?* Có thì map, không thì quarantine. Xử lý nhóm 2
như nhóm 3 sẽ đẩy quarantine lên hàng nghìn hàng và vứt đi một nửa dữ liệu tốt.

Mốc thời gian xác nhận đúng ngày 08-10: trước ngày đó có 0 nhãn chữ, từ ngày đó nhãn
chữ chiếm gần như toàn bộ (961/984 hàng ngày 08-10).

**Bẫy thứ tự — đo được, không phải suy đoán.** Cả **312/312** bản ghi lỗi đều là bản
ghi **mới nhất** của ticket tương ứng (`row_number() = 1`, và cả 312 đều là `op='u'`).
Nếu thêm điều kiện lọc *sau* `row_number()`, 312 ticket đó rụng khỏi Silver
(12.480 → 12.168) và `gold_training_set` hụt theo. Đúng phải là **lọc bản ghi hỏng
trước, xếp hạng sau**: ta loại *bản ghi*, không loại *ticket*. Đã kiểm tra cả 312
ticket đều còn một trạng thái hợp lệ từ lần cập nhật trước đó, nên sau khi lọc, bản
ghi hợp lệ liền trước lên làm `rn = 1` và ticket vẫn còn. Kết quả: 312 ticket bị
quarantine **vẫn có mặt đủ** trong `silver_tickets`.

Macro dùng chung cho cả hai model (`silver_tickets` lấy giá trị, `quarantine_tickets`
tìm bản ghi lỗi) nên hai bảng không thể lệch nhau: bản ghi nào bị Silver loại thì xuất
hiện ở quarantine, không thừa không thiếu. Lý do bị loại được phân loại thành 4 kiểu
để người trực đọc là biết phải làm gì:

| `reject_reason` | số hàng |
|---|---|
| priority là số nhưng ngoài miền 1..4: `0` / `5` / `-1` | 49 / 37 / 32 |
| priority rỗng: nguồn gửi chuỗi trắng | 43 |
| priority là chuỗi không có trong bảng quy đổi: `P1` / `unknown` / `P2` | 39 / 39 / 38 |
| priority thiếu: nguồn gửi NULL | 35 |

> **Nên chặn dữ liệu lỗi ở tầng Bronze hay tầng Silver?**
>
> Ở Silver. Bronze phải giữ nguyên payload gốc, kể cả payload sai — vì Bronze là bản
> ghi *nguồn đã gửi gì*, không phải bản ghi *ta muốn nhận gì*. Nếu Bronze từ chối
> hàng lỗi, bản ghi đó không tồn tại ở đâu cả, và toàn bộ việc điều tra sự cố về sau
> mất căn cứ: chính nhờ Bronze còn giữ `priority_raw` nguyên dạng mà mới xác định
> được nguồn bắt đầu đổi format từ 08-10, phân loại được ba nhóm giá trị, và đếm được
> đúng 312 bản ghi lỗi. Sau khi bổ sung bảng quy đổi, cũng chính Bronze cho phép
> **tính lại** lịch sử — nếu đã chặn từ đầu thì 7.142 nhãn chữ mất luôn, không backfill
> được. Nguyên tắc: Bronze trung thực, Silver mới là nơi hợp đồng được phát biểu và
> thực thi.
>
> **Vì sao không để `dbt test` fail và dừng cả DAG khi gặp hàng lỗi?**
>
> Vì bán kính ảnh hưởng của việc dừng lớn hơn bán kính ảnh hưởng của lỗi vài bậc.
> 312 bản ghi hỏng trên 14.300 (2,2%) không có quyền chặn 12.480 ticket hợp lệ,
> 130.683 event và 31.200 chunk RAG đang chờ tới tay người dùng. Dừng DAG biến một sự
> cố chất lượng dữ liệu cục bộ thành một sự cố mất dịch vụ toàn hệ thống — và vẫn
> không sửa được bản ghi hỏng, vì việc đó cần người đọc và quyết định.
>
> Quarantine + chạy tiếp đổi một *hard failure* thành một *hàng đợi có kích thước đo
> được*: pipeline giữ SLA, còn 312 hàng kia nằm chờ người trực với lý do bị loại ghi
> sẵn. Điều đó **không** đồng nghĩa bỏ `dbt test`: test vẫn phải fail và phải dừng —
> nhưng dừng khi dữ liệu **đã làm sạch** vi phạm contract, tức khi chính lớp phòng
> vệ hỏng. Phân biệt hai loại tín hiệu: "nguồn gửi rác" là chuyện thường ngày, cần
> định tuyến; "dữ liệu sau khi làm sạch vẫn sai" là lỗi của ta, cần dừng.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **A và B** |

### Bài A — query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Phiếu #1052: dashboard mất 38 giây, ba tháng trước 2 giây, không ai sửa dòng code nào. |
| **Nguyên nhân** | Hai nguyên nhân cộng hưởng, cùng một chủ đề: *engine không có cách nào biết file nào vô ích trước khi mở nó.* (1) **Small-file problem** — dataset là 5.000 file phẳng cho 130.683 hàng, tức ~26 hàng/file. DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, nên 5.000 file tí hon tốn 5.000.000 đơn vị công quét cho một tập chỉ có 130.683 hàng: 97% công việc là chi phí mở file, không phải đọc dữ liệu. Không ai sửa code vì nguyên nhân không nằm trong code — nó nằm ở việc số file lớn dần theo thời gian. (2) **Predicate không sargable** — `where strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call, nên engine không so được kết quả hàm với tên thư mục partition, cũng không so được với thống kê min/max của row group; nó buộc phải đọc hết rồi mới biết hàng nào dùng được. |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... TO 'data/gold_events_v2' (format parquet, partition_by (event_date), row_group_size 2048)`, `order by customer_name, event_time`. `queries/dashboard.sql`: đọc dataset mới với `hive_partitioning = true` và viết lại điều kiện thành `event_date = date '2026-08-09'`. |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324 (536,3×)** · `files` **5.000 → 14** · `result hash` `4379e4c5d9f3` **không đổi** · không mất hàng nào (130.683 → 130.683) |

Đóng góp của từng quyết định, **đo riêng** thay vì suy luận:

| Layout | rows scanned | |
|---|---|---|
| 5.000 file phẳng, filter bọc `strftime` (hiện trạng) | 5.000.000 | — |
| 5.000 file phẳng, filter viết lại sargable | 5.000.000 | sargable **một mình không giúp gì** |
| gộp file + sắp thứ tự, **không** partition | 130.683 | 38× — hết chi phí làm tròn theo file |
| 14 partition, **không** sắp thứ tự | 9.324 | 536× |
| 14 partition + sắp thứ tự ← bản đã chọn | 9.324 | 536× |

Ba điều đọc ra từ bảng này:

- Viết lại predicate cho sargable **một mình không đem lại gì** (vẫn 5.000.000). Nó
  chỉ có giá trị *sau khi* đường dẫn mang thông tin ngày — hai thay đổi này phải đi
  cùng nhau, và đó cũng là lý do "tối ưu query" đơn lẻ không cứu được dashboard này.
- Phần lớn lợi ích đến từ **gộp file** (38×), phần còn lại từ **partition pruning** (14×).
- `order by customer_name` đóng góp **bằng không** vào chỉ số này. Ý định ban đầu là
  để min/max của row group loại được row group không chứa `ACME`, nhưng đo ra 9.324 dù
  có sắp hay không: `ACME` vừa là tên đứng đầu alphabet vừa chiếm ~37% số hàng mỗi
  ngày, nên dù đã sắp nó vẫn trải gần nửa file và không row group nào bị loại. Vẫn giữ
  `ORDER BY` vì nó làm dữ liệu clustered theo khách (có ích cho khách hàng hiếm và cho
  tỷ lệ nén), nhưng **không** được kể công cho con số 536×.

Vì sao partition theo `event_date` mà không theo `customer_name`: `event_date` có 14
giá trị phân biệt → 14 thư mục. `customer_name` có 650 giá trị → 650 thư mục cho
130.683 hàng (~200 hàng/thư mục), tức tái tạo đúng small-file problem vừa sửa, chỉ đổi
hình dạng. Nguyên tắc: cột lọc có ít giá trị thì partition, cột lọc có nhiều giá trị
thì xử bằng thứ tự hàng trong file.

`row_group_size 2048`: một ngày chỉ ~9.330 hàng, mặc định 122.880 nghĩa là cả ngày gói
trong **một** row group — min/max của nó phủ toàn bộ 650 khách và mất hết tác dụng lọc.
2.048 là kích thước vector của DuckDB, nhỏ hơn nữa thì chi phí metadata lấn phần công
đọc tiết kiệm được.

### Bài B — consumer bị giết giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test`: bị giết ở lô 7 rồi khởi động lại, kết quả **mất 500 hàng** (19.500 so với 20.000 của lượt chạy không sự cố). |
| **Nguyên nhân** | Thứ tự thao tác trong `consume()` là `commit()` → crash → `write_batch()`, tức **at-most-once**: offset được ghi nhận *trước* khi dữ liệu được ghi. Chết ở lô 7 nghĩa là offset đã dịch tới 3.500 trong khi chỉ 3.000 hàng thực sự nằm trong kho; lần khởi động lại đọc từ 3.500 nên lô 7 không bao giờ được xử lý. Cơ chế tổng quát: `commit` là một lời hứa "mọi thứ tới vị trí này đã xử lý xong", nên hứa trước khi làm thì mọi lần chết giữa chừng đều biến thành mất dữ liệu vĩnh viễn — và mất **im lặng**, vì offset trông hoàn toàn hợp lệ sau khi restart. |
| **Cách khắc phục** | `ingest/consumer.py`: (a) đảo thành `write_batch()` → crash → `commit()` (at-least-once); (b) `event_id varchar primary key` trong `DDL` và `insert ... on conflict (event_id) do update set ...` trong `write_batch()` để phép ghi trở nên idempotent. |
| **Bằng chứng** | trước: A = 20.000, C = 19.500 → **mất 500 hàng** · sau: A = 20.000, C = **20.000**, 20.000 `event_id` khác nhau → **không mất, không trùng**, `C == A` ✓ (khởi động lại ghi 17.000 message, tức đã phát lại 500 message của lô 7, và upsert hấp thụ hết) |

Đảo thứ tự **một mình là chưa đủ** — nó chỉ đổi loại lỗi: at-least-once nghĩa là lô 7
được ghi hai lần, và với `INSERT` thuần thì "mất 500" thành "trùng 500". Exactly-once
không tồn tại ở tầng giao vận; thứ chọn được là **at-least-once cộng một phép ghi
idempotent**, và cặp đó cho hiệu quả tương đương exactly-once ở trạng thái cuối.

**`DO UPDATE` khác `DO NOTHING` ở đâu khi một message được phát lại với nội dung đã đổi?**
`DO NOTHING` giữ lại bản ghi tới trước, nên trạng thái đích phụ thuộc vào *lần nào tới
trước* — với cùng một log, hai lần replay khác nhau có thể cho hai kết quả khác nhau.
`DO UPDATE` luôn hội tụ về nội dung của lần phát sau; vì replay luôn diễn ra theo đúng
thứ tự log, "lần sau" cũng chính là "mới hơn theo log", nên trạng thái cuối trùng khớp
với lượt chạy không sự cố — đúng tiêu chí `C == A`. Chọn `DO UPDATE`.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lần trên cùng dữ liệu và so checksum. Câu hỏi không phải "nó có chạy không" mà "kết quả có phụ thuộc số lần chạy không". Với mỗi model incremental, đọc câu lệnh ghi **thật** mà dbt sinh ra (`dbt/target/run/...`) chứ không đọc `config()` rồi đoán: `INSERT` là ghi cộng dồn, và ở đâu có ghi cộng dồn thì ở đó mọi cơ chế retry đều là cơ chế nhân bản. |
| 2 | Đo độ lệch giữa thời điểm sự kiện **xảy ra** và thời điểm nó **tới kho**, rồi đối chiếu với watermark mà model đang dùng. Watermark theo event time cộng dữ liệu về muộn là mất dữ liệu vĩnh viễn và im lặng. Đồng thời không tin cột "ổn định" như bằng chứng của tính đúng — một phép biến đổi tất định vẫn tất định khi nó bỏ sót dữ liệu. |
| 3 | Tìm những cột **không có ai phát biểu điều kỳ vọng** về chúng: không contract, không test miền giá trị. Đó là nơi một thay đổi ở nguồn sẽ đi qua mà không ai biết. Kiểm tra bằng cách so phân bố giá trị ở Bronze với ở Silver theo từng ngày — chỗ phân bố đổi đột ngột là ngày hợp đồng bị phá, và nếu `dbt test` vẫn xanh ở ngày đó thì phép đo đã hỏng trước cả dữ liệu. |

---

## Bảng tự chấm

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | `8dd7c98653` ×3 (và ×5) | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | `3db448685c` ×3 | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 11/11 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,7258 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 tiêu chí | 4/4 tiêu chí | ✓ |
| *(thưởng)* bài A — rows scanned | 5.000.000 → 9.324 (536,3×) | ≥ 10× | ✓ |
| *(thưởng)* bài B — crash-test | ĐẠT | ĐẠT | ✓ |

Danh sách file đã sửa (đều nằm trong nhóm được phép sửa; `expected/`,
`seed/generate.py`, `tools/verify.py`, `tools/explain.py`, `tools/common.py`
không bị chạm):

```
dags/ai_training_pipeline.py               dbt/models/silver/schema.yml
dbt/macros/normalize_priority.sql          dbt/models/silver/silver_tickets.sql
dbt/models/gold/gold_feature_daily.sql     ingest/consumer.py
dbt/models/gold/gold_training_set.sql      queries/dashboard.sql
dbt/models/silver/quarantine_tickets.sql   tools/compact.py
```

---

## Phụ lục — một sự cố của môi trường, không thuộc nội dung lab

Trên máy chạy lab (WSL2), `make verify` ban đầu **không chạy nổi tới bảng kết quả**:
dbt đổ ở một ngày ngẫu nhiên với

```
Compilation Error in model gold_doc_chunks
  dbt was unable to infer all dependencies for the model "gold_doc_chunks".
  This typically happens when ref() is placed within a conditional block.
```

Thông báo này gây nhầm đường, vì không model nào đặt `ref()` trong khối điều kiện.
Quá trình khoanh vùng:

1. Lỗi **không** xác định: lần thì ở run 3 ngày 08-04, lần thì run 3 ngày 08-06.
2. Tắt partial parse làm lỗi **nặng hơn** (đổ ngay run 1) → thủ phạm là bước
   **full re-parse**, không phải cache.
3. Hạ dbt về 1.9.11 vẫn lỗi → không phải hồi quy theo version.
4. Chỉ 3 model dùng `ref()` đổ, còn 4 model dùng `source()` thì pass; dump manifest
   cho thấy `refs` **vẫn được capture** (= 1) nhưng `depends_on.nodes` **rỗng** cho
   *toàn bộ* node cùng lúc → dbt đã bỏ hẳn bước link `process_refs()`.
5. Đọc `dbt/parser/manifest.py`: `started_at = time.time()` (dòng 290), và
   `process_refs()` **bỏ qua** mọi node có `created_at < self.started_at` (dòng 1296) —
   một phép so sánh trên **wall clock**.
6. Đo `time.time()` so với `time.monotonic()`: đồng hồ hệ thống **nhảy lùi 561,9 giây**,
   3 lần trong 12 giây lấy mẫu, `timedatectl` báo `System clock synchronized: no`
   (systemd-timesyncd tranh chấp với Hyper-V host time sync — cũng chính là nguồn của
   cảnh báo `Clock skew detected` mà `make` in ra).

Root cause: đồng hồ nhảy lùi giữa lúc `ManifestLoader` khởi tạo và lúc node được tạo
→ node mới sinh trông như "cũ hơn" loader → dbt tưởng chúng đã được link từ trước →
`depends_on` rỗng → `ref()` đổ lúc chạy.

Khắc phục tại chỗ: một file `.pth` trong `.venv` bọc `time.time()` cho nó không bao giờ
giảm (khi đọc được giá trị nhỏ hơn lần trước thì cộng dồn tiếp bằng `time.monotonic()`).
Sau đó 30/30 lượt parse giữ đủ `depends_on`, và `make verify` chạy trọn vẹn. Bản vá nằm
**ngoài** repo (trong `.venv`, vốn bị `.gitignore`) nên không ảnh hưởng bài nộp; nếu
dựng lại venv thì cần đặt lại. Cách sửa gốc cần quyền root:
`sudo timedatectl set-ntp false && sudo hwclock -s`.

Ghi lại ở đây vì đây đúng là một tình huống vận hành thật: thông báo lỗi trỏ vào tầng
sai (dbt/`ref()`), lỗi không tái hiện ổn định, và root cause nằm hai tầng bên dưới —
ở đồng hồ của máy.
