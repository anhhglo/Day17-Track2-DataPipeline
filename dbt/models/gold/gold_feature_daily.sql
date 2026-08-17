-- ---------------------------------------------------------------------------
-- gold_feature_daily — đặc trưng theo ngày cho agent định tuyến.
-- Grain: 1 hàng / 1 cặp (event_date, customer_id).
-- ---------------------------------------------------------------------------
-- KHUNG THỰC HIỆN — NHIỆM VỤ 2
--
--   Mỗi ngày vận hành, model chỉ tính lại phần "mới". Định nghĩa "mới" nằm ở
--   khối is_incremental() bên dưới:
--
--       WHERE event_date <toán tử> (
--                 SELECT max(event_date) FROM <bảng đích>
--             ) <lùi lại bao nhiêu ngày?>
--
--   Câu hỏi cần trả lời trước khi sửa:
--     1. Đo phân bố của (_ingested_at - event_time) trong bronze_events.
--        P99 bằng bao nhiêu? Bao nhiêu phần trăm bản ghi tới kho muộn hơn
--        một ngày so với lúc sự kiện xảy ra?
--     2. Một bản ghi có event_date = 08-12 nhưng _ingested_at = 08-15: hôm
--        08-15, max(event_date) trong bảng đích đang là bao nhiêu? Bản ghi
--        đó có thoả điều kiện lọc hiện tại không? Ngày hôm sau thì sao?
--     3. Window tính lại nên lùi bao nhiêu ngày? Căn cứ vào P99 hay vào max?
--        Mỗi ngày lùi thêm phải trả giá gì ở MỌI lượt chạy sau này?
--     4. Khi window mở rộng, cùng một (event_date, customer_id) sẽ được tính
--        lại nhiều lần. Cần thêm gì vào config() để lần tính sau THAY THẾ
--        lần tính trước thay vì cộng dồn? Grain này có mấy cột khoá?
--
--   Cảnh báo: sửa điều kiện lọc mà không xử lý ý 4 sẽ làm bảng mất tính ổn
--   định — make verify in riêng hai cột "ỔN ĐỊNH" và "SỐ HÀNG" để bạn thấy
--   rõ hai vấn đề này tách nhau.
-- ---------------------------------------------------------------------------

{{ config(
    materialized         = 'incremental',
    unique_key           = ['event_date', 'customer_id'],
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
-- Lookback window 3 ngày.
--
-- Vì sao không dùng `>` (hay `>=`) trên watermark: watermark là max(event_date)
-- ĐÃ CÓ trong bảng đích, tức mốc theo thời điểm sự kiện XẢY RA. Nhưng dữ liệu
-- được nạp theo thời điểm nó TỚI KHO. Đo trên bronze_events:
--
--     P50 = 0,13 ngày · P95 = 1,81 · P99 = 2,73 · max = 2,94
--     5,05% bản ghi tới kho muộn hơn một ngày so với lúc sự kiện xảy ra
--
-- Phân bố có hai cụm tách biệt: cụm đúng hạn 0–6 giờ, và cụm về muộn 43–71
-- giờ. Một event xảy ra 08-12 nhưng tới kho 08-15 có event_date = 08-12, nhỏ
-- hơn watermark (đã là 08-14), nên `>` loại nó — và loại vĩnh viễn, vì
-- watermark chỉ tăng. Đổi sang `>=` chỉ nới thêm đúng một ngày, không đủ.
--
-- Chọn 3 ngày = ceil(P99 = 2,73). Con số này cũng phủ luôn max = 2,94 ngày,
-- và khớp với đo thực tế: bản ghi muộn nhất tới kho vào event_date + 3 ngày.
-- Giá phải trả là ở MỌI lượt chạy về sau, không phải một lần: mỗi lượt tính
-- lại ~4 ngày dữ liệu thay vì 1 ngày. Lùi thêm ngày nào cũng tốn thêm chừng
-- đó công quét mỗi đêm, nên không lùi theo max cho "chắc ăn".
--
-- Window rộng hơn nghĩa là cùng một (event_date, customer_id) được tính lại ở
-- nhiều lượt chạy, nên config() phải có unique_key hai cột + strategy merge:
-- lần tính sau THAY THẾ lần trước. Nếu chỉ nới window mà vẫn insert thì bảng
-- này lặp lại đúng lỗi của nhiệm vụ 1.
where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
{% endif %}

group by 1, 2, 3, 4
