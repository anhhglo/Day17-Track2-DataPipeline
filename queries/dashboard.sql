-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Truy vấn không đổi ngữ nghĩa so với bản gốc — chỉ đổi HAI thứ:
--
--   1. Đọc dataset đã được tools/compact.py tái cấu trúc: 14 file có
--      partition theo ngày, thay cho 5.000 file phẳng. `hive_partitioning`
--      cho engine đọc giá trị event_date ra từ TÊN THƯ MỤC, nhờ đó nó loại
--      được 13/14 dữ liệu trước khi mở bất kỳ file nào.
--
--   2. Điều kiện lọc ngày viết lại cho SARGABLE. Bản gốc là
--
--          where strftime(event_time, '%Y-%m-%d') = '2026-08-09'
--
--      Cột bị bọc trong một function call, nên engine không so được kết quả
--      hàm đó với tên thư mục partition, cũng không so được với thống kê
--      min/max của row group — buộc phải đọc hết rồi mới biết hàng nào dùng
--      được. Viết `event_date = date '...'` thì cột đứng một mình ở một vế:
--      engine dùng được cả partition pruning lẫn row group pruning.
--      event_date là event_time::date nên tập hàng trả về y nguyên.

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  and event_date = date '2026-08-09'
group by 1
