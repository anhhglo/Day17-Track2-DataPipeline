#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    src_rows = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    # ------------------------------------------------------------------
    # Ba quyết định của layout mới. Đóng góp của từng quyết định vào
    # `rows scanned` đã được đo riêng (query lọc ACME + ngày 2026-08-09):
    #
    #     5.000 file phẳng (hiện trạng)               5.000.000
    #     gộp file, có sắp thứ tự, KHÔNG partition      130.683   (38×)
    #     14 partition, KHÔNG sắp thứ tự                  9.324   (536×)
    #     14 partition + sắp thứ tự  <- bản này          9.324   (536×)
    #
    # partition_by (event_date)
    #   Dashboard lọc theo hai cột: customer_name và ngày của event_time.
    #   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi mở file, và
    #   thông tin duy nhất nó có trước đó là ĐƯỜNG DẪN. event_date có 14 giá trị
    #   -> 14 thư mục, lọc một ngày là bỏ qua 13/14 dữ liệu mà không đọc byte
    #   nào: 130.683 -> 9.324 rows scanned. Đây là phần đóng góp lớn thứ hai,
    #   và nó chỉ phát huy khi điều kiện lọc được viết sargable — `event_date =
    #   date '...'` chứ không phải `strftime(event_time, '%Y-%m-%d') = '...'`.
    #   Không partition theo customer_name: cột đó có 650 giá trị phân biệt, tức
    #   650 thư mục cho 130.683 hàng (~200 hàng/thư mục) — chính là small-file
    #   problem vừa sửa, chỉ đổi hình dạng.
    #
    # order by customer_name, event_time
    #   Ý định: xếp các hàng cùng một khách liền nhau để thống kê min/max của
    #   mỗi row group loại được row group không chứa 'ACME'.
    #   ĐO THỰC TẾ: đóng góp BẰNG KHÔNG (9.324 dù có sắp hay không). Lý do:
    #   'ACME' là tên đứng đầu alphabet VÀ chiếm ~37% số hàng mỗi ngày, nên dù
    #   đã sắp thứ tự nó vẫn nằm trong các row group đầu và trải rộng gần nửa
    #   file — không có row group nào bị loại. Vẫn giữ ORDER BY vì nó làm dữ
    #   liệu clustered theo khách (có ích cho khách hàng hiếm, và cho nén), song
    #   không được tính là nguyên nhân của con số 536× trong báo cáo.
    #
    # row_group_size 2048
    #   Một ngày chỉ ~9.330 hàng, mặc định là 122.880 -> cả ngày gói trong MỘT
    #   row group, min/max của nó phủ toàn bộ 650 khách và mất hết tác dụng lọc.
    #   2.048 = kích thước vector của DuckDB (nhỏ hơn nữa thì chi phí metadata
    #   lấn phần công đọc tiết kiệm được) -> 5 row group mỗi ngày.
    # ------------------------------------------------------------------
    con.execute(f"""
        copy (
            select * from read_parquet('{SRC}/*.parquet')
            order by customer_name, event_time
        ) to '{DST}' (
            format          parquet,
            partition_by    (event_date),
            overwrite_or_ignore,
            row_group_size  2048
        )
    """)

    dst_files = sorted(DST.glob("**/*.parquet"))
    dst_rows = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]

    assert dst_rows == src_rows, f"mất dữ liệu: {src_rows:,} -> {dst_rows:,}"

    print(f"  đích  : {DST}  ({len(dst_files):,} file / "
          f"{len(list(DST.glob('*')))} thư mục partition)")
    print(f"  số hàng: {src_rows:,} -> {dst_rows:,}  (không mất hàng nào)")
    print("\n  xong. Đo lại bằng:  make explain\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
