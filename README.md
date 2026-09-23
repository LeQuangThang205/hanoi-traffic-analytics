# Hanoi Traffic Analytics

He thong thu thap va phan tich du lieu giao thong ket hop thoi tiet tai Ha Noi,
su dung Apache Spark (PySpark Local Mode). Chi tiet kien truc/roadmap xem `PROJECT_CONTEXT.md`.

Trang thai: **Phase 1 — Project Foundation** (`CURRENT_PHASE = 1`).

## Cai dat (Phase 0/1)

```bash
pip install -r requirements.txt
copy .env.example .env
```

Dien `TOMTOM_API_KEY` vao `.env`. Khong commit `.env`.

## Chay thu Spark (Phase 0)

```bash
python test_spark.py
```

## Cau truc (Phase 1)

```text
config.py            # doc secret tu .env
data/roads.csv       # danh sach tuyen duong
data/traffic_data.csv  # (Phase 2+) du lieu thu thap
processed/           # (Phase 5+) output Spark
utils/               # helper (Phase 2+)
collector.py / spark_analysis.py / app.py  # placeholder
```

Chi tiet luat phat trien xem `AGENTS.md`.
