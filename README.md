# Bài tập lớn: Wikipedia – Bảng dân số quốc gia (UN)
Nội dung bài tập:
{ country, region, subregion, population:int, area_km2:float, density:float }
•	Q1 (tổng dân số/region): region\tpopulation → cộng.
•	Q2 (top mật độ): có thể 1-pass bằng reducer giữ top-10 theo density (lọc area_km2>0).
•	Q3 (phân phối pop_bucket): pop_bucket\t1.

## Mục tiêu tổng quát

Dự án thu thập dữ liệu dân số quốc gia từ Wikipedia, lưu trữ vào MongoDB, thực hiện ETL bằng Hadoop Streaming, trả lời các câu hỏi phân tích Q1–Q4 và huấn luyện mô hình phân cụm bằng Spark MLlib. Kết quả cuối cùng được phục vụ thông qua một API Express và giao diện web một nút bấm hiển thị báo cáo bằng tiếng Việt.

## Luồng dữ liệu tổng quan

1. **Scrape (Python)**
  - `src/main.py` thu thập dữ liệu dân số và khu vực → lưu vào các collection MongoDB: `wiki_population_raw`, `wiki_area_raw`, `wiki_pop_raw`.
2. **Export (Python)**
  - `src/export_for_hadoop.py` xuất `wiki_pop_raw` thành file JSONL cho Hadoop.
  - `src/export_graph_for_hadoop.py` xuất đồ thị ảnh hưởng thành JSONL phục vụ Q4.
3. **Hadoop Streaming (Python mapper/reducer)**
  - `hadoop/etl` làm sạch dữ liệu, tạo `pop_clean.jsonl`.
  - `hadoop/q1`, `hadoop/q2`, `hadoop/q3`, `hadoop/q4` trả lời các câu hỏi.
  - Kết quả kéo về thư mục `data/hadoop/` ở dạng: `q1.tsv`, `q2.jsonl`, `q3.tsv`, `q4.jsonl`.
4. **Spark MLlib (PySpark)**
  - `spark/train_country_clusters.py` đọc `pop_clean.jsonl`, chuẩn hóa đặc trưng và huấn luyện KMeans.
  - Ghi tóm tắt vào `data/spark/training_summary.json`.
5. **Web/API (Node.js + Express)**
  - `server/index.js` chỉ cung cấp `GET /api/report`.
  - Khi gọi, API đọc mẫu từ MongoDB và ghép kết quả Hadoop + Spark từ các file ở `data/`.
  - `web/index.html` (tiếng Việt) có một nút duy nhất tạo báo cáo.

## Chuẩn bị môi trường

1. **Python**
  ```bash
  pip install -r requirements.txt
  ```

2. **Node.js**
  ```bash
  cd server
  npm install
  ```

3. **PySpark (trên máy hoặc cluster)**
  ```bash
  pip install pyspark
  ```

4. **Biến môi trường (.env)**
  ```env
  MONGODB_URI=mongodb://localhost:27017
  DB_NAME=wikipedia_db
  PORT=3000
  REPORT_SAMPLE_LIMIT=20
  ```
  Tùy chọn:
  ```env
  HADOOP_OUTPUT_DIR=d:/KetMonWebandAI/data/hadoop
  SPARK_OUTPUT_DIR=d:/KetMonWebandAI/data/spark
  ```

## 1. Thu thập dữ liệu vào MongoDB

```bash
cd src
python main.py
```

Collections tạo ra:
- `wiki_population_raw`: dữ liệu dân số gốc + bảng HTML
- `wiki_area_raw`: dữ liệu khu vực
- `wiki_pop_raw`: bảng đã chuẩn hóa (Population + Region)

## 2. Chuẩn bị dữ liệu cho Hadoop

```bash
# Xuất từ MongoDB -> data/population_raw.jsonl
python src/export_for_hadoop.py

# Xuất đồ thị ảnh hưởng -> data/graph_edges.jsonl
python src/export_graph_for_hadoop.py
```

Upload hai file này lên HDFS (ví dụ):

```bash
hdfs dfs -mkdir -p /data/wiki/bronze
hdfs dfs -put -f data/population_raw.jsonl /data/wiki/bronze/
hdfs dfs -put -f data/graph_edges.jsonl /data/wiki/bronze/
```

## 3. ETL với Hadoop Streaming

```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/bronze/population_raw.jsonl \
  -output /data/wiki/silver/pop_clean_temp \
  -mapper hadoop/etl/mapper.py \
  -reducer hadoop/etl/reducer.py \
  -file hadoop/etl/mapper.py \
  -file hadoop/etl/reducer.py

hoac

hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /data/wiki/bronze/population_raw.jsonl \
    -output /data/wiki/silver/pop_clean_temp \
    -mapper /hadoop/etl/mapper.py \
    -reducer /hadoop/etl/reducer.py \
    -file /hadoop/etl/mapper.py \
    -file /hadoop/etl/reducer.py

hdfs dfs -getmerge /data/wiki/silver/pop_clean_temp/part-* data/pop_clean.jsonl
hdfs dfs -put -f data/pop_clean.jsonl /data/wiki/silver/pop_clean.jsonl
```

## 4. Chạy các câu hỏi Hadoop Q1–Q4

### Q1 – Tổng dân số theo khu vực
```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q1 \
  -mapper hadoop/q1/mapper.py \
  -reducer hadoop/q1/reducer.py \
  -file hadoop/q1/mapper.py \
  -file hadoop/q1/reducer.py

hdfs dfs -getmerge /data/wiki/results/q1/part-* data/hadoop/q1.tsv
```

### Q2 – Top 10 mật độ dân số cao nhất
```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q2 \
  -mapper hadoop/q2/mapper.py \
  -reducer hadoop/q2/reducer.py \
  -file hadoop/q2/mapper.py \
  -file hadoop/q2/reducer.py

hdfs dfs -getmerge /data/wiki/results/q2/part-* data/hadoop/q2.jsonl
```

### Q3 – Phân phối theo bucket dân số
```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q3 \
  -mapper hadoop/q3/mapper.py \
  -reducer hadoop/q3/reducer.py \
  -file hadoop/q3/mapper.py \
  -file hadoop/q3/reducer.py

hdfs dfs -getmerge /data/wiki/results/q3/part-* data/hadoop/q3.tsv
```

### Q4 – Bậc vào/ra từ đồ thị ảnh hưởng Wikipedia
```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/bronze/graph_edges.jsonl \
  -output /data/wiki/results/q4 \
  -mapper hadoop/q4/mapper.py \
  -reducer hadoop/q4/reducer.py \
  -file hadoop/q4/mapper.py \
  -file hadoop/q4/reducer.py \
  -D mapreduce.job.reduces=1

hdfs dfs -getmerge /data/wiki/results/q4/part-* data/hadoop/q4.jsonl
```

> Các file trong `data/hadoop/` là nguồn dữ liệu mà API Express sử dụng.

## 5. Huấn luyện cụm với Spark MLlib

```bash
spark-submit \
  --master local[*] \
  spark/train_country_clusters.py \
  --input data/pop_clean.jsonl \
  --output data/spark/training_summary.json \
  --clusters 4
```

## 6. Khởi chạy backend & web

```bash
cd server
npm start
# server chạy tại http://localhost:3000
```

Mở trình duyệt tới `http://localhost:3000`. Trang web chỉ có **một nút “Tạo Báo Cáo Tổng Hợp”**. Khi nhấn nút, frontend gọi `/api/report` và hiển thị báo cáo gồm:

- Mẫu dữ liệu MongoDB
- Kết quả Hadoop Q1–Q4
- Tóm tắt huấn luyện Spark KMeans

Tất cả nội dung hiển thị bằng tiếng Việt, code và ghi chú trong dự án bằng tiếng Anh.

## 7. Kiểm thử nhanh không cần Hadoop (tùy chọn)

```bash
cat data/population_raw.jsonl | python hadoop/etl/mapper.py | sort | python hadoop/etl/reducer.py > data/pop_clean_local.jsonl
cat data/pop_clean_local.jsonl | python hadoop/q1/mapper.py | sort | python hadoop/q1/reducer.py > data/hadoop/q1.tsv
```

## Lưu ý triển khai EC2

- Cài đặt MongoDB Atlas hoặc MongoDB local trên EC2.
- Cài Java, Hadoop, Spark, Python 3.10+, Node.js 18+.
- Sao chép `.env` với thông tin kết nối MongoDB.
- Mở port 3000 hoặc dùng Nginx reverse proxy.
- Thiết lập cron hoặc script để tái chạy pipeline (scrape → Hadoop → Spark) khi cần cập nhật dữ liệu.



## Giao diện web + backend Node.js

- Backend Express + Mongoose nằm trong thư mục `server/`. API duy nhất:
  - `GET /api/report` đọc mẫu dữ liệu từ MongoDB, ghép kết quả Hadoop Q1–Q4 và tóm tắt Spark.
- Giao diện web tĩnh (`web/`) phục vụ báo cáo tiếng Việt với một nút duy nhất "Tạo Báo Cáo Tổng Hợp".
- Server đọc cấu hình MongoDB và đường dẫn kết quả từ `.env`: `MONGODB_URI`, `DB_NAME`, `REPORT_SAMPLE_LIMIT`, `HADOOP_OUTPUT_DIR`, `SPARK_OUTPUT_DIR`.
- Cài đặt:
  1. Python dependencies: `pip install -r requirements.txt`.
  2. Node dependencies: `cd server && npm install`.
  3. Chạy backend: `npm start` (mặc định tại `http://localhost:3000`).
  4. Mở trình duyệt và nhấn nút để sinh báo cáo.


wikipedia-scrape/
# HƯỚNG DẪN CHẠY DỰ ÁN - Wikipedia Population Scraping & MapReduce

## Cấu trúc dự án

```
project/
├── src/                      # Python scraping & export scripts
│   ├── main.py               # Thu thập dữ liệu vào MongoDB
│   ├── scrape.py             # Scraping logic
│   ├── storage.py            # Hàm đọc/ghi MongoDB
│   ├── export_for_hadoop.py  # Xuất collection wiki_pop_raw -> JSONL
│   ├── export_graph_for_hadoop.py  # Chuyển graph.json -> graph_edges.jsonl
│   └── standardize_country.py
│
├── hadoop/                   # MapReduce jobs (ETL + Q1–Q4)
│   ├── etl/
│   ├── q1/
│   ├── q2/
│   ├── q3/
│   └── q4/
│
├── spark/                    # Spark MLlib training script
├── server/                   # Express API
├── web/                      # Frontend (1 nút báo cáo tiếng Việt)
├── data/
│   ├── hadoop/               # Q1–Q4 merge results (JSONL/TSV)
│   └── spark/                # Spark summary JSON
└── requirements.txt
```

---

## HƯỚNG DẪN CHẠY

### 1. Scrape & lưu vào MongoDB

```bash
cd src
python main.py
```

Kết quả MongoDB:
- `wiki_population_raw`
- `wiki_area_raw`
- `wiki_pop_raw` (bảng đã chuẩn hóa)

### 2. Xuất dữ liệu và tải lên HDFS

```bash
python export_for_hadoop.py
python export_graph_for_hadoop.py

hdfs dfs -mkdir -p /data/wiki/bronze
hdfs dfs -put -f data/population_raw.jsonl /data/wiki/bronze/
hdfs dfs -put -f data/graph_edges.jsonl /data/wiki/bronze/
```

### 3. ETL bằng Hadoop Streaming

```bash
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/bronze/population_raw.jsonl \
  -output /data/wiki/silver/pop_clean_temp \
  -mapper hadoop/etl/mapper.py \
  -reducer hadoop/etl/reducer.py \
  -file hadoop/etl/mapper.py \
  -file hadoop/etl/reducer.py

hdfs dfs -getmerge /data/wiki/silver/pop_clean_temp/part-* data/pop_clean.jsonl
hdfs dfs -put -f data/pop_clean.jsonl /data/wiki/silver/pop_clean.jsonl
```

### 4. Chạy Q1–Q4 (MapReduce)

```bash
# Q1: Tổng dân số theo khu vực
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q1 \
  -mapper hadoop/q1/mapper.py \
  -reducer hadoop/q1/reducer.py \
  -file hadoop/q1/mapper.py \
  -file hadoop/q1/reducer.py
hdfs dfs -getmerge /data/wiki/results/q1/part-* data/hadoop/q1.tsv

# Q2: Top mật độ dân số
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q2 \
  -mapper hadoop/q2/mapper.py \
  -reducer hadoop/q2/reducer.py \
  -file hadoop/q2/mapper.py \
  -file hadoop/q2/reducer.py
hdfs dfs -getmerge /data/wiki/results/q2/part-* data/hadoop/q2.jsonl

# Q3: Phân phối bucket dân số
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q3 \
  -mapper hadoop/q3/mapper.py \
  -reducer hadoop/q3/reducer.py \
  -file hadoop/q3/mapper.py \
  -file hadoop/q3/reducer.py
hdfs dfs -getmerge /data/wiki/results/q3/part-* data/hadoop/q3.tsv

# Q4: Thống kê độ ảnh hưởng từ graph_edges.jsonl
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/bronze/graph_edges.jsonl \
  -output /data/wiki/results/q4 \
  -mapper hadoop/q4/mapper.py \
  -reducer hadoop/q4/reducer.py \
  -file hadoop/q4/mapper.py \
  -file hadoop/q4/reducer.py \
  -D mapreduce.job.reduces=1
hdfs dfs -getmerge /data/wiki/results/q4/part-* data/hadoop/q4.jsonl
```

### 5. Huấn luyện Spark MLlib

```bash
spark-submit \
  --master local[*] \
  spark/train_country_clusters.py \
  --input data/pop_clean.jsonl \
  --output data/spark/training_summary.json \
  --clusters 4
```

### 6. Khởi chạy backend và tạo báo cáo web

```bash
cd server
npm install
npm start
# Mở http://localhost:3000 và nhấn "Tạo Báo Cáo Tổng Hợp"
```

## TEST LOCAL (KHÔNG CẦN HADOOP)

```bash
cd src
python export_for_hadoop.py
cat ../data/population_raw.jsonl | python ../hadoop/etl/mapper.py | sort | python ../hadoop/etl/reducer.py > ../data/pop_clean_local.jsonl

cat ../data/pop_clean_local.jsonl | python ../hadoop/q1/mapper.py | sort | python ../hadoop/q1/reducer.py
cat ../data/pop_clean_local.jsonl | python ../hadoop/q2/mapper.py | sort | python ../hadoop/q2/reducer.py
cat ../data/pop_clean_local.jsonl | python ../hadoop/q3/mapper.py | sort | python ../hadoop/q3/reducer.py
```


## MÔ TẢ LOGIC

### ETL Mapper
- Bỏ dấu phẩy trong số
- Chuyển N/A → null
- Filter: country rỗng, population/area/density ≤ 0
- Chuẩn hóa Region/Subregion (title case)
- Thêm pop_bucket: <1M, 1M-10M, 10M-50M, 50M-100M, >100M

### Q1: Tổng dân số/Region
- Mapper: region → population
- Reducer: Sum(population) group by region

### Q2: Top 10 mật độ
- Mapper: density → json_record (filter area_km2 > 0)
- Reducer: Heap giữ top 10, sort DESC

### Q3: Phân phối pop_bucket
- Mapper: pop_bucket → 1
- Reducer: Count group by pop_bucket

---

## YÊU CẦU

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
pandas
beautifulsoup4
requests
pymongo
python-dotenv
```

**Environment (.env):**
```
MONGODB_URI=mongodb://localhost:27017
DB_NAME=wikipedia_db
```


## LLƯU Ý

1. **Mapper/Reducer phải có quyền execute:**
   ```bash
   chmod +x hadoop/**/*.py
   ```

2. **HDFS phải sẵn sàng:**
   ```bash
   hdfs dfs -ls /
   ```

3. **MongoDB phải chạy:**
   ```bash
   mongod --port 27017
   ```

4. **Test local trước khi chạy Hadoop!**


## KẾT QUẢ MONG ĐỢI

| Phase | Output | Location |
|-------|--------|----------|
| Phase 1 | 3 MongoDB collections | `wiki_pop_raw`, `wiki_area_raw`, `wiki_joined` |
| Phase 2 | Clean JSONL | `/data/wiki/silver/pop_clean.jsonl` |
| Q1 | Region populations | `/data/wiki/results/q1_*/` |
| Q2 | Top 10 densities | `/data/wiki/results/q2_*/` |
| Q3 | Bucket distribution | `/data/wiki/results/q3_*/` |

