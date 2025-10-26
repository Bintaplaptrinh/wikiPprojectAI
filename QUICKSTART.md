# QUICK START

## 1. Scrape dữ liệu vào MongoDB
```bash
cd src
python main.py
```

## 2. Xuất dữ liệu cho Hadoop
```bash
python export_for_hadoop.py
python export_graph_for_hadoop.py

# Đưa file lên HDFS (ví dụ)
hdfs dfs -mkdir -p /data/wiki/bronze
hdfs dfs -put -f data/population_raw.jsonl /data/wiki/bronze/
hdfs dfs -put -f data/graph_edges.jsonl /data/wiki/bronze/
```

## 3. Chạy Hadoop Streaming
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

# Q1 – Tổng dân số theo khu vực
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q1 \
  -mapper hadoop/q1/mapper.py \
  -reducer hadoop/q1/reducer.py \
  -file hadoop/q1/mapper.py \
  -file hadoop/q1/reducer.py
hdfs dfs -getmerge /data/wiki/results/q1/part-* data/hadoop/q1.tsv

# Q2 – Top mật độ
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q2 \
  -mapper hadoop/q2/mapper.py \
  -reducer hadoop/q2/reducer.py \
  -file hadoop/q2/mapper.py \
  -file hadoop/q2/reducer.py
hdfs dfs -getmerge /data/wiki/results/q2/part-* data/hadoop/q2.jsonl

# Q3 – Phân phối dân số
hadoop jar $HADOOP_STREAMING_JAR \
  -input /data/wiki/silver/pop_clean.jsonl \
  -output /data/wiki/results/q3 \
  -mapper hadoop/q3/mapper.py \
  -reducer hadoop/q3/reducer.py \
  -file hadoop/q3/mapper.py \
  -file hadoop/q3/reducer.py
hdfs dfs -getmerge /data/wiki/results/q3/part-* data/hadoop/q3.tsv

# Q4 – Đồ thị ảnh hưởng (dùng data/graph_edges.jsonl)
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

## 4. Huấn luyện Spark MLlib
```bash
spark-submit \
  spark/train_country_clusters.py \
  --input data/pop_clean.jsonl \
  --output data/spark/training_summary.json
```

## 5. Khởi chạy web API
```bash
cd server
npm start
```

Xem `README.md` để biết hướng dẫn chi tiết từng bước.
