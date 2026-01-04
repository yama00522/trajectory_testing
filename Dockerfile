FROM python:3.11-slim

WORKDIR /app

# 依存パッケージをコピーしてインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 必要なファイルをコピー
COPY run_pipeline.py .
COPY csv_merger.py .

# 出力ディレクトリを作成
RUN mkdir -p output_csv merged_csv merged_outdate

# パイプラインを実行
CMD ["python", "run_pipeline.py"]
