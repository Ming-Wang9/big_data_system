FROM python:3-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install grpcio grpcio-tools protobuf pandas pyarrow

COPY table.proto .
RUN pip install grpcio-tools==1.70.0
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. table.proto

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY upload.py csvsum.py parquetsum.py bigdata.py ./

CMD ["python3", "-u", "server.py"]
