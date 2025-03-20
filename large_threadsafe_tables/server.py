from concurrent import futures
import grpc
import table_pb2
import table_pb2_grpc
import os
import uuid
import pandas as pd
import io
import pyarrow.parquet as pq
import pyarrow.compute as pc
import threading

class TableServicer(table_pb2_grpc.TableServicer):
    def __init__(self):
        self.uploads = {}
        self.csv_dir = "uploads/csv"
        self.parquet_dir = "uploads/parquet"
        self.lock = threading.Lock()  
        os.makedirs(self.csv_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)

    def Upload(self, request, context):
        try:
            file_id = str(uuid.uuid4())
            csv_path = os.path.join(self.csv_dir, f"{file_id}.csv")
            parquet_path = os.path.join(self.parquet_dir, f"{file_id}.parquet")

            with open(csv_path, 'wb') as f:
                f.write(request.csv_data)

            df = pd.read_csv(io.BytesIO(request.csv_data))
            df.to_parquet(parquet_path)

            with self.lock:
                self.uploads[file_id] = {
                    'csv': csv_path,
                    'parquet': parquet_path
                }

            return table_pb2.UploadResp(error="")

        except Exception as e:
            return table_pb2.UploadResp(error=f"Upload failed: {str(e)}")

    def ColSum(self, request, context):
        total = 0
        errors = []

        with self.lock:
            files = list(self.uploads.items())

        for file_id, entry in files:
            try:
                if request.format == "csv":
                    df = pd.read_csv(entry['csv'])
                    if request.column in df.columns:
                        total += df[request.column].sum().item()
                else:
                    pf = pq.ParquetFile(entry['parquet'])
                    if request.column not in pf.schema_arrow.names:
                        continue
                        
                    table = pq.read_table(
                        entry['parquet'],
                        columns=[request.column],
                        use_threads=False
                    )
                    total += pc.sum(table[request.column]).as_py()

            except Exception as e:
                errors.append(f"Error processing {file_id}: {str(e)}")

        if errors:
            return table_pb2.ColSumResp(
                total=0,
                error="; ".join(errors)
            )
        return table_pb2.ColSumResp(
            total=total,
            error=""
        )

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[("grpc.so_reuseport", 0)]
    )
    table_pb2_grpc.add_TableServicer_to_server(TableServicer(), server)
    server.add_insecure_port("[::]:5440")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()