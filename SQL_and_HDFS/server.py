import time
import grpc
import lender_pb2
import lender_pb2_grpc
from concurrent import futures
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import requests
import json
import logging
from retry import retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LenderServicer(lender_pb2_grpc.LenderServicer):
    
    def __init__(self):
        # Initialize HDFS connection
        self.hdfs = fs.HadoopFileSystem("boss", 9000)
        
    def DbToHdfs(self, request, context):
        logger.info("DbToHdfs request received")
        try:
            @retry(exceptions=(pymysql.OperationalError, pymysql.Error), tries=5, delay=5, backoff=2)
            def connect_to_mysql():
                # Connect to MySQL database
                engine = create_engine('mysql+pymysql://root:abc@mysql/CS544', pool_size=10, max_overflow=20)
                return engine
            
            engine = connect_to_mysql()
            
            # SQL query to join loans and loan_types tables and filter rows
            query = """
            SELECT loans.*, loan_types.loan_type_name
            FROM loans
            INNER JOIN loan_types ON loans.loan_type_id = loan_types.id
            WHERE loans.loan_amount > 30000 AND loans.loan_amount < 800000;
            """
            
            # Load data into a Pandas DataFrame
            df = pd.read_sql(query, engine)
            row_count = len(df)
            logger.info(f"Loaded {row_count} rows from database")
            
            if row_count != 426716:
                logger.warning(f"Expected 426716 rows but got {row_count}")
            
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Create /partitions directory in HDFS if it doesn't exist
            try:
                self.hdfs.create_dir("/partitions")
                logger.info("Created /partitions directory in HDFS")
            except Exception as e:
                logger.warning(f"Directory /partitions might already exist: {str(e)}")
            
            # Write table to HDFS as a Parquet file
            parquet_path = "/hdma-wi-2021.parquet"
            logger.info(f"Writing table to HDFS at {parquet_path}")
            pq.write_table(
                table, 
                parquet_path,
                filesystem=self.hdfs,
                compression="snappy",
                row_group_size=1048576  # 1 MB block size
            )
            logger.info(f"Successfully wrote table to HDFS at {parquet_path}")

            # Set replication factor to 2
            def set_hdfs_replication(path, replication):
                url = f"http://boss:9870/webhdfs/v1{path}?op=SETREPLICATION&replication={replication}&user.name=root"
                response = requests.put(url)
                if response.status_code != 200:
                    raise Exception(f"Failed to set replication: {response.text}")
            
            set_hdfs_replication(parquet_path, 2)
            logger.info(f"Set replication factor to 2 for {parquet_path}")
            
            # Log file size
            file_info = self.hdfs.get_file_info(parquet_path)
            logger.info(f"File size: {file_info.size} bytes")
            
            logger.info("Data successfully uploaded to HDFS")
            return lender_pb2.StatusString(status="success")
            
        except Exception as e:
            logger.error(f"Error in DbToHdfs: {str(e)}", exc_info=True)
            return lender_pb2.StatusString(status=f"error: {str(e)}")

    
    def BlockLocations(self, request, context):
        logger.info(f"BlockLocations request received for path: {request.path}")
        try:
            path = request.path
            if not path.startswith('/'):
                path = '/' + path
            
            # WebHDFS API call to get block locations
            url = f"http://boss:9870/webhdfs/v1{path}?op=GETFILEBLOCKLOCATIONS"
            logger.info(f"Sending GET request to {url}")
            
            response = requests.get(url)
            if response.status_code != 200:
                logger.error(f"Failed request: {response.status_code} - {response.text}")
                return lender_pb2.BlockLocationsResp(error=f"WebHDFS API request failed: {response.text}")
            
            data = response.json()
            logger.info(f"WebHDFS API raw response: {json.dumps(data, indent=2)}")
            
            if 'BlockLocations' not in data:
                return lender_pb2.BlockLocationsResp(error="Invalid response structure: missing 'BlockLocations'")
            
            # Count blocks per DataNode
            blocks = data['BlockLocations'].get('BlockLocation', [])
            block_counts = {}
            
            for block in blocks:
                hosts = block.get('hosts', [])
                for host in hosts:
                    block_counts[host] = block_counts.get(host, 0) + 1
            
            logger.info(f"Block counts: {block_counts}")
            return lender_pb2.BlockLocationsResp(block_entries=block_counts)

        except Exception as e:
            logger.error(f"Exception in BlockLocations: {e}")
            return lender_pb2.BlockLocationsResp(error=str(e))
            

    def CalcAvgLoan(self, request, context):
        logger.info(f"CalcAvgLoan request received for county code: {request.county_code}")
        try:
            county_code = request.county_code
            county_file_path = f"/partitions/{county_code}.parquet"
            source = ""
            
            try:
                # Attempt to read the county-specific file
                table = pq.read_table(county_file_path, filesystem=self.hdfs)
                source = "reuse"
                logger.info(f"Using existing county file: {county_file_path}")
                
            except (FileNotFoundError, OSError, pa.ArrowInvalid) as e:
                logger.info(f"County file not found or corrupted: {str(e)}")
                
                if isinstance(e, FileNotFoundError):
                    source = "create"
                    logger.info(f"Creating new county file: {county_file_path}")
                else:
                    source = "recreate"  # Use 'recreate' for corrupted files
                    logger.info(f"Recreating corrupted county file: {county_file_path}")
                
                try:
                    # Read from the main Parquet file and filter by county code
                    big_table = pq.read_table(
                        "/hdma-wi-2021.parquet",  
                        filters=[("county_code", "=", county_code)],
                        filesystem=self.hdfs
                    )
                    
                    # Write filtered data to a new county-specific Parquet file
                    pq.write_table(
                        big_table,
                        county_file_path, 
                        filesystem=self.hdfs,
                        compression="snappy"
                    )
                    
                    # Verify the file was written successfully
                    file_info = self.hdfs.get_file_info(county_file_path)
                    if file_info.size == 0:
                        raise Exception(f"Failed to write file: {county_file_path} has 0 bytes")
                    
                    table = big_table
                    
                except Exception as inner_e:
                    logger.error(f"Error reading main file: {str(inner_e)}")
                    return lender_pb2.CalcAvgLoanResp(error=str(inner_e))
            
            # Calculate average loan amount
            df = table.to_pandas()
            if len(df) == 0:
                return lender_pb2.CalcAvgLoanResp(error=f"No data found for county code: {county_code}")
            
            avg_loan = int(df['loan_amount'].mean())
            return lender_pb2.CalcAvgLoanResp(avg_loan=avg_loan, source=source)
            
        except Exception as e:
            logger.error(f"Error in CalcAvgLoan: {str(e)}")
            return lender_pb2.CalcAvgLoanResp(error=str(e))

def serve():
    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderServicer(), server)
    server.add_insecure_port('[::]:5000')
    server.start()
    logger.info("Server started, listening on port 5000")
    try:
        while True:
            time.sleep(86400)  # Keep the server running
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
