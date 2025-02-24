import gzip
import csv
import grpc
from concurrent import futures
import property_lookup_pb2
import property_lookup_pb2_grpc

class PropertyLookupServicer(property_lookup_pb2_grpc.PropertyLookupServicer):
    def __init__(self):
        self.data = self.load_data("addresses.csv.gz")  

    def load_data(self, file_path):
        data = []
        with gzip.open(file_path, "rt") as f:
            reader = csv.reader(f)
            next(reader)  
            for row in reader:
                if len(row) >= 12:  
                    address = row[9]  
                    zipcode = row[11]  
                    data.append((address, zipcode))
        return data

    def LookupByZip(self, request, context):
        target_zip = str(request.zip)
        matching_addresses = [
            addr for addr, zipcode in self.data 
            if zipcode == target_zip
        ]
        matching_addresses.sort()
        return property_lookup_pb2.Response(
            address=matching_addresses[:request.limit]
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
    property_lookup_pb2_grpc.add_PropertyLookupServicer_to_server(PropertyLookupServicer(), server) 
    server.add_insecure_port("0.0.0.0:5000")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()