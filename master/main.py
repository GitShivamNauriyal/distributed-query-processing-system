import grpc
import query_pb2
import query_pb2_grpc
import time

def run_query():
    """
    Acts as a gRPC client to send a query to a worker node.
    """
    print("Master node is starting...")
    # This is a basic example. A real implementation would have a discovery
    # mechanism or get worker addresses from a config file.
    worker_address = 'worker1:50051'
    
    # Establish a connection (channel) to the worker's gRPC server.
    # 'insecure_channel' means it doesn't use encryption (fine for this project).
    with grpc.insecure_channel(worker_address) as channel:
        # Create a client "stub" which has the methods defined in our .proto file.
        stub = query_pb2_grpc.QueryServiceStub(channel)
        
        print(f"Sending query to {worker_address}...")
        
        # This is a hardcoded sub-query. The query optimizer will generate this dynamically.
        sql_to_execute = "SELECT * FROM sales;"
        
        try:
            # Call the remote procedure on the worker.
            response = stub.ExecuteSubQuery(
                query_pb2.SubQueryRequest(query_sql=sql_to_execute)
            )
            print("Received response from worker:")
            print(response.result_json)
        except grpc.RpcError as e:
            print(f"Could not connect to the worker: {e}")

if __name__ == '__main__':
    # Give the workers a moment to start up before the master tries to connect.
    time.sleep(5)
    run_query()