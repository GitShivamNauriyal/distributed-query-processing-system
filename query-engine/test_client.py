import grpc
import json

# Import the gRPC classes generated from your .proto file
from protos import query_pb2, query_pb2_grpc

def run_tests():
    """
    Connects to the master node's gRPC server and sends a series of test queries.
    """
    # The master container's port 50050 is mapped to port 50050 on your local machine.
    master_address = 'localhost:50050'
    
    # These are the queries we want to test.
    test_queries = [
        {
            "description": "--- 1. Simple Broadcast Query (gets data from both workers) ---",
            "sql": "SELECT * FROM sales;"
        },
        {
            "description": "\n--- 2. Aggregate Query (counts rows on both workers, sums on master) ---",
            "sql": "SELECT COUNT(*) FROM sales;"
        },
        {
            "description": "\n--- 3. Distributed JOIN (fetches employees from worker1, joins with sales on all workers) ---",
            "sql": "SELECT sales.product, sales.region, employees.name FROM sales JOIN employees ON sales.employee_id = employees.id;"
        }
    ]

    # Establish a connection (channel) to the master's gRPC server.
    with grpc.insecure_channel(master_address) as channel:
        # Create a client "stub" for the MasterService.
        stub = query_pb2_grpc.MasterServiceStub(channel)
        
        print(f"Connecting to Master Node at {master_address}")
        
        # Loop through our list of tests and execute each one.
        for test in test_queries:
            print(test["description"])
            try:
                # Create the request message with the SQL query.
                request = query_pb2.QueryRequest(sql=test["sql"])
                
                # Make the remote procedure call to the master.
                response = stub.ExecuteQuery(request)
                
                if response.error:
                    print(f"ERROR from master: {response.error_message}")
                else:
                    # The result is a JSON string, so we parse and print it nicely.
                    result_data = json.loads(response.result_json)
                    print(json.dumps(result_data, indent=2))
            
            except grpc.RpcError as e:
                print(f"An RPC error occurred: {e.code()} - {e.details()}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    run_tests()
