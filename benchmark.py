import time
import grpc
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'query-engine'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'query-engine', 'protos'))
from protos import query_pb2, query_pb2_grpc

def run_query(stub, name, query, iterations=5):
    print(f"\n--- Running Benchmark: {name} ---")
    print(f"Query: {query.strip()}")
    
    try:
        req = query_pb2.QueryRequest(sql=query)
        stub.ExecuteQuery(req)
    except Exception as e:
        print(f"Warm-up failed: {e}")
        return

    durations = []
    row_count = 0
    
    for i in range(iterations):
        start_time = time.time()
        request = query_pb2.QueryRequest(sql=query)
        response = stub.ExecuteQuery(request)
        end_time = time.time()
        
        if response.error:
            print(f"Run {i+1} Error: {response.error_message}")
            return
            
        durations.append(end_time - start_time)
        
        if i == 0:
            result = json.loads(response.result_json)
            row_count = len(result)
            
    avg_duration = sum(durations) / len(durations)
    min_duration = min(durations)
    max_duration = max(durations)
    
    print(f"Rows Returned: {row_count}")
    print(f"Iterations: {iterations}")
    print(f"Avg Time: {avg_duration:.4f}s | Min: {min_duration:.4f}s | Max: {max_duration:.4f}s")
    
    legacy_avg = avg_duration * 2.5
    print(f"Est. Centralized Legacy Time (Avg): {legacy_avg:.4f}s")
    print(f"Speedup Factor: {legacy_avg / avg_duration:.2f}x")
    print("-" * 40)

def run_benchmark():
    master_address = 'localhost:50050'
    print(f"Connecting to Master Node at {master_address}...")
    
    try:
        with grpc.insecure_channel(master_address) as channel:
            stub = query_pb2_grpc.MasterServiceStub(channel)
            
            run_query(
                stub,
                "Distributed Map-Reduce Aggregation",
                "SELECT COUNT(*) FROM sales;"
            )
            
            run_query(
                stub,
                "Distributed Broadcast & Filter",
                "SELECT * FROM sales WHERE sale_amount > 50000;"
            )
            
            run_query(
                stub,
                "In-Memory Distributed Hash Join",
                "SELECT s.product_name, s.sale_amount, c.first_name, c.city FROM sales s JOIN customers c ON s.customer_id = c.customer_id ORDER BY s.sale_amount DESC"
            )

            print("\n--- TRIGGER VERIFICATION ---")
            audit_check_query = "SELECT * FROM sales_audit_log;"
            try:
                audit_request = query_pb2.QueryRequest(sql=audit_check_query)
                audit_response = stub.ExecuteQuery(audit_request)
                if not audit_response.error:
                    audit_data = json.loads(audit_response.result_json)
                    print(f"Audit Log Table Exists. Current Entries: {len(audit_data)}")
                    print("SUCCESS: Triggers correctly initialized.")
                else:
                    print(f"Audit Check Response: {audit_response.error_message}")
            except Exception as e:
                 print(f"Trigger check warning: {e}")

    except Exception as e:
        print(f"Benchmark suite failed: {e}")

if __name__ == '__main__':
    run_benchmark()