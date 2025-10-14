import grpc
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword
from concurrent import futures

# Import the generated gRPC classes
from protos import query_pb2, query_pb2_grpc

# --- 1. Global Metadata Catalog ---
METADATA = {
    'sales': {
        'partition_key': 'region',
        'nodes': ['worker1:50051', 'worker2:50051']
    },
    'employees': {
        'partition_key': None,
        'nodes': ['worker1:50051']
    }
}

# --- Helper function to reliably extract table names ---
def extract_tables(parsed):
    tables = []
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    tables.append(identifier.get_real_name())
            elif isinstance(item, Identifier):
                tables.append(item.get_real_name())
            elif item.ttype is Keyword and item.value.upper() in ['WHERE', 'GROUP', 'ORDER', 'LIMIT', 'JOIN']:
                break
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
    return tables

# --- 2. Execution Engine ---
def send_query_to_worker(address, sql_query):
    try:
        with grpc.insecure_channel(address) as channel:
            stub = query_pb2_grpc.QueryServiceStub(channel)
            print(f"Executing on {address}: \"{sql_query}\"")
            response = stub.ExecuteSubQuery(
                query_pb2.SubQueryRequest(query_sql=sql_query)
            )
            return json.loads(response.result_json)
    except Exception as e:
        print(f"WORKER ERROR on {address}: {e}")
        return [{"error": str(e)}]

# --- 3. The Master Service Implementation ---
class MasterServicer(query_pb2_grpc.MasterServiceServicer):
    def ExecuteQuery(self, request, context):
        sql = request.sql
        print(f"\nReceived query from client: {sql}")
        
        try:
            parsed = sqlparse.parse(sql)[0]
            
            if parsed.get_type() == 'SELECT' and 'JOIN' in sql.upper():
                plan = self.plan_join_query(parsed)
            elif 'COUNT(' in sql.upper() or 'SUM(' in sql.upper():
                plan = self.plan_aggregate_query(parsed)
            else:
                plan = self.plan_simple_query(parsed)

            final_result = self.execute_plan(plan)
            
            return query_pb2.QueryResponse(
                result_json=json.dumps(final_result, indent=2)
            )
        except Exception as e:
            print(f"FATAL ERROR: {e}")
            return query_pb2.QueryResponse(
                result_json="[]", error=True, error_message=str(e)
            )

    def plan_simple_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables:
            raise Exception("Could not find table in query.")
        
        target_nodes = METADATA.get(tables[0], {}).get('nodes', [])
        if not target_nodes:
            raise Exception(f"Table '{tables[0]}' not found in metadata.")
        
        return [{'type': 'broadcast', 'nodes': target_nodes, 'query': str(parsed)}] # CHANGED HERE

    def plan_aggregate_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables:
            raise Exception("Could not find table in query.")
            
        target_nodes = METADATA.get(tables[0], {}).get('nodes', [])
        return [
            {'type': 'map_aggregate', 'nodes': target_nodes, 'query': str(parsed)}, # CHANGED HERE
            {'type': 'reduce_aggregate'}
        ]

    def plan_join_query(self, parsed):
        tables = extract_tables(parsed)
        smaller_table, larger_table = 'employees', 'sales'
        
        fetch_plan = {
            'type': 'fetch',
            'node': METADATA[smaller_table]['nodes'][0],
            'query': f"SELECT * FROM {smaller_table};"
        }
        
        join_plan = {
            'type': 'broadcast_join',
            'nodes': METADATA[larger_table]['nodes'],
            'join_sql': str(parsed), # CHANGED HERE
            'broadcast_data_key': smaller_table
        }
        return [fetch_plan, join_plan]

    def execute_plan(self, plan):
        context_data = {}
        final_result = []

        with futures.ThreadPoolExecutor() as executor:
            for step in plan:
                if step['type'] == 'broadcast':
                    tasks = [executor.submit(send_query_to_worker, node, step['query']) for node in step['nodes']]
                    for future in futures.as_completed(tasks):
                        final_result.extend(future.result())

                elif step['type'] == 'fetch':
                    context_data[step['query'].split(' ')[-1].strip(';')] = send_query_to_worker(step['node'], step['query'])
                
                elif step['type'] == 'broadcast_join':
                    print("DEMO JOIN: A real system would now send the 'employees' data to workers for a local join.")
                    final_result.append({"sales_data_from_worker1": send_query_to_worker(METADATA['sales']['nodes'][0], "SELECT * FROM sales;")})
                    final_result.append({"sales_data_from_worker2": send_query_to_worker(METADATA['sales']['nodes'][1], "SELECT * FROM sales;")})
                    final_result.append({"employees_data_that_would_be_broadcasted": context_data['employees']})

                elif step['type'] == 'map_aggregate':
                    tasks = [executor.submit(send_query_to_worker, node, step['query']) for node in step['nodes']]
                    intermediate_values = []
                    for future in futures.as_completed(tasks):
                        res = future.result()
                        if res and isinstance(res, list) and res[0]:
                            numeric_value = next(iter(res[0].values()))
                            intermediate_values.append(int(numeric_value))
                    context_data['intermediate_aggregates'] = intermediate_values

                elif step['type'] == 'reduce_aggregate':
                    final_sum = sum(context_data.get('intermediate_aggregates', [0]))
                    final_result = [{'final_aggregate': final_sum}]

        return final_result

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    query_pb2_grpc.add_MasterServiceServicer_to_server(MasterServicer(), server)
    server.add_insecure_port('[::]:50050')
    print("Master node server started on port 50050. Listening for client...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()