import grpc
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword
from concurrent import futures
from protos import query_pb2, query_pb2_grpc

# --- METADATA Catalog (Unchanged) ---
METADATA = {
    'customers': {
        'partition_key': 'region',
        'nodes': { 'North': 'worker1:50051', 'South': 'worker3:50051' }
    },
    'employees': {
        'partition_key': 'region',
        'nodes': { 'North': 'worker2:50051', 'South': 'worker4:50051' }
    },
    'sales': {
        'partition_key': 'sale_date',
        'nodes': { 'H1': 'worker5:50051', 'H2': 'worker6:50051' }
    }
}

# --- Helper Functions (Unchanged) ---
def extract_tables(parsed):
    tables = []
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers(): tables.append(identifier.get_real_name())
            elif isinstance(item, Identifier):
                tables.append(item.get_real_name())
            elif item.ttype is Keyword and item.value.upper() in ['WHERE', 'GROUP', 'ORDER', 'LIMIT', 'JOIN']:
                break
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
    return tables

def send_query_to_worker(address, sql_query):
    try:
        with grpc.insecure_channel(address) as channel:
            stub = query_pb2_grpc.QueryServiceStub(channel)
            print(f"Executing on {address}: \"{sql_query}\"")
            response = stub.ExecuteSubQuery(query_pb2.SubQueryRequest(query_sql=sql_query))
            return json.loads(response.result_json)
    except Exception as e:
        print(f"WORKER ERROR on {address}: {e}")
        return [{"error": str(e)}]

# --- Master Service (Upgraded Logic) ---
class MasterServicer(query_pb2_grpc.MasterServiceServicer):
    def ExecuteQuery(self, request, context):
        sql = request.sql
        print(f"\nReceived query from client: {sql}")
        try:
            parsed = sqlparse.parse(sql)[0]
            plan = []
            if parsed.get_type() == 'SELECT':
                if 'JOIN' in sql.upper():
                    plan = self.plan_join_query(parsed)
                elif 'COUNT(' in sql.upper() or 'SUM(' in sql.upper() or 'AVG(' in sql.upper():
                    plan = self.plan_aggregate_query(parsed)
                else:
                    plan = self.plan_simple_query(parsed)
            
            final_result = self.execute_plan(plan)
            return query_pb2.QueryResponse(result_json=json.dumps(final_result, indent=2, default=str))
        except Exception as e:
            print(f"FATAL ERROR in ExecuteQuery: {e}")
            return query_pb2.QueryResponse(result_json="[]", error=True, error_message=str(e))

    def plan_simple_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables: raise Exception("No table found.")
        table_meta = METADATA.get(tables[0])
        if not table_meta: raise Exception(f"Table '{tables[0]}' not in METADATA.")
        target_nodes = list(table_meta['nodes'].values())
        return [{'type': 'broadcast', 'nodes': target_nodes, 'query': str(parsed)}]

    def plan_aggregate_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables: raise Exception("No table found.")
        table_meta = METADATA.get(tables[0])
        target_nodes = list(table_meta['nodes'].values())
        return [
            {'type': 'map_aggregate', 'nodes': target_nodes, 'query': str(parsed)},
            {'type': 'reduce_aggregate'}
        ]

    # === NEW, MORE ROBUST JOIN PLANNER ===
    def plan_join_query(self, parsed):
        tables = extract_tables(parsed)
        plan = []

        # Step 1: Create a plan to fetch all data from every table involved in the join.
        for table in tables:
            table_meta = METADATA.get(table)
            if not table_meta: raise Exception(f"Table '{table}' not in METADATA.")
            nodes = list(table_meta['nodes'].values())
            plan.append({
                'type': 'fetch_for_join',
                'table': table,
                'nodes': nodes,
                'query': f"SELECT * FROM {table};"
            })

        # Step 2: Add a final step to perform the joins on the master node.
        # This is a much more robust approach than trying to parse ON clauses.
        plan.append({'type': 'master_hash_join', 'tables': tables})
        return plan

    def execute_plan(self, plan):
        context_data = {}
        final_result = []

        with futures.ThreadPoolExecutor() as executor:
            for step in plan:
                step_type = step['type']
                
                if step_type == 'broadcast':
                    tasks = {executor.submit(send_query_to_worker, node, step['query']): node for node in step['nodes']}
                    for future in futures.as_completed(tasks):
                        final_result.extend(future.result())

                elif step_type == 'fetch_for_join':
                    table_name = step['table']
                    tasks = {executor.submit(send_query_to_worker, node, step['query']): node for node in step['nodes']}
                    context_data[table_name] = []
                    for future in futures.as_completed(tasks):
                        context_data[table_name].extend(future.result())
                
                # === NEW, MORE ROBUST JOIN EXECUTION LOGIC ===
                elif step_type == 'master_hash_join':
                    print("Performing hash join on master node...")
                    tables = step['tables']
                    if len(tables) < 2 or tables[0] not in context_data:
                        print(f"Not enough data to perform join. Needed: {tables}. Have: {list(context_data.keys())}")
                        continue

                    # Start with the first table's data
                    joined_results = context_data[tables[0]]

                    # Sequentially join with the rest of the tables
                    for i in range(1, len(tables)):
                        next_table_name = tables[i]
                        if next_table_name not in context_data: continue

                        # Find the common column (join key) between the current result and the next table.
                        # This is a simple but effective way to auto-detect the join key.
                        current_keys = set(joined_results[0].keys()) if joined_results else set()
                        next_keys = set(context_data[next_table_name][0].keys()) if context_data[next_table_name] else set()
                        join_key = list(current_keys.intersection(next_keys))
                        
                        if not join_key: 
                            print(f"Could not find common join key between intermediate result and table '{next_table_name}'")
                            continue
                        
                        join_key = join_key[0] # Use the first common key found
                        print(f"Joining with '{next_table_name}' on key '{join_key}'...")

                        # Build a hash map on the next table for efficient lookups
                        hash_map = {row[join_key]: row for row in context_data[next_table_name]}
                        
                        new_joined_results = []
                        for row in joined_results:
                            key_value = row.get(join_key)
                            if key_value in hash_map:
                                # Merge the row from the current result with the matching row from the hash map
                                new_row = {**row, **hash_map[key_value]}
                                new_joined_results.append(new_row)
                        
                        joined_results = new_joined_results # The result of this join becomes the input for the next

                    final_result = joined_results

                elif step_type == 'map_aggregate':
                    # ... (this part is unchanged)
                    tasks = [executor.submit(send_query_to_worker, node, step['query']) for node in step['nodes']]
                    intermediate_values = []
                    for future in futures.as_completed(tasks):
                        res = future.result()
                        if res and isinstance(res, list) and res[0]:
                            numeric_value = next(iter(res[0].values()))
                            intermediate_values.append(int(numeric_value))
                    context_data['intermediate_aggregates'] = intermediate_values
                
                elif step_type == 'reduce_aggregate':
                    # ... (this part is unchanged)
                    total = sum(context_data.get('intermediate_aggregates', [0]))
                    final_result = [{'final_aggregate': total}]
        return final_result

def serve():
    # ... (this part is unchanged)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    query_pb2_grpc.add_MasterServiceServicer_to_server(MasterServicer(), server)
    server.add_insecure_port('[::]:50050')
    print("Master node server started on port 50050. Listening for client...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

