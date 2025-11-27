import grpc
import json
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword
from concurrent import futures
from protos import query_pb2, query_pb2_grpc
from datetime import datetime

# --- METADATA Catalog---
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
    },
    'sales_audit_log': {
        'partition_key': 'sale_id',
        'nodes': { 'shard1': 'worker5:50051', 'shard2': 'worker6:50051' }
    }
}

# --- Helper Functions ---
def extract_tables(parsed):
    tables = []
    
    # --- Method 1: SELECT statements (look for FROM) ---
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    tables.append(identifier.get_real_name())
            elif isinstance(item, Identifier):
                tables.append(item.get_real_name())
            elif item.ttype is Keyword and item.value.upper() in ['WHERE', 'GROUP', 'ORDER', 'LIMIT', 'JOIN', 'SET']:
                break
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
            
    # --- Method 2: INSERT statements (look for INTO) ---
    if not tables and parsed.get_type() == 'INSERT':
        into_index = -1
        for i, token in enumerate(parsed.tokens):
            if token.ttype is Keyword and token.value.upper() == 'INTO':
                into_index = i
                break
        
        if into_index != -1:
            for j in range(into_index + 1, len(parsed.tokens)):
                token = parsed.tokens[j]
                if token.is_whitespace: continue
                
                if hasattr(token, 'get_real_name'):
                    name = token.get_real_name()
                    if name: tables.append(name)
                else:
                    tables.append(token.value)
                break 

    # --- Method 3: UPDATE statements (look after UPDATE keyword) ---
    if not tables and parsed.get_type() == 'UPDATE':
        # Iterate to find the first token that isn't whitespace or the UPDATE keyword
        for token in parsed.tokens:
            if token.is_whitespace: continue
            if token.ttype is Keyword.DML and token.value.upper() == 'UPDATE': continue
            
            # The first thing we find after UPDATE is our table
            if hasattr(token, 'get_real_name'):
                name = token.get_real_name()
                if name: tables.append(name)
            else:
                tables.append(token.value)
            break

    return tables

def send_query_to_worker(address, sql_query):
    try:
        with grpc.insecure_channel(address) as channel:
            stub = query_pb2_grpc.QueryServiceStub(channel)
            print(f"Executing on {address}: \"{sql_query}\"")
            response = stub.ExecuteSubQuery(query_pb2.SubQueryRequest(query_sql=sql_query))
            
            # --- THIS IS THE FIX ---
            # Parse the JSON string returned by the worker
            data = json.loads(response.result_json)
            
            # Ensure the return value is always a list.
            # If the worker returned a single dict (like for INSERT status), wrap it in a list.
            if isinstance(data, list):
                return data
            else:
                return [data]
            # -----------------------

    except Exception as e:
        print(f"WORKER ERROR on {address}: {e}")
        return [{"error": str(e)}]

class MasterServicer(query_pb2_grpc.MasterServiceServicer):
    def ExecuteQuery(self, request, context):
        sql = request.sql
        print(f"\nReceived query from client: {sql}")
        try:
            parsed = sqlparse.parse(sql)[0]
            plan = []
            
            # Detect Query Type
            if parsed.get_type() == 'SELECT':
                if 'JOIN' in sql.upper():
                    plan = self.plan_join_query(parsed)
                elif 'COUNT(' in sql.upper() or 'SUM(' in sql.upper() or 'AVG(' in sql.upper():
                    plan = self.plan_aggregate_query(parsed)
                else:
                    plan = self.plan_simple_query(parsed)
            
            elif parsed.get_type() == 'INSERT':
                plan = self.plan_insert_query(parsed, sql)
            
            else:
                # Default to broadcast for unsupported types (like DELETE/UPDATE without specific logic)
                # Or raise error. For now, let's try simple plan.
                plan = self.plan_simple_query(parsed)
            
            final_result = self.execute_plan(plan)
            return query_pb2.QueryResponse(result_json=json.dumps(final_result, indent=2, default=str))
        except Exception as e:
            print(f"FATAL ERROR in ExecuteQuery: {e}")
            return query_pb2.QueryResponse(result_json="[]", error=True, error_message=str(e))

    # --- Planners ---
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

    def plan_join_query(self, parsed):
        tables = extract_tables(parsed)
        plan = []
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
        plan.append({'type': 'master_hash_join', 'tables': tables})
        return plan

    def plan_insert_query(self, parsed, sql):
        tables = extract_tables(parsed)
        if not tables: raise Exception("Could not identify table for INSERT.")
        table_name = tables[0]
        
        if table_name not in METADATA:
             raise Exception(f"Table '{table_name}' unknown.")

        partition_key = METADATA[table_name]['partition_key']
        
        # Normalize SQL for easier parsing
        # Remove newlines and extra spaces
        normalized_sql = ' '.join(sql.split())
        
        try:
            # 1. Extract Columns
            # Looking for: INSERT INTO table (col1, col2) ...
            # Find the first open parenthesis after the table name
            # Note: This assumes the standard format "INSERT INTO table (cols) VALUES (vals)"
            
            # Find index of table name (case insensitive search might be safer but let's assume consistency)
            # A safer way is to find 'INTO' then the next word is table, then '('
            tokens = normalized_sql.split()
            into_index = -1
            for i, t in enumerate(tokens):
                if t.upper() == 'INTO':
                    into_index = i
                    break
            
            if into_index == -1: raise Exception("Invalid INSERT syntax: missing INTO")
            
            # Find start of columns
            cols_start_idx = normalized_sql.find('(', normalized_sql.find(tokens[into_index+1]))
            cols_end_idx = normalized_sql.find(')', cols_start_idx)
            
            if cols_start_idx == -1 or cols_end_idx == -1:
                 raise Exception("Could not parse column list")
            
            raw_cols = normalized_sql[cols_start_idx+1:cols_end_idx]
            columns = [c.strip() for c in raw_cols.split(',')]
            
            # 2. Extract Values
            # Looking for: ... VALUES (val1, val2)
            values_keyword_idx = normalized_sql.upper().find('VALUES')
            if values_keyword_idx == -1: raise Exception("VALUES clause not found")
            
            vals_start_idx = normalized_sql.find('(', values_keyword_idx)
            vals_end_idx = normalized_sql.rfind(')') # Use rfind to get the last closing parenthesis
            
            if vals_start_idx == -1 or vals_end_idx == -1:
                 raise Exception("Could not parse values list")
            
            raw_vals = normalized_sql[vals_start_idx+1:vals_end_idx]
            
            # Intelligent splitting of values (handling commas inside quotes is hard with simple split)
            # For this demo, we will assume values don't contain commas. 
            # Ideally, use a proper tokenizer.
            values = [v.strip() for v in raw_vals.split(',')]
            
            # Clean up quotes from strings
            clean_values = []
            for v in values:
                if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
                    clean_values.append(v[1:-1])
                else:
                    clean_values.append(v) # Keep numbers/dates as strings for logic

            # 3. Map Columns to Values
            if len(columns) != len(clean_values):
                 raise Exception(f"Column count ({len(columns)}) does not match value count ({len(clean_values)}).")
            
            col_val_map = dict(zip(columns, clean_values))
            
            # 4. Determine Target Node
            if partition_key not in col_val_map:
                 raise Exception(f"Partition key '{partition_key}' must be provided in INSERT.")
            
            partition_value = col_val_map[partition_key]
            target_node = None
            nodes_map = METADATA[table_name]['nodes']
            
            if table_name == 'sales':
                # Date-based routing
                try:
                    sale_date = datetime.strptime(str(partition_value), '%Y-%m-%d')
                    # Here is the "Sorting" Logic:
                    if sale_date.month <= 6:
                        target_node = nodes_map['H1'] # Routes to Node 5 (Jan-Jun)
                    else:
                        target_node = nodes_map['H2'] # Routes to Node 6 (Jul-Dec)
                except ValueError:
                     raise Exception(f"Invalid date format for partition key: {partition_value}. Use YYYY-MM-DD.")
            else:
                # Region-based routing (Exact Match)
                if partition_value in nodes_map:
                    target_node = nodes_map[partition_value]
            
            if not target_node:
                raise Exception(f"Could not route INSERT for {partition_key}='{partition_value}'. Available nodes: {list(nodes_map.keys())}")
                
            return [{'type': 'direct_insert', 'node': target_node, 'query': sql}]

        except Exception as e:
             raise Exception(f"Error planning INSERT: {e}")

    # --- Execution Coordinator ---
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
                
                elif step_type == 'direct_insert':
                    # Execute on a single node
                    node = step['node']
                    print(f"Routing INSERT to {node}")
                    res = send_query_to_worker(node, step['query'])
                    
                    if isinstance(res, list):
                        final_result.extend(res)
                    else:
                         final_result.append(res)
                    # ---------------------------------

                elif step_type == 'fetch_for_join':
                    table_name = step['table']
                    tasks = {executor.submit(send_query_to_worker, node, step['query']): node for node in step['nodes']}
                    context_data[table_name] = []
                    for future in futures.as_completed(tasks):
                        context_data[table_name].extend(future.result())

                elif step_type == 'master_hash_join':
                    print("Performing hash join on master node...")
                    tables = step['tables']
                    if len(tables) < 2 or tables[0] not in context_data: continue
                    joined_results = context_data[tables[0]]
                    for i in range(1, len(tables)):
                        next_table = tables[i]
                        if next_table not in context_data: continue
                        
                        current_keys = set(joined_results[0].keys()) if joined_results else set()
                        next_keys = set(context_data[next_table][0].keys()) if context_data[next_table] else set()
                        join_key = list(current_keys.intersection(next_keys))
                        if not join_key: continue
                        join_key = join_key[0]

                        hash_map = {row[join_key]: row for row in context_data[next_table]}
                        new_joined = []
                        for row in joined_results:
                            val = row.get(join_key)
                            if val in hash_map:
                                new_joined.append({**row, **hash_map[val]})
                        joined_results = new_joined
                    final_result = joined_results

                elif step_type == 'map_aggregate':
                    tasks = [executor.submit(send_query_to_worker, node, step['query']) for node in step['nodes']]
                    intermediate = []
                    for future in futures.as_completed(tasks):
                        res = future.result()
                        if res and isinstance(res, list) and res[0]:
                             # Extract first value regardless of key name (count, sum, etc)
                             val = next(iter(res[0].values()))
                             intermediate.append(int(val))
                    context_data['aggs'] = intermediate
                
                elif step_type == 'reduce_aggregate':
                    total = sum(context_data.get('aggs', [0]))
                    final_result = [{'final_aggregate': total}]
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