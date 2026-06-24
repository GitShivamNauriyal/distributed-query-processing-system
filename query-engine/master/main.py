import grpc
import json
import sqlglot
import sqlglot.expressions as exp
from concurrent import futures
from protos import query_pb2, query_pb2_grpc
from datetime import datetime

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

def extract_tables(parsed):
    return [table.name for table in parsed.find_all(exp.Table)]

def send_query_to_worker(address, sql_query, params_json=None):
    try:
        with grpc.insecure_channel(address) as channel:
            stub = query_pb2_grpc.QueryServiceStub(channel)
            print(f"Executing on {address}: \"{sql_query}\" with params {params_json}")
            metadata = (('authorization', 'super-secret-token'),)
            response = stub.ExecuteSubQuery(
                query_pb2.SubQueryRequest(query_sql=sql_query, params_json=params_json),
                metadata=metadata
            )
            
            data = json.loads(response.result_json)
            
            if isinstance(data, list):
                return data
            else:
                return [data]
                
    except grpc.RpcError as e:
        print(f"WORKER ERROR on {address} (gRPC RpcError): {e}")
        return [{"error": f"503 Service Unavailable: Data Node Partition Offline ({address})"}]
    except Exception as e:
        print(f"WORKER ERROR on {address}: {e}")
        return [{"error": str(e)}]

class MasterServicer(query_pb2_grpc.MasterServiceServicer):
    def ExecuteQuery(self, request, context):
        sql = request.sql
        print(f"\nReceived query from client: {sql}")
        try:
            parsed = sqlglot.parse_one(sql)
            plan = []
            
            if isinstance(parsed, exp.Select):
                is_join = any(parsed.find_all(exp.Join))
                is_agg = any(parsed.find_all(exp.AggFunc))
                
                if is_join:
                    plan = self.plan_join_query(parsed)
                elif is_agg:
                    plan = self.plan_aggregate_query(parsed)
                else:
                    plan = self.plan_simple_query(parsed)
            
            elif isinstance(parsed, exp.Insert):
                plan = self.plan_insert_query(parsed, sql)
            
            else:
                plan = self.plan_simple_query(parsed)
            
            final_result = self.execute_plan(plan)
            return query_pb2.QueryResponse(result_json=json.dumps(final_result, indent=2, default=str))
        except sqlglot.errors.ParseError as e:
            return query_pb2.QueryResponse(result_json="[]", error=True, error_message=f"SQL Parsing Error: {e}")
        except Exception as e:
            print(f"FATAL ERROR in ExecuteQuery: {e}")
            return query_pb2.QueryResponse(result_json="[]", error=True, error_message=str(e))

    def plan_simple_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables: raise Exception("No table found.")
        table_meta = METADATA.get(tables[0])
        if not table_meta: raise Exception(f"Table '{tables[0]}' not in METADATA.")
        target_nodes = list(table_meta['nodes'].values())
        return [{'type': 'broadcast', 'nodes': target_nodes, 'query': parsed.sql(), 'params': None}]

    def plan_aggregate_query(self, parsed):
        tables = extract_tables(parsed)
        if not tables: raise Exception("No table found.")
        table_meta = METADATA.get(tables[0])
        target_nodes = list(table_meta['nodes'].values())
        return [
            {'type': 'map_aggregate', 'nodes': target_nodes, 'query': parsed.sql(), 'params': None},
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
                'query': f"SELECT * FROM {table};",
                'params': None
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
        
        try:
            table_exp = parsed.args.get('this')
            if not table_exp or not table_exp.this:
                raise Exception("Table not found in AST.")
            
            columns = []
            if 'expressions' in table_exp.args:
                columns = [col.name for col in table_exp.expressions]
            else:
                raise Exception("Column list is required for routing.")
                
            values_clause = parsed.expression
            if not values_clause or not isinstance(values_clause, exp.Values):
                raise Exception("VALUES clause not found.")
            
            tup = values_clause.expressions[0]
            clean_values = []
            for val_exp in tup.expressions:
                if isinstance(val_exp, exp.Literal):
                    clean_values.append(val_exp.this)
                else:
                    clean_values.append(str(val_exp))
                    
            if len(columns) != len(clean_values):
                 raise Exception(f"Column count ({len(columns)}) does not match value count ({len(clean_values)}).")
            
            col_val_map = dict(zip(columns, clean_values))
            
            if partition_key not in col_val_map:
                 raise Exception(f"Partition key '{partition_key}' must be provided in INSERT.")
            
            partition_value = col_val_map[partition_key]
            
            target_node = None
            nodes_map = METADATA[table_name]['nodes']
            
            if table_name == 'sales':
                try:
                    sale_date = datetime.strptime(str(partition_value), '%Y-%m-%d')
                    if sale_date.month <= 6:
                        target_node = nodes_map['H1']
                    else:
                        target_node = nodes_map['H2']
                except ValueError:
                     raise Exception(f"Invalid date format for partition key: {partition_value}. Use YYYY-MM-DD.")
            else:
                if partition_value in nodes_map:
                    target_node = nodes_map[partition_value]
            
            if not target_node:
                raise Exception(f"Could not route INSERT for {partition_key}='{partition_value}'. Available nodes: {list(nodes_map.keys())}")
                
            placeholders = ', '.join(['%s'] * len(clean_values))
            col_str = ', '.join(columns)
            param_query = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"
            
            return [{'type': 'direct_insert', 'node': target_node, 'query': param_query, 'params': json.dumps(clean_values)}]

        except Exception as e:
             raise Exception(f"Error planning INSERT: {e}")

    def execute_plan(self, plan):
        context_data = {}
        final_result = []

        with futures.ThreadPoolExecutor() as executor:
            for step in plan:
                step_type = step['type']
                
                if step_type == 'broadcast':
                    tasks = {executor.submit(send_query_to_worker, node, step['query'], step.get('params')): node for node in step['nodes']}
                    for future in futures.as_completed(tasks):
                        final_result.extend(future.result())
                
                elif step_type == 'direct_insert':
                    node = step['node']
                    print(f"Routing INSERT to {node}")
                    res = send_query_to_worker(node, step['query'], step.get('params'))
                    
                    if isinstance(res, list):
                        final_result.extend(res)
                    else:
                         final_result.append(res)

                elif step_type == 'fetch_for_join':
                    table_name = step['table']
                    tasks = {executor.submit(send_query_to_worker, node, step['query'], step.get('params')): node for node in step['nodes']}
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
                    tasks = [executor.submit(send_query_to_worker, node, step['query'], step.get('params')) for node in step['nodes']]
                    intermediate = []
                    for future in futures.as_completed(tasks):
                        res = future.result()
                        if res and isinstance(res, list) and res[0] and 'error' not in res[0]:
                             val = next(iter(res[0].values()))
                             if val is not None:
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