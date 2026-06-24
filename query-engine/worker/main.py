import grpc
import os
import psycopg2
import json
from concurrent import futures

from protos import query_pb2, query_pb2_grpc

class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, key):
        self._valid_metadata = ('authorization', key)

    def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata)
        if metadata.get('authorization') == self._valid_metadata[1]:
            return continuation(handler_call_details)
        else:
            def deny(_, context):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, 'Invalid token')
            return grpc.unary_unary_rpc_method_handler(deny)

class QueryServicer(query_pb2_grpc.QueryServiceServicer):
    """
    This class implements the gRPC service methods defined in query.proto.
    """
    def __init__(self):
        db_host = os.getenv('DATABASE_HOST', 'localhost')
        
        try:
            self.db_conn = psycopg2.connect(
                host=db_host,
                database="distributed_db",
                user="user",
                password="password"
            )
            self.db_conn.autocommit = True
            print(f"Worker connected to database at {db_host}")
        except Exception as e:
            print(f"Failed to connect to database: {e}")

    def ExecuteSubQuery(self, request, context):
        """
        This method is called by the master. It executes the received SQL query.
        """
        query = request.query_sql
        params_json = request.params_json
        print(f"Received query: {query}")
        print(f"Params: {params_json}")
        
        try:
            params = json.loads(params_json) if params_json else ()

            cursor = self.db_conn.cursor()
            cursor.execute(query, params)
            
            if cursor.description:
                colnames = [desc[0] for desc in cursor.description]
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(colnames, row)))
                
                result_json = json.dumps(results, indent=2, default=str)
            else:
                result_json = json.dumps([{"status": "success", "rows_affected": cursor.rowcount}])
            
            cursor.close()
            
            return query_pb2.PartialResult(result_json=result_json)

        except Exception as e:
            print(f"An error occurred: {e}")
            return query_pb2.PartialResult(result_json=json.dumps({"error": str(e)}))

def serve():
    """
    Starts the gRPC server and listens for incoming requests.
    """
    auth_interceptor = AuthInterceptor('super-secret-token')
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        interceptors=(auth_interceptor,)
    )
    
    query_pb2_grpc.add_QueryServiceServicer_to_server(QueryServicer(), server)
    
    server.add_insecure_port('[::]:50051')
    
    print("Worker node starting on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()