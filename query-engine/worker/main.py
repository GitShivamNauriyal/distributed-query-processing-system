import grpc
import os
import psycopg2
import json
from concurrent import futures

# Import the generated gRPC classes
from protos import query_pb2, query_pb2_grpc

class QueryServicer(query_pb2_grpc.QueryServiceServicer):
    """
    This class implements the gRPC service methods defined in query.proto.
    """
    def __init__(self):
        # Get the database hostname from the environment variable set in docker-compose.
        db_host = os.getenv('DATABASE_HOST', 'localhost')
        
        # Establish the database connection when the servicer is initialized.
        self.db_conn = psycopg2.connect(
            host=db_host,
            database="distributed_db",
            user="user",
            password="password"
        )
        print(f"Worker connected to database at {db_host}")

    def ExecuteSubQuery(self, request, context):
        """
        This method is called by the master. It executes the received SQL query.
        """
        query = request.query_sql
        print(f"Received query: {query}")
        
        results = []
        try:
            # Create a cursor to perform database operations.
            cursor = self.db_conn.cursor()
            cursor.execute(query)
            
            # Fetch column names from the cursor description.
            colnames = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and format them as a list of dictionaries.
            for row in cursor.fetchall():
                results.append(dict(zip(colnames, row)))
            
            cursor.close()
        except Exception as e:
            print(f"An error occurred: {e}")
            # In case of an error, return an empty result with an error message.
            return query_pb2.PartialResult(result_json=json.dumps({"error": str(e)}))

        # Convert the Python list of dictionaries to a JSON string.
        result_json = json.dumps(results, indent=2, default=str)
        
        # Return the result in the format defined by our .proto file.
        return query_pb2.PartialResult(result_json=result_json)

def serve():
    """
    Starts the gRPC server and listens for incoming requests.
    """
    # Create a gRPC server with a thread pool of 10 workers.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Register our servicer class with the server.
    query_pb2_grpc.add_QueryServiceServicer_to_server(QueryServicer(), server)
    
    # The server will listen on all available network interfaces inside the container.
    server.add_insecure_port('[::]:50051')
    
    print("Worker node starting on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()