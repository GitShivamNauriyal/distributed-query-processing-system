const express = require("express")
const cors = require("cors")
const grpc = require("@grpc/grpc-js")
const protoLoader = require("@grpc/proto-loader")

// --- Configuration ---
const PROTO_PATH = __dirname + "/../../query-engine/protos/query.proto"
const MASTER_GRPC_ADDRESS = "localhost:50050"

// --- Express App Setup ---
const app = express()
app.use(cors())
app.use(express.json())
const PORT = 8080

// --- gRPC Client Setup ---
// Load the protobuf file
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
})
const queryProto = grpc.loadPackageDefinition(packageDefinition).query

// Create the gRPC client that will connect to our Python master node
const masterClient = new queryProto.MasterService(
    MASTER_GRPC_ADDRESS,
    grpc.credentials.createInsecure()
)

// --- API Endpoint ---
// This is the endpoint our React app will call
app.post("/query", (req, res) => {
    const { sql } = req.body
    if (!sql) {
        return res.status(400).json({ error: "SQL query is required." })
    }

    console.log(`Received query from frontend: ${sql}`)

    // Make the gRPC call to the master node
    masterClient.ExecuteQuery({ sql: sql }, (error, response) => {
        if (error) {
            console.error("gRPC Error:", error.details)
            return res.status(500).json({ error: error.details })
        }

        console.log("Received response from master node.")
        // The response from the master contains a JSON string, so we parse it
        const resultData = JSON.parse(response.result_json)
        res.json(resultData)
    })
})

// --- Start Server ---
app.listen(PORT, () => {
    console.log(`API Gateway server running on http://localhost:${PORT}`)
})
