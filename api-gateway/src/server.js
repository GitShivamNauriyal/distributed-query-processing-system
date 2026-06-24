const express = require("express")
const cors = require("cors")
const grpc = require("@grpc/grpc-js")
const protoLoader = require("@grpc/proto-loader")

const PROTO_PATH = __dirname + "/../../query-engine/protos/query.proto"
const MASTER_GRPC_ADDRESS = "localhost:50050"

const app = express()
app.use(cors())
app.use(express.json())
const PORT = 8080

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
})
const queryProto = grpc.loadPackageDefinition(packageDefinition).query

const masterClient = new queryProto.MasterService(
    MASTER_GRPC_ADDRESS,
    grpc.credentials.createInsecure()
)

app.post("/query", (req, res) => {
    const { sql } = req.body
    masterClient.ExecuteQuery({ sql }, (error, response) => {
        if (error) {
            console.error("gRPC Error:", error)
            return res.status(500).json({ error: error.message })
        }

        if (response.error) {
            console.error("Master Node Logic Error:", response.error_message)
            return res.status(400).json({
                error: true,
                error_message: response.error_message,
            })
        }

        try {
            const results = JSON.parse(response.result_json)
            res.json(results)
        } catch (e) {
            console.error("JSON Parse Error:", e) // Log the parse error
            res.status(500).json({ error: "Failed to parse master response" })
        }
    })
})

app.listen(PORT, () => {
    console.log(`API Gateway server running on http://localhost:${PORT}`)
})
