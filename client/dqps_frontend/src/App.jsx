import { useState } from "react"
import { motion, AnimatePresence } from "framer-motion"

function App() {
    const [sqlQuery, setSqlQuery] = useState("SELECT * FROM sales;")
    const [results, setResults] = useState([])
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState("")
    const [animationKey, setAnimationKey] = useState(0)

    const handleSubmitQuery = async (e) => {
        e.preventDefault()
        setIsLoading(true)
        setError("")
        try {
            const response = await fetch("http://localhost:8080/query", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ sql: sqlQuery }),
            })

            const data = await response.json()

            // Debug Log: See exactly what the backend sent
            console.log("Frontend received:", data)

            if (!response.ok || data.error) {
                throw new Error(
                    data.error_message ||
                        data.error ||
                        "An error occurred on the server."
                )
            }
            setResults(data)
            setAnimationKey((prevKey) => prevKey + 1)
        } catch (err) {
            console.error("Query Error:", err)
            setError(err.message)
            setResults([])
        } finally {
            setIsLoading(false)
        }
    }

    // Animation variants
    const tableVariants = {
        hidden: { opacity: 0 },
        visible: { opacity: 1, transition: { staggerChildren: 0.03 } },
    }

    const rowVariants = {
        hidden: { opacity: 0, y: 15 },
        visible: {
            opacity: 1,
            y: 0,
            transition: { type: "spring", stiffness: 300, damping: 24 },
        },
    }

    const renderContent = () => {
        // 1. Handle No Results
        if (!results || results.length === 0) {
            return (
                <motion.div
                    key="no-results"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="flex items-center justify-center h-full text-gray-500"
                >
                    <p>No results to display.</p>
                </motion.div>
            )
        }

        // 2. Handle Success Message (INSERT/UPDATE/DELETE)
        // We check if the first item has a 'status' property of 'success'
        if (results[0] && results[0].status === "success") {
            console.log("success")
            return (
                <motion.div
                    key="success-message"
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="flex flex-col items-center justify-center h-full text-green-400 space-y-4"
                >
                    <div className="p-4 bg-green-900/30 rounded-full border-2 border-green-500/50">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-12 w-12"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                            strokeWidth={2}
                        >
                            <path
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                d="M5 13l4 4L19 7"
                            />
                        </svg>
                    </div>
                    <div className="text-center">
                        <h3 className="text-xl font-bold">
                            Query Executed Successfully
                        </h3>
                        <p className="text-gray-400">
                            {results[0].rows_affected} row(s) affected.
                        </p>
                    </div>
                </motion.div>
            )
        } else {
            console.log("failure")
        }

        // 3. Handle Standard Data Table (SELECT)
        const headers = Object.keys(results[0])

        return (
            <div className="h-full overflow-auto rounded-lg border border-gray-700 scrollbar-thin scrollbar-thumb-gray-600 scrollbar-track-gray-800">
                <motion.table
                    key={`results-table-${animationKey}`}
                    className="min-w-full text-sm text-left whitespace-nowrap"
                    variants={tableVariants}
                    initial="hidden"
                    animate="visible"
                >
                    <thead className="sticky top-0 bg-gray-800 text-gray-300 uppercase tracking-wider z-10 shadow-lg">
                        <tr>
                            {headers.map((header) => (
                                <th
                                    key={header}
                                    className="p-4 font-semibold bg-gray-800"
                                >
                                    {header.replace(/_/g, " ")}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700 bg-gray-900">
                        {results.map((row, rowIndex) => (
                            <motion.tr
                                key={rowIndex}
                                className="hover:bg-gray-800/50 transition-colors duration-150"
                                variants={rowVariants}
                            >
                                {headers.map((header) => (
                                    <td
                                        key={`${rowIndex}-${header}`}
                                        className="p-4 font-mono text-cyan-300 border-b border-gray-800 last:border-0"
                                    >
                                        {typeof row[header] === "object"
                                            ? JSON.stringify(row[header])
                                            : String(row[header])}
                                    </td>
                                ))}
                            </motion.tr>
                        ))}
                    </tbody>
                </motion.table>
            </div>
        )
    }

    return (
        <div className="bg-gray-950 text-white h-screen w-screen overflow-hidden flex flex-col font-sans selection:bg-cyan-500/30">
            {/* Header */}
            <header className="flex-shrink-0 bg-gray-900/80 backdrop-blur-md border-b border-gray-800 shadow-lg z-50">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex items-center gap-4">
                    <div className="p-2 bg-cyan-900/20 rounded-lg border border-cyan-500/20">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            className="h-6 w-6 text-cyan-400"
                            fill="none"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                            strokeWidth={2}
                        >
                            <path
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
                            />
                        </svg>
                    </div>
                    <div>
                        <h1 className="text-xl font-bold text-white tracking-tight">
                            Distributed{" "}
                            <span className="text-cyan-400">Query Engine</span>
                        </h1>
                        <p className="text-xs text-gray-500 font-medium">
                            6-Node Cluster • Partitioned Data
                        </p>
                    </div>
                </div>
            </header>

            {/* Main Content Area */}
            <main className="flex-grow flex flex-col md:flex-row gap-6 p-4 sm:p-6 lg:p-8 overflow-hidden max-w-7xl mx-auto w-full">
                {/* Left Panel: Query Editor */}
                <div className="flex flex-col md:w-1/3 h-full min-h-[300px]">
                    <form
                        onSubmit={handleSubmitQuery}
                        className="flex flex-col h-full bg-gray-900 rounded-xl p-1 border border-gray-800 shadow-2xl ring-1 ring-white/5"
                    >
                        <div className="p-4 border-b border-gray-800 flex justify-between items-center bg-gray-900/50 rounded-t-xl">
                            <label
                                htmlFor="query-editor"
                                className="text-sm font-semibold text-gray-300 uppercase tracking-wider"
                            >
                                SQL Input
                            </label>
                            <span className="text-xs text-gray-600 font-mono">
                                Ready
                            </span>
                        </div>

                        <div className="flex-grow relative bg-black/20">
                            <textarea
                                id="query-editor"
                                className="absolute inset-0 w-full h-full p-4 bg-transparent text-gray-200 font-mono text-sm focus:outline-none resize-none placeholder-gray-700"
                                value={sqlQuery}
                                onChange={(e) => setSqlQuery(e.target.value)}
                                placeholder="SELECT * FROM sales;"
                                spellCheck="false"
                            />
                        </div>

                        <div className="p-4 border-t border-gray-800 bg-gray-900/50 rounded-b-xl">
                            <button
                                type="submit"
                                disabled={isLoading}
                                className="w-full py-3 px-4 bg-gradient-to-r from-cyan-600 to-blue-600 rounded-lg font-semibold text-white shadow-lg hover:from-cyan-500 hover:to-blue-500 focus:ring-2 focus:ring-cyan-500/50 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200 transform active:scale-[0.98] flex items-center justify-center gap-2"
                            >
                                {isLoading ? (
                                    <>
                                        <svg
                                            className="animate-spin h-5 w-5 text-white"
                                            xmlns="http://www.w3.org/2000/svg"
                                            fill="none"
                                            viewBox="0 0 24 24"
                                        >
                                            <circle
                                                className="opacity-25"
                                                cx="12"
                                                cy="12"
                                                r="10"
                                                stroke="currentColor"
                                                strokeWidth="4"
                                            ></circle>
                                            <path
                                                className="opacity-75"
                                                fill="currentColor"
                                                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                                            ></path>
                                        </svg>
                                        Processing...
                                    </>
                                ) : (
                                    <>
                                        <svg
                                            xmlns="http://www.w3.org/2000/svg"
                                            className="h-5 w-5"
                                            viewBox="0 0 20 20"
                                            fill="currentColor"
                                        >
                                            <path
                                                fillRule="evenodd"
                                                d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z"
                                                clipRule="evenodd"
                                            />
                                        </svg>
                                        Run Query
                                    </>
                                )}
                            </button>
                        </div>
                    </form>
                </div>

                {/* Right Panel: Results */}
                <div className="flex flex-col md:w-2/3 h-full bg-gray-900 rounded-xl border border-gray-800 shadow-2xl overflow-hidden ring-1 ring-white/5">
                    <div className="p-4 border-b border-gray-800 bg-gray-900/50 flex items-center justify-between">
                        <h2 className="text-sm font-semibold text-gray-300 uppercase tracking-wider">
                            Query Results
                        </h2>
                        {/* Show row count only if it's not a success message */}
                        {results.length > 0 && !results[0].status && (
                            <span className="text-xs px-2 py-1 bg-gray-800 rounded-full text-cyan-400 border border-gray-700">
                                {results.length} rows found
                            </span>
                        )}
                    </div>

                    <div className="relative flex-grow min-h-0 bg-black/20 p-1">
                        <AnimatePresence mode="wait">
                            {isLoading ? (
                                <motion.div
                                    key="loading"
                                    initial={{ opacity: 0 }}
                                    animate={{ opacity: 1 }}
                                    exit={{ opacity: 0 }}
                                    className="absolute inset-0 flex items-center justify-center bg-gray-800/50 rounded-lg"
                                >
                                    <p className="text-cyan-400 text-xl animate-pulse">
                                        Loading...
                                    </p>
                                </motion.div>
                            ) : error ? (
                                <motion.div
                                    key="error"
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    exit={{ opacity: 0, y: -10 }}
                                    className="absolute inset-0 flex items-center justify-center p-6"
                                >
                                    <div className="bg-red-500/10 border border-red-500/50 text-red-200 p-6 rounded-xl max-w-lg text-center backdrop-blur-sm">
                                        <div className="bg-red-500/20 w-12 h-12 rounded-full flex items-center justify-center mx-auto mb-4">
                                            <svg
                                                xmlns="http://www.w3.org/2000/svg"
                                                className="h-6 w-6 text-red-500"
                                                fill="none"
                                                viewBox="0 0 24 24"
                                                stroke="currentColor"
                                                strokeWidth={2}
                                            >
                                                <path
                                                    strokeLinecap="round"
                                                    strokeLinejoin="round"
                                                    d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                                                />
                                            </svg>
                                        </div>
                                        <strong className="block text-lg font-bold text-red-400 mb-2">
                                            Execution Failed
                                        </strong>
                                        <pre className="text-sm whitespace-pre-wrap font-mono bg-black/30 p-3 rounded-lg text-left overflow-x-auto">
                                            {error}
                                        </pre>
                                    </div>
                                </motion.div>
                            ) : (
                                // Call the separate render function here
                                renderContent()
                            )}
                        </AnimatePresence>
                    </div>
                </div>
            </main>
        </div>
    )
}

export default App
