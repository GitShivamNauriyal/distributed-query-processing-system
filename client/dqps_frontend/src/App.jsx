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
            if (!response.ok || data.error) {
                throw new Error(
                    data.error_message || "An error occurred on the server."
                )
            }
            setResults(data)
            setAnimationKey((prevKey) => prevKey + 1)
        } catch (err) {
            setError(err.message)
            setResults([])
        } finally {
            setIsLoading(false)
        }
    }

    // Animation variants for the table container
    const tableVariants = {
        hidden: { opacity: 0 },
        visible: {
            opacity: 1,
            transition: {
                staggerChildren: 0.03, // Each row will appear slightly after the one before
            },
        },
    }

    // Animation variants for each table row
    const rowVariants = {
        hidden: { opacity: 0, y: 15 },
        visible: {
            opacity: 1,
            y: 0,
            transition: { type: "spring", stiffness: 300, damping: 24 },
        },
    }

    const renderTable = () => {
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
        const headers = Object.keys(results[0])

        return (
            // KEY CHANGE: This outer div is now the dedicated scroll container.
            // It takes up the full height of its parent and will show a scrollbar if the table inside is too tall.
            <div className="h-full overflow-auto rounded-lg border border-gray-700">
                <motion.table
                    key={`results-table-${animationKey}`}
                    className="min-w-full text-sm text-left whitespace-nowrap"
                    variants={tableVariants}
                    initial="hidden"
                    animate="visible"
                >
                    <thead className="sticky top-0 bg-gray-800 text-gray-300 uppercase tracking-wider z-10">
                        <tr>
                            {headers.map((header) => (
                                <th key={header} className="p-4 font-semibold">
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
                                        className="p-4 font-mono text-cyan-300"
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
        <div className="bg-gray-900 text-white h-screen w-screen overflow-hidden flex flex-col font-sans">
            {/* Header */}
            <header className="flex-shrink-0 bg-gray-800/50 border-b border-gray-700 shadow-md">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4 flex items-center gap-4">
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        className="h-8 w-8 text-cyan-400"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                    >
                        <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            d="M4 7v10m16-10v10M4 12h16m-12 5h8m-8-10h8m-8 5h8m5-5h.01M19 17h.01"
                        />
                    </svg>
                    <div>
                        <h1 className="text-2xl font-bold text-cyan-400">
                            Distributed SQL Query Engine
                        </h1>
                        <p className="text-sm text-gray-400">
                            A web-based interface for executing queries across a
                            distributed database.
                        </p>
                    </div>
                </div>
            </header>

            {/* Main Content Area */}
            <main className="flex-grow flex flex-col md:flex-row gap-4 p-4 sm:p-6 lg:p-8 overflow-hidden">
                {/* Left Panel: Query Editor */}
                <div className="flex flex-col md:w-1/3 h-full">
                    <form
                        onSubmit={handleSubmitQuery}
                        className="flex flex-col h-full bg-gray-800 rounded-lg p-4 border border-gray-700"
                    >
                        <label
                            htmlFor="query-editor"
                            className="mb-2 text-lg font-semibold text-gray-300"
                        >
                            SQL Query
                        </label>
                        <textarea
                            id="query-editor"
                            className="flex-grow w-full p-4 bg-black/30 border-2 border-gray-600 rounded-lg text-gray-200 font-mono focus:outline-none focus:ring-2 focus:ring-cyan-500 focus:border-cyan-500 transition resize-none"
                            value={sqlQuery}
                            onChange={(e) => setSqlQuery(e.target.value)}
                            placeholder="SELECT * FROM sales;"
                        />
                        <button
                            type="submit"
                            disabled={isLoading}
                            className="mt-4 px-6 py-3 bg-cyan-600 rounded-lg font-semibold text-white hover:bg-cyan-500 disabled:bg-gray-600 disabled:cursor-not-allowed transition-all duration-200 ease-in-out shadow-lg transform hover:scale-105"
                        >
                            {isLoading ? "Executing..." : "Execute Query"}
                        </button>
                    </form>
                </div>

                {/* Right Panel: Results */}
                <div className="flex flex-col md:w-2/3 h-full bg-gray-800 rounded-lg p-4 border border-gray-700">
                    <h2 className="text-lg font-semibold mb-4 text-gray-300">
                        Results
                    </h2>
                    <div className="relative flex-grow min-h-0">
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
                                    className="bg-red-900/50 border border-red-500 text-red-300 p-4 rounded-lg"
                                >
                                    <strong className="font-bold">
                                        Error:
                                    </strong>
                                    <pre className="mt-2 whitespace-pre-wrap font-mono">
                                        {error}
                                    </pre>
                                </motion.div>
                            ) : (
                                renderTable()
                            )}
                        </AnimatePresence>
                    </div>
                </div>
            </main>
        </div>
    )
}

export default App
