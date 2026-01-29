import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Validate from './pages/Validate'
import Results from './pages/Results'
import Schema from './pages/Schema'
import Query from './pages/Query'
import History from './pages/History'

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="validate" element={<Validate />} />
        <Route path="results/:id" element={<Results />} />
        <Route path="schema" element={<Schema />} />
        <Route path="query" element={<Query />} />
        <Route path="history" element={<History />} />
      </Route>
    </Routes>
  )
}

export default App
