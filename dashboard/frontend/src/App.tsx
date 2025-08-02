import { useEffect, useMemo, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

interface PerformanceRow {
  source_name: string;
  run_id: number;
  extracted_count: number | null;
  url_count: number | null;
  slots_left: number | null;
  scrape_seconds: number | null;
  urls_per_second: number | null;
  records_per_second: number | null;
  inferred_max_concurrency: number | null;
  courses_extracted_ts: string | null;
  start_scrape_ts: string | null;
}

export default function App() {
  const [rows, setRows] = useState<PerformanceRow[]>([]);
  const [source, setSource] = useState('');
  const [runId, setRunId] = useState('');
  const [start, setStart] = useState('');
  const [selected, setSelected] = useState<string | null>(null);
  const [sortField, setSortField] = useState<keyof PerformanceRow>('courses_extracted_ts');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const fetchData = async () => {
    const params = new URLSearchParams({ limit: '100', offset: '0' });
    if (source) params.set('source_name', source);
    if (runId) params.set('run_id', runId);
    if (start) params.set('start_ts', start);
    const res = await fetch(`/api/performance?${params.toString()}`);
    const json = await res.json();
    setRows(json.data || []);
  };

  useEffect(() => { fetchData(); }, []);

  const sorted = useMemo(() => {
    return [...rows].sort((a, b) => {
      const aVal = a[sortField] ?? 0;
      const bVal = b[sortField] ?? 0;
      if (aVal < bVal) return sortDir === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
  }, [rows, sortField, sortDir]);

  const chartData = selected ? rows.filter(r => r.source_name === selected) : [];
  const slowThreshold = 1; // urls/sec

  const handleSort = (field: keyof PerformanceRow) => {
    if (sortField === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir('desc');
    }
  };

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold">Scraper Performance</h1>
      <div className="flex gap-2 items-end flex-wrap">
        <div>
          <label className="block text-sm">Source</label>
          <input value={source} onChange={e => setSource(e.target.value)} className="border p-1" />
        </div>
        <div>
          <label className="block text-sm">Run ID</label>
          <input value={runId} onChange={e => setRunId(e.target.value)} className="border p-1" />
        </div>
        <div>
          <label className="block text-sm">After</label>
          <input type="date" value={start} onChange={e => setStart(e.target.value)} className="border p-1" />
        </div>
        <button onClick={fetchData} className="bg-blue-500 text-white px-3 py-1 rounded">Apply</button>
      </div>
      <table className="min-w-full text-sm">
        <thead>
          <tr className="bg-gray-100">
            <th className="cursor-pointer" onClick={() => handleSort('source_name')}>Source</th>
            <th className="cursor-pointer" onClick={() => handleSort('run_id')}>Run</th>
            <th>Extracted</th>
            <th>URLs</th>
            <th>Slots Left</th>
            <th className="cursor-pointer" onClick={() => handleSort('scrape_seconds')}>Seconds</th>
            <th className="cursor-pointer" onClick={() => handleSort('urls_per_second')}>URLs/s</th>
            <th className="cursor-pointer" onClick={() => handleSort('records_per_second')}>Records/s</th>
            <th>Concurrency</th>
            <th className="cursor-pointer" onClick={() => handleSort('courses_extracted_ts')}>Finished</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(r => (
            <tr key={`${r.source_name}-${r.run_id}`}
                className={`border-b ${r.urls_per_second && r.urls_per_second < slowThreshold ? 'bg-red-100' : ''}`}
                onClick={() => setSelected(r.source_name)}>
              <td>{r.source_name}</td>
              <td>{r.run_id}</td>
              <td>{r.extracted_count}</td>
              <td>{r.url_count}</td>
              <td>{r.slots_left}</td>
              <td>{r.scrape_seconds}</td>
              <td>{r.urls_per_second?.toFixed(2)}</td>
              <td>{r.records_per_second?.toFixed(2)}</td>
              <td>{r.inferred_max_concurrency}</td>
              <td>{r.courses_extracted_ts}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {chartData.length > 0 && (
        <div>
          <h2 className="text-xl mt-4">Records per Second - {selected}</h2>
          <LineChart width={600} height={300} data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="run_id" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="records_per_second" stroke="#8884d8" />
            <Line type="monotone" dataKey="urls_per_second" stroke="#82ca9d" />
          </LineChart>
        </div>
      )}
    </div>
  );
}
