// dashboard/frontend/src/App.tsx

import { useEffect, useState } from 'react';

type JsonObject = Record<string, any>;

export default function App() {
  const [views, setViews] = useState<string[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [data, setData] = useState<Record<string, JsonObject[]>>({});
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadViews = async () => {
      try {
        const res = await fetch('/api/views');
        if (!res.ok) {
          const t = await res.text();
          throw new Error(t);
        }
        const json = await res.json();
        const list: string[] = json.views || [];
        setViews(list);
        if (list.length) {
          setActive(list[0]);
        }
      } catch (err: any) {
        console.error('Failed to load views', err);
        setError(`Failed to load views: ${err.message || err}`);
      }
    };
    loadViews();
  }, []);

  useEffect(() => {
    if (!active || data[active]) return;
    const loadData = async () => {
      try {
        const res = await fetch(`/api/view/${encodeURIComponent(active)}?limit=100`);
        if (!res.ok) {
          const t = await res.text();
          throw new Error(t);
        }
        const json = await res.json();
        setData(prev => ({ ...prev, [active]: json.data || [] }));
      } catch (err: any) {
        console.error('Failed to load data', err);
        setError(`Failed to load data for ${active}: ${err.message || err}`);
      }
    };
    loadData();
  }, [active, data]);

  const rows = active ? data[active] || [] : [];
  const headers = rows.length
    ? Array.from(new Set(rows.flatMap(r => Object.keys(r))))
    : [];

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold">Database Views</h1>
      {error && (
        <div className="bg-red-100 text-red-800 p-2 rounded">Error: {error}</div>
      )}
      <div className="flex space-x-4 border-b">
        {views.map(v => (
          <button
            key={v}
            className={`p-2 -mb-px ${active === v ? 'border-b-2 border-blue-500 font-semibold' : ''}`}
            onClick={() => setActive(v)}
          >
            {v.replace(/_/g, ' ')}
          </button>
        ))}
      </div>
      {active && (
        <table className="min-w-full text-sm border-collapse">
          <thead>
            <tr className="bg-gray-100">
              {headers.map(h => (
                <th key={h} className="p-2 text-left">
                  {h.replace(/_/g, ' ')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, idx) => (
              <tr key={idx} className="border-b">
                {headers.map(h => (
                  <td key={h} className="p-2 align-top">
                    {String(r[h] ?? '')}
                  </td>
                ))}
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={headers.length || 1} className="p-4 text-center">
                  No data
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}

