// dashboard/frontend/src/App.tsx

import { useEffect, useState, Fragment } from 'react';
import React from 'react';
import GridView from './components/GridView';


type JsonObject = Record<string, any>;

interface CoursePreview {
  course_code: string | null;
  course_title: string | null;
  course_description_preview: string | null;
  course_credits: string | null;
  courses_crtd_dt: string | null;
}

interface CourseResponse {
  school_name: string;
  distinct_course_count: number;
  sample_courses: CoursePreview[];
}

export default function App() {
  const [views, setViews] = useState<string[]>([]);
  const [activeView, setActiveView] = useState<string | null>(null);
  const [data, setData] = useState<Record<string, JsonObject[]>>({});
  const [error, setError] = useState<string | null>(null);

  // for the course preview:
  const [expandedName, setExpandedName] = useState<string | null>(null);
  const [previews, setPreviews] = useState<Record<string, CourseResponse>>({});

  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const [lastSelectedIndex, setLastSelectedIndex] = useState<number | null>(null);

  // load list of views
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch('/api/views');
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        setViews(json.views || []);
        if (json.views?.length) setActiveView(json.views[0]);
      } catch (err: any) {
        console.error('Failed to load views', err);
        setError(`Failed to load views: ${err.message || err}`);
      }
    })();
  }, []);

  // load data for currently active view
  useEffect(() => {
    if (!activeView || data[activeView]) return;
    (async () => {
      try {
        const res = await fetch(`/api/view/${encodeURIComponent(activeView)}?limit=100`);
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        setData(d => ({ ...d, [activeView]: json.data || [] }));
      } catch (err: any) {
        console.error(`Failed to load data for ${activeView}`, err);
        setError(`Failed to load data for ${activeView}: ${err.message || err}`);
      }
    })();
  }, [activeView, data]);

  const rows = activeView ? data[activeView] || [] : [];
  const headers = rows.length
    ? Array.from(new Set(rows.flatMap(r => Object.keys(r))))
    : [];

    // find the first header that includes "name"
  const nameColumn = headers.find(h => /name/i.test(h)) || null;

  // click handler to expand/collapse and fetch preview
  const onRowClick = async (row: JsonObject) => {
    if (!nameColumn) return;
    const schoolName = String(row[nameColumn]);
    if (expandedName === schoolName) {
      setExpandedName(null);
      return;
    }
    setExpandedName(schoolName);

    if (!previews[schoolName]) {
      try {
        const res = await fetch(`/api/school/${encodeURIComponent(schoolName)}/courses?limit=5`);
        if (!res.ok) throw new Error(await res.text());
        const previewJson: CourseResponse = await res.json();
        setPreviews(p => ({ ...p, [schoolName]: previewJson }));
      } catch (err: any) {
        console.error(`Failed to load preview for ${schoolName}`, err);
        setError(`Failed to load preview for ${schoolName}: ${err.message || err}`);
      }
    }
  };

  // ─── handle click on the row‐number cell ────────────────────────────────────
  const handleRowSelect = (
    e: React.MouseEvent<HTMLTableCellElement, MouseEvent>,
    index: number
  ) => {
    // prevent the row‐click expansion handler
    e.stopPropagation();

    if (e.shiftKey && lastSelectedIndex !== null) {
      // select every index between lastSelectedIndex and this one
      const start = Math.min(lastSelectedIndex, index);
      const end   = Math.max(lastSelectedIndex, index);
      const range = Array.from({ length: end - start + 1 }, (_, i) => start + i);
      setSelectedRows(prev =>
        Array.from(new Set([...prev, ...range]))
      );
    } else {
      // single‐row select
      setSelectedRows([index]);
    }
    setLastSelectedIndex(index);
  };
  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold">Database Views</h1>
      {error && (
        <div className="bg-red-100 text-red-800 p-2 rounded">
          Error: {error}
        </div>
      )}

      {/* view selector */}
      <div className="flex space-x-4 border-b">
        {views.map(v => (
          <button
            key={v}
            onClick={() => {
              setActiveView(v);
              setExpandedName(null);
            }}
            className={`p-2 -mb-px ${
              activeView === v
                ? 'border-b-2 border-blue-500 font-semibold'
                : ''
            }`}
          >
            {v.replace(/dashboard_/g, '').replace(/_/g, ' ')}
          </button>
        ))}
      </div>
    {activeView && (
      <GridView rowData={rows} headers={headers} />
    )}
      
    </div>
  );
}


