// dashboard/frontend/src/App.tsx

import { useEffect, useState, Fragment } from 'react';

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

      {/* table */}
      {activeView && (
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
            {rows.map((r, i) => {
              const thisName = nameColumn ? String(r[nameColumn]) : null;
              const isExpanded = thisName === expandedName;
              return (
                <Fragment key={i}>
                  <tr
                    className="border-b cursor-pointer hover:bg-gray-50"
                    onClick={() => onRowClick(r)}
                  >
                    {headers.map(h => (
                      <td key={h} className="p-2 align-top">
                        {String(r[h] ?? '')}
                      </td>
                    ))}
                  </tr>

                  {/* preview row */}
                  {isExpanded && previews[thisName!] && (
                    <tr className="bg-gray-50">
                      <td colSpan={headers.length} className="p-2">
                        <div className="space-y-2">
                          <div>
                            Total distinct courses:{' '}
                            {previews[thisName!].distinct_course_count}
                          </div>
                          <ul className="list-disc pl-5">
                            {previews[thisName!].sample_courses.map((c, idx) => (
                              <li key={idx}>
                                <strong>
                                  {c.course_code} {c.course_title}
                                </strong>
                                {c.course_description_preview && (
                                  <> – {c.course_description_preview}</>
                                )}
                                {c.course_credits && <> ({c.course_credits})</>}
                              </li>
                            ))}
                          </ul>
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}

            {rows.length === 0 && (
              <tr>
                <td
                  colSpan={headers.length || 1}
                  className="p-4 text-center"
                >
                  Data Loading…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  );
}


