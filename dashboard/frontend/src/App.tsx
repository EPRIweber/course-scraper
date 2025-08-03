// dashboard/frontend/src/App.tsx

import { useEffect, useState, Fragment } from 'react';

type JsonObject = Record<string, any>;

interface CourseResponse {
  school_name?: string;
  distinct_course_count?: number;
  sample_courses?: JsonObject[];
  [k: string]: any;
}

export default function App() {
  const [rows, setRows] = useState<JsonObject[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [previews, setPreviews] = useState<Record<string, CourseResponse>>({});
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = async () => {
    try {
      const res = await fetch('/api/schools_status');
      if (!res.ok) {
        const text = await res.text();
        console.error("Fetch failed:", res.status, text);
        setError(`Failed to fetch status: ${res.status} ${text}`);
        return;
      }
      const json = await res.json();
      setRows(json.data || []);
      setError(null);
      console.log("schools_status response:", json);
    } catch (err: any) {
      console.error("Network or parse error:", err);
      setError(`Network error: ${err.message || err}`);
    }
  };

  useEffect(() => {
    fetchStatus();
    const id = setInterval(fetchStatus, 15000);
    return () => clearInterval(id);
  }, []);

  const toggle = async (name: string) => {
    if (expanded === name) {
      setExpanded(null);
      return;
    }
    setExpanded(name);
    if (!previews[name]) {
      try {
        const res = await fetch(`/api/school/${encodeURIComponent(name)}/courses?limit=5`);
        if (!res.ok) {
          const t = await res.text();
          console.error("Preview fetch failed:", res.status, t);
          return;
        }
        const json = await res.json();
        setPreviews(prev => ({ ...prev, [name]: json }));
        console.log(`preview for ${name}:`, json);
      } catch (err) {
        console.error("Error fetching preview:", err);
      }
    }
  };

  // derive headers from first row (union of keys)
  const schoolHeaders = rows.length
    ? Array.from(new Set(rows.flatMap(r => Object.keys(r))))
    : [];

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold">School Scrape Status</h1>
      {error && (
        <div className="bg-red-100 text-red-800 p-2 rounded">
          Error: {error}
        </div>
      )}
      <table className="min-w-full text-sm border-collapse">
        <thead>
          <tr className="bg-gray-100">
            {schoolHeaders.map(h => (
              <th key={h} className="p-2 text-left">
                {h.replace(/_/g, ' ')}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(r => {
            const name = r.school_name || r.school || 'unknown';
            return (
              <Fragment key={name}>
                <tr
                  className="border-b cursor-pointer hover:bg-gray-50"
                  onClick={() => toggle(name)}
                >
                  {schoolHeaders.map(h => (
                    <td key={h} className="p-2 align-top">
                      {String(r[h] ?? '')}
                    </td>
                  ))}
                </tr>
                {expanded === name && previews[name] && (
                  <tr className="bg-gray-50">
                    <td colSpan={schoolHeaders.length} className="p-2">
                      <div className="p-2 space-y-2 border rounded">
                        <div className="font-semibold">
                          Preview for {previews[name].school_name ?? name}
                        </div>
                        <div>
                          Total distinct courses:{' '}
                          {previews[name].distinct_course_count ?? 'N/A'}
                        </div>
                        <ul className="list-disc pl-5">
                          {(previews[name].sample_courses || []).map((course, idx) => (
                            <li key={idx} className="mb-1">
                              <div>
                                <strong>
                                  {(course.course_code ?? course.courseCode ?? '').toString().trim()}{' '}
                                  {(course.course_title ?? course.title ?? '').toString().trim()}
                                </strong>
                                {course.course_credits && <> ({course.course_credits})</>}
                              </div>
                              {course.course_description_preview || course.description ? (
                                <div className="text-sm">
                                  {course.course_description_preview ??
                                    course.description ??
                                    ''}
                                </div>
                              ) : null}
                              {/* show any extra fields dynamically for debugging */}
                              <div className="text-xs text-gray-500">
                                {Object.entries(course)
                                  .filter(
                                    ([k, _]) =>
                                      ![
                                        'course_code',
                                        'course_title',
                                        'course_description_preview',
                                        'course_description',
                                        'course_credits',
                                        'courses_crtd_dt',
                                      ].includes(k)
                                  )
                                  .map(([k, v]) => (
                                    <div key={k}>
                                      <em>{k}:</em> {String(v)}
                                    </div>
                                  ))}
                              </div>
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
          {rows.length === 0 && !error && (
            <tr>
              <td colSpan={schoolHeaders.length || 5} className="p-4 text-center">
                Loading...
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
