import { useEffect, useState, Fragment } from 'react';

interface SchoolStatus {
  school_name: string;
  schema_count: number;
  url_count: number;
  course_count: number;
  has_courses: number;
  last_scrape_ts: string | null;
  summary_status: string | null;
  status_indicator: string | null;
}

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
  const [rows, setRows] = useState<SchoolStatus[]>([]);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [previews, setPreviews] = useState<Record<string, CourseResponse>>({});

  const fetchStatus = async () => {
    const res = await fetch('/api/schools_status');
    const json = await res.json();
    setRows(json.data || []);
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
      const res = await fetch(`/api/school/${encodeURIComponent(name)}/courses?limit=5`);
      const json = await res.json();
      setPreviews(prev => ({ ...prev, [name]: json }));
    }
  };

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-2xl font-bold">School Scrape Status</h1>
      <table className="min-w-full text-sm">
        <thead>
          <tr className="bg-gray-100">
            <th>School</th>
            <th>Status</th>
            <th>Indicator</th>
            <th>Courses</th>
            <th>Last Scrape</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <Fragment key={r.school_name}>
              <tr className="border-b cursor-pointer" onClick={() => toggle(r.school_name)}>
                <td>{r.school_name}</td>
                <td>{r.summary_status}</td>
                <td>{r.status_indicator}</td>
                <td>{r.course_count}</td>
                <td>{r.last_scrape_ts}</td>
              </tr>
              {expanded === r.school_name && previews[r.school_name] && (
                <tr className="bg-gray-50">
                  <td colSpan={5}>
                    <div className="p-2 space-y-2">
                      <div>
                        Total distinct courses: {previews[r.school_name].distinct_course_count}
                      </div>
                      <ul className="list-disc pl-5">
                        {previews[r.school_name].sample_courses.map((c, idx) => (
                          <li key={idx}>
                            <strong>
                              {c.course_code} {c.course_title}
                            </strong>
                            {c.course_description_preview && (
                              <> - {c.course_description_preview}</>
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
          ))}
        </tbody>
      </table>
    </div>
  );
}
