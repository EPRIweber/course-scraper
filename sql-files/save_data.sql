-- Create a user-defined table type to match the data being sent.
-- This defines the structure of the data we'll pass from Python.
CREATE TYPE dbo.CourseData_v1 AS TABLE (
    course_code NVARCHAR(255),
    course_title NVARCHAR(512) NOT NULL,
    course_description NVARCHAR(MAX)
);
GO

CREATE OR ALTER PROCEDURE dbo.save_course_data
(
    @source_id_in UNIQUEIDENTIFIER,
    @course_data dbo.CourseData_v1 READONLY
)
AS
BEGIN
    SET NOCOUNT ON;

    -- The MERGE statement now uses the incoming table-valued parameter (@course_data)
    -- as its source, making the operation incredibly efficient.
    MERGE dbo.courses WITH(HOLDLOCK) AS target
    USING (
        SELECT
            @source_id_in AS sid,
            course_code AS code,
            course_title AS title,
            course_description AS description
        FROM
            @course_data
    ) AS source
    ON (target.course_source_id = source.sid
        AND COALESCE(target.course_code, '') = COALESCE(source.code, '')
        AND target.course_title = source.title)
    WHEN MATCHED THEN
        UPDATE SET
            course_description = source.description
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (course_source_id, course_code, course_title, course_description)
        VALUES (source.sid, source.code, source.title, source.description);
END
GO