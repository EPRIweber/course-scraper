CREATE OR ALTER PROCEDURE dbo.get_data
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client.
    SET NOCOUNT ON;

    -- The main query to fetch the top 100 course records.
    SELECT TOP 100
        c.course_code,
        c.course_title,
        c.course_description
    FROM
        dbo.courses c
    JOIN
        dbo.sources s ON s.source_id = c.course_source_id
    WHERE
        s.source_id = @source_id_in;
END
GO