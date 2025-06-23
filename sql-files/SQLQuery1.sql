CREATE OR ALTER PROCEDURE dbo.get_schema
(
    @source_id_in UNIQUEIDENTIFIER
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client
    -- which is a good practice for performance.
    SET NOCOUNT ON;

    -- The main query to fetch the schema JSON
    SELECT ss.scraper_schema_json
    FROM dbo.scraper_schemas ss
    JOIN dbo.sources s ON s.source_id = ss.scraper_schema_source_id
    WHERE s.source_id = @source_id_in;
END
GO