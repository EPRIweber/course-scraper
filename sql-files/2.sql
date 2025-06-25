CREATE OR ALTER PROCEDURE dbo.save_schema
(
    @source_id_in UNIQUEIDENTIFIER,
    @schema_json_in NVARCHAR(MAX)
)
AS
BEGIN
    -- This prevents the count of affected rows from being sent to the client.
    SET NOCOUNT ON;

    -- The MERGE statement to either insert a new schema or update an existing one.
    MERGE dbo.scraper_schemas AS target
    USING (SELECT @source_id_in AS source_id, @schema_json_in AS schema_json) AS source
    ON (target.scraper_schema_source_id = source.source_id)
    WHEN MATCHED THEN
        UPDATE SET
            scraper_schema_json = source.schema_json
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (scraper_schema_source_id, scraper_schema_json)
        VALUES (source.source_id, source.schema_json);
END
GO