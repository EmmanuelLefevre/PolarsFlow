

SELECT
    file_name,
    file_path,
    content_type,
    file_size,
    file_url
FROM "file"."main_cleansed"."contents_cleaned"
WHERE content_type IN ('file', 'dir')