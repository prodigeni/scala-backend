SELECT [wiki_id] AS wiki_id,
       COUNT(CASE WHEN img_media_type = 'ARCHIVE'    THEN 1 ELSE null END) AS archive,
       COUNT(CASE WHEN img_media_type = 'AUDIO'      THEN 1 ELSE null END) AS audio,
       COUNT(CASE WHEN img_media_type = 'BITMAP'     THEN 1 ELSE null END) AS bitmap,
       COUNT(CASE WHEN img_media_type = 'DRAWING'    THEN 1 ELSE null END) AS drawing,
       COUNT(CASE WHEN img_media_type = 'EXECUTABLE' THEN 1 ELSE null END) AS executable,
       COUNT(CASE WHEN img_media_type = 'MULTIMEDIA' THEN 1 ELSE null END) AS multimedia,
       COUNT(CASE WHEN img_media_type = 'OFFICE'     THEN 1 ELSE null END) AS office,
       COUNT(CASE WHEN img_media_type = 'TEXT'       THEN 1 ELSE null END) AS text,
       COUNT(CASE WHEN img_media_type = 'UNKNOWN'    THEN 1 ELSE null END) AS unknown,
       COUNT(CASE WHEN img_media_type = 'VIDEO'      THEN 1 ELSE null END) AS video
  FROM image
