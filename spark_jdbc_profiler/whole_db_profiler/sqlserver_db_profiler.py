def profile_whole_db(spark, jdbc_url, connnection_properties):
    """
    Profile the whole database.

    Sample result dataframe:
    TABLE_NAME,TABLE_ROWS,Size (MB),Primary Col,Unique_Key,Foreign_Key
    files,7182964,4680,id,null,null
    file_text,123791,1912,filename,null,null
    conversion_log,4826848,1877,id,null,null
    """
    query = _get_query()

    jdbcDF = (spark.read.jdbc(
        jdbc_url,
        query,
        properties=connnection_properties))
    return jdbcDF


def _get_query():
    query = '''(
                select a.TableName, a.TABLE_ROWS, b.SizeMB, c.Primary_Key, d.Foreign_Key
                from
                  (SELECT TBL.object_id, TBL.name as TableName, SUM(PART.rows) AS TABLE_ROWS
                  FROM sys.tables TBL
                  INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id
                  INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id
                  AND PART.index_id = IDX.index_id
                  AND IDX.index_id < 2
                  GROUP BY TBL.object_id, TBL.name) a
                LEFT JOIN 
                  (
                    select
                         cte.TableName, 
                         cast((cte.pages * 8.)/1024 as decimal(10,3)) as SizeMB
                     from (
                       SELECT
                       t.name as TableName,
                       SUM (CASE
                               WHEN (i.index_id < 2) THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)
                               ELSE lob_used_page_count + row_overflow_used_page_count
                               END) as pages
                       FROM 
                         sys.dm_db_partition_stats  AS s 
                       JOIN sys.tables AS t ON s.object_id = t.object_id
                       JOIN sys.indexes AS i ON i.[object_id] = t.[object_id] 
                           AND s.index_id = i.index_id
                       GROUP BY t.name
                     ) cte 
                   ) b
                 ON a.TableName = b.TableName
                LEFT JOIN
                       (
                          SELECT TABLE_NAME,
                          STRING_AGG(COLUMN_NAME, ',') AS Primary_Key
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                          WHERE CONSTRAINT_NAME like 'PK_%'
                          GROUP BY TABLE_NAME
                       ) c
                       ON a.TableName = c.TABLE_NAME
                     LEFT JOIN
                       (
                          SELECT TABLE_NAME,
                          STRING_AGG(COLUMN_NAME, ',') AS Foreign_Key
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                          WHERE CONSTRAINT_NAME like 'FK_%'
                          GROUP BY TABLE_NAME
                       ) d
                       ON a.TableName = d.TABLE_NAME   
                    ) a'''

    return query
