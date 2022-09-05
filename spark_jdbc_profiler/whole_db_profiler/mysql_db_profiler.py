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
                      SELECT a.TABLE_NAME,
                             a.TABLE_ROWS,
                            ROUND((a.DATA_LENGTH + a.INDEX_LENGTH) / 1024 / 1024) AS `Size (MB)`,
                            b.`Primary_Key`,
                            d.`Unique_Key`,
                            c.`Foreign_Key`,
                            e.`Default_Values`
                     FROM 
                           information_schema.tables a
                     LEFT JOIN
                       (
                          SELECT TABLE_NAME,
                          GROUP_CONCAT(COLUMN_NAME ) as `Primary_Key`
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                          where CONSTRAINT_NAME = 'PRIMARY'
                          GROUP BY TABLE_NAME
                       ) b
                       ON a.TABLE_NAME = b.TABLE_NAME

                     LEFT JOIN
                       (
                           SELECT TABLE_NAME,
                          GROUP_CONCAT(COLUMN_NAME ) as `Foreign_Key`
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                          WHERE
                            CONSTRAINT_NAME like '%fk%'
                            or constraint_name like '%foreign%'
                          GROUP BY TABLE_NAME
                       ) c
                       ON a.TABLE_NAME = c.TABLE_NAME   

                     LEFT JOIN
                       (
                          SELECT TABLE_NAME,
                          GROUP_CONCAT(COLUMN_NAME ) as `Unique_Key`
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                          WHERE
                            CONSTRAINT_NAME like '%uni%'
                          GROUP BY TABLE_NAME
                       ) d
                       ON a.TABLE_NAME = d.TABLE_NAME  
                    
                    LEFT JOIN
                       (
                           SELECT TABLE_NAME,
                          GROUP_CONCAT( CONCAT(COLUMN_NAME, ':', column_default)) as `Default_Values`
                          FROM INFORMATION_SCHEMA.columns
                          WHERE
                            column_default is not null and column_default <> ''
                            and table_schema not in ('information_schema', 'sys',
                                                    'performance_schema','mysql')
                          GROUP BY TABLE_NAME
                       ) e
                       ON a.TABLE_NAME = e.TABLE_NAME   

                     WHERE
                      a.TABLE_TYPE= 'BASE TABLE'
                     order by DATA_LENGTH + INDEX_LENGTH desc
                    ) a'''

    return query
