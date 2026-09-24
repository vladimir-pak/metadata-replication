package com.gpb.replication.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;

import com.gpb.replication.enums.DatabaseType;
import com.gpb.replication.exceptions.MetadataReplicationException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractMetadataCopyStreamer {

    protected abstract int getFetchSize();

    protected int getQueryTimeoutSeconds() {
        return 0;
    }

    /**
     * Hook для vendor-specific настройки source connection.
     *
     * Oracle: ничего.
     * PostgreSQL: позже здесь можно включить autoCommit=false
     * для cursor-based fetching.
     */
    protected void configureSourceConnection(
            Connection connection) throws SQLException {
    }

    protected void configureStatement(
            PreparedStatement statement) throws SQLException {

        statement.setFetchSize(getFetchSize());

        try {
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (SQLException e) {
            log.debug(
                    "JDBC driver does not support fetch direction configuration",
                    e
            );
        }

        if (getQueryTimeoutSeconds() > 0) {
            statement.setQueryTimeout(
                    getQueryTimeoutSeconds()
            );
        }
    }

    protected final long copyDatabases(
            Connection sourceConnection,
            Connection targetConnection,
            DatabaseType databaseType,
            String sourceSql,
            SourceSqlBinder binder,
            CopyRowSerializer serializer) {

        return stream(
                sourceConnection,
                targetConnection,
                sourceSql,
                binder,
                buildDatabaseCopySql(databaseType),
                serializer,
                "DATABASE"
        );
    }

    protected final long copySchemas(
            Connection sourceConnection,
            Connection targetConnection,
            DatabaseType databaseType,
            String sourceSql,
            SourceSqlBinder binder,
            CopyRowSerializer serializer) {

        return stream(
                sourceConnection,
                targetConnection,
                sourceSql,
                binder,
                buildSchemaCopySql(databaseType),
                serializer,
                "SCHEMA"
        );
    }

    protected final long copyTables(
            Connection sourceConnection,
            Connection targetConnection,
            DatabaseType databaseType,
            String sourceSql,
            SourceSqlBinder binder,
            CopyRowSerializer serializer) {

        return stream(
                sourceConnection,
                targetConnection,
                sourceSql,
                binder,
                buildTableCopySql(databaseType),
                serializer,
                "TABLE"
        );
    }

    private long stream(
            Connection sourceConnection,
            Connection targetConnection,
            String sourceSql,
            SourceSqlBinder binder,
            String copySql,
            CopyRowSerializer serializer,
            String entityType) {

        long started = System.nanoTime();

        try {

            configureSourceConnection(sourceConnection);

            try (
                    PreparedStatement statement =
                            sourceConnection.prepareStatement(
                                    sourceSql,
                                    ResultSet.TYPE_FORWARD_ONLY,
                                    ResultSet.CONCUR_READ_ONLY
                            )
            ) {

                configureStatement(statement);
                binder.bind(statement);

                log.info(
                        "Starting {} metadata source query, fetchSize={}",
                        entityType,
                        getFetchSize()
                );

                try (ResultSet resultSet = statement.executeQuery()) {

                    PGConnection pgConnection =
                            targetConnection.unwrap(PGConnection.class);

                    CopyIn copyIn =
                            pgConnection
                                    .getCopyAPI()
                                    .copyIn(copySql);

                    long sourceRows = 0;
                    long streamedRows = 0;
                    long skippedRows = 0;

                    try {

                        while (resultSet.next()) {

                            sourceRows++;

                            byte[] row =
                                    serializer.serialize(resultSet);

                            /*
                             * null from serializer means that the source row
                             * was intentionally excluded from the snapshot.
                             *
                             * This keeps STANDARD streaming zero-copy:
                             * no intermediate collection is required just
                             * to apply schema/table exclusion rules.
                             */
                            if (row == null) {
                                skippedRows++;
                                continue;
                            }

                            copyIn.writeToCopy(
                                    row,
                                    0,
                                    row.length
                            );

                            streamedRows++;
                        }

                        long copiedRows = copyIn.endCopy();

                        long elapsedMs =
                                (System.nanoTime() - started)
                                        / 1_000_000;

                        log.info(
                                "{} metadata COPY completed: "
                                        + "sourceRows={}, streamedRows={}, "
                                        + "skippedRows={}, copiedRows={}, elapsedMs={}",
                                entityType,
                                sourceRows,
                                streamedRows,
                                skippedRows,
                                copiedRows,
                                elapsedMs
                        );

                        return copiedRows >= 0
                                ? copiedRows
                                : streamedRows;

                    } catch (Exception e) {

                        try {
                            copyIn.cancelCopy();
                        } catch (SQLException cancelException) {
                            e.addSuppressed(cancelException);
                        }

                        throw e;
                    }
                }
            }

        } catch (Exception e) {

            throw new MetadataReplicationException(
                    "Failed to stream "
                            + entityType
                            + " metadata",
                    e
            );
        }
    }

    private String buildDatabaseCopySql(
            DatabaseType databaseType) {

        return """
                COPY metadata_replication.database_metadata_%s
                (
                    id,
                    fqn,
                    service_name,
                    name,
                    hash_data
                )
                FROM STDIN
                WITH
                (
                    FORMAT CSV,
                    DELIMITER E'\\t',
                    NULL '\\N',
                    QUOTE '"',
                    ESCAPE '"',
                    ENCODING 'UTF8'
                )
                """.formatted(suffix(databaseType));
    }

    private String buildSchemaCopySql(
            DatabaseType databaseType) {

        return """
                COPY metadata_replication.schema_metadata_%s
                (
                    id,
                    fqn,
                    service_name,
                    db_name,
                    name,
                    parent_fqn,
                    hash_data
                )
                FROM STDIN
                WITH
                (
                    FORMAT CSV,
                    DELIMITER E'\\t',
                    NULL '\\N',
                    QUOTE '"',
                    ESCAPE '"',
                    ENCODING 'UTF8'
                )
                """.formatted(suffix(databaseType));
    }

    private String buildTableCopySql(
            DatabaseType databaseType) {

        return """
                COPY metadata_replication.table_metadata_%s
                (
                    id,
                    fqn,
                    service_name,
                    db_name,
                    schema_name,
                    description,
                    name,
                    parent_fqn,
                    data,
                    hash_data
                )
                FROM STDIN
                WITH
                (
                    FORMAT CSV,
                    DELIMITER E'\\t',
                    NULL '\\N',
                    QUOTE '"',
                    ESCAPE '"',
                    ENCODING 'UTF8'
                )
                """.formatted(suffix(databaseType));
    }

    private String suffix(DatabaseType databaseType) {
        return databaseType
                .name()
                .toLowerCase(Locale.ROOT);
    }
}