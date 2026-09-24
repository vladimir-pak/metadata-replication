package com.gpb.replication.stream.postgres;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class PostgresJdbcUrl {

    private static final String PREFIX =
            "jdbc:postgresql:";

    private static final String NETWORK_PREFIX =
            "jdbc:postgresql://";

    private PostgresJdbcUrl() {
    }

    public static String withDatabase(
            String jdbcUrl,
            String databaseName) {

        if (jdbcUrl == null || jdbcUrl.isBlank()) {

            throw new IllegalArgumentException(
                    "PostgreSQL JDBC URL must not be blank"
            );
        }

        if (databaseName == null || databaseName.isBlank()) {

            throw new IllegalArgumentException(
                    "PostgreSQL database name must not be blank"
            );
        }

        if (!jdbcUrl.startsWith(PREFIX)) {
            throw new IllegalArgumentException(
                    "Not a PostgreSQL JDBC URL: "
                            + jdbcUrl
            );
        }

        int queryIndex = jdbcUrl.indexOf('?');

        String base =
                queryIndex >= 0
                        ? jdbcUrl.substring(
                            0,
                            queryIndex
                        )
                        : jdbcUrl;

        String query =
                queryIndex >= 0
                        ? jdbcUrl.substring(
                            queryIndex
                        )
                        : "";

        String encodedDatabase = encode(databaseName);

        if (base.startsWith(NETWORK_PREFIX)) {

            int authorityStart = NETWORK_PREFIX.length();

            int databaseSeparator =
                    base.indexOf(
                        '/',
                        authorityStart
                    );

            String serverPart =
                    databaseSeparator >= 0
                            ? base.substring(
                                0,
                                databaseSeparator
                            )
                            : base;

            return serverPart
                    + "/"
                    + encodedDatabase
                    + query;
        }

        /*
         * jdbc:postgresql:database
         */
        return PREFIX
                + encodedDatabase
                + query;
    }

    private static String encode(
            String value) {

        return URLEncoder.encode(
                        value,
                        StandardCharsets.UTF_8
                )
                .replace("+", "%20");
    }
}
