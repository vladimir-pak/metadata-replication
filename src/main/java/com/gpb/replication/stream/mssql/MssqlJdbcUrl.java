package com.gpb.replication.stream.mssql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class MssqlJdbcUrl {

    private static final String PREFIX = "jdbc:sqlserver://";

    private MssqlJdbcUrl() {
    }

    public static String withDatabase(
            String jdbcUrl,
            String databaseName) {

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException(
                    "MSSQL JDBC URL must not be blank"
            );
        }

        if (databaseName == null || databaseName.isBlank()) {
            throw new IllegalArgumentException(
                    "MSSQL database name must not be blank"
            );
        }

        if (!jdbcUrl.toLowerCase(Locale.ROOT).startsWith(PREFIX)) {
            throw new IllegalArgumentException(
                    "Only Microsoft SQL Server JDBC URL is supported: "
                            + jdbcUrl
            );
        }

        int propertiesStart = jdbcUrl.indexOf(';');

        String serverPart = propertiesStart >= 0
                ? jdbcUrl.substring(0, propertiesStart)
                : jdbcUrl;

        String propertiesPart = propertiesStart >= 0
                ? jdbcUrl.substring(propertiesStart + 1)
                : "";

        List<String> properties = splitProperties(propertiesPart);
        List<String> result = new ArrayList<>(properties.size() + 1);

        for (String property : properties) {
            String key = propertyKey(property);

            if ("databasename".equalsIgnoreCase(key)
                    || "database".equalsIgnoreCase(key)) {
                continue;
            }

            if (!property.isBlank()) {
                result.add(property);
            }
        }

        result.add(
                "databaseName=" + escapePropertyValue(databaseName)
        );

        return serverPart
                + ";"
                + String.join(";", result)
                + ";";
    }

    private static String propertyKey(String property) {
        int equals = property.indexOf('=');
        if (equals < 0) {
            return property.trim();
        }
        return property.substring(0, equals).trim();
    }

    private static List<String> splitProperties(String value) {
        List<String> result = new ArrayList<>();

        if (value == null || value.isEmpty()) {
            return result;
        }

        StringBuilder current = new StringBuilder();
        boolean inBraces = false;

        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);

            if (ch == '{' && !inBraces) {
                inBraces = true;
                current.append(ch);
                continue;
            }

            if (ch == '}' && inBraces) {
                if (i + 1 < value.length()
                        && value.charAt(i + 1) == '}') {
                    current.append("}}");
                    i++;
                    continue;
                }

                inBraces = false;
                current.append(ch);
                continue;
            }

            if (ch == ';' && !inBraces) {
                if (!current.isEmpty()) {
                    result.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }

            current.append(ch);
        }

        if (!current.isEmpty()) {
            result.add(current.toString());
        }

        return result;
    }

    private static String escapePropertyValue(String value) {
        return "{" + value.replace("}", "}}") + "}";
    }
}
