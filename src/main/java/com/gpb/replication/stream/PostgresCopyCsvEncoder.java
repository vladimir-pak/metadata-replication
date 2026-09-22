package com.gpb.replication.stream;

import java.nio.charset.StandardCharsets;

public final class PostgresCopyCsvEncoder {

    private PostgresCopyCsvEncoder() {
    }

    public static byte[] encode(Object... values) {

        StringBuilder row = new StringBuilder(1024);

        for (int i = 0; i < values.length; i++) {

            if (i > 0) {
                row.append('\t');
            }

            appendValue(row, values[i]);
        }

        row.append('\n');

        return row.toString()
                .getBytes(StandardCharsets.UTF_8);
    }

    private static void appendValue(
            StringBuilder target,
            Object value) {

        if (value == null) {
            target.append("\\N");
            return;
        }

        String text = value.toString();

        if (text.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(
                    "COPY value contains zero byte"
            );
        }

        /*
         * Все ненулевые значения намеренно заключаем в кавычки.
         *
         * Это позволяет безопасно передавать:
         * - TAB
         * - CR/LF
         * - JSON
         * - SQL
         * - строку "\N"
         */
        target.append('"');

        for (int i = 0; i < text.length(); i++) {

            char ch = text.charAt(i);

            if (ch == '"') {
                target.append("\"\"");
            } else {
                target.append(ch);
            }
        }

        target.append('"');
    }
}