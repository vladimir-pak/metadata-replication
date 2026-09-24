package com.gpb.replication.stream;

import java.sql.ResultSet;

@FunctionalInterface
public interface CopyRowSerializer {

    /**
     * @return encoded COPY row or {@code null} when the source row
     *         must be intentionally skipped.
     */
    byte[] serialize(ResultSet rs) throws Exception;
}
