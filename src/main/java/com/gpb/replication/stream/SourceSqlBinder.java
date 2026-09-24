package com.gpb.replication.stream;

import java.sql.PreparedStatement;

@FunctionalInterface
public interface SourceSqlBinder {

    SourceSqlBinder NONE = ps -> {
    };

    void bind(PreparedStatement ps) throws Exception;
}
