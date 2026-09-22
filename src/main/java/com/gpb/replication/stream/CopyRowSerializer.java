package com.gpb.replication.stream;

import java.sql.ResultSet;

@FunctionalInterface
public interface CopyRowSerializer {

    byte[] serialize(ResultSet rs) throws Exception;
}
