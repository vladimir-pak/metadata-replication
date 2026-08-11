package com.gpb.replication.postgres.service;

public interface ParsingService {

    void dbReplicate();

    void schemaReplicate();

    void tableReplicate();
}
