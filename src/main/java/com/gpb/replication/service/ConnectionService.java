package com.gpb.replication.service;

import com.gpb.replication.dto.SourceConnection;

public interface ConnectionService {
    SourceConnection getConn(String serviceName);
}
