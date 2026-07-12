package com.gpb.replication.postgres.service;


import java.util.List;

import com.gpb.replication.postgres.dto.SourceDbConnections;

public interface DbSourcesService {
     List<SourceDbConnections> getDbConnections();
}
