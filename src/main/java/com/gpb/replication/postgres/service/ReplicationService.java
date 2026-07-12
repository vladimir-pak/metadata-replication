package com.gpb.replication.postgres.service;

public interface ReplicationService {
   void startReplicationAsync(String serviceName);
   void startReplication(String serviceName);
}
