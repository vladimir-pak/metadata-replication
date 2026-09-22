package com.gpb.replication.service.impl;

import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.exceptions.ConnectionNotFoundError;
import com.gpb.replication.service.ConnectionService;
import com.gpb.replication.vault.VaultSecretService;

import lombok.RequiredArgsConstructor;

@Service 
@RequiredArgsConstructor 
@ConditionalOnProperty(
        name = "sources.connections.provider",
        havingValue = "vault"
)
public class VaultConnectionImpl implements ConnectionService {

    private final VaultSecretService vaultSecretService;

    public SourceConnection getConn(String serviceName) {
        Optional<SourceConnection> vaultSecret =
                vaultSecretService.getServiceSecrets(serviceName);
                
        if (vaultSecret.isEmpty()) {
            throw new ConnectionNotFoundError(
                    "Connection not found in vault. serviceName="
                            + serviceName
            );
        }

        return vaultSecret.get();
    };
}
