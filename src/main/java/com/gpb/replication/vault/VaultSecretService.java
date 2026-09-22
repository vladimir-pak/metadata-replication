package com.gpb.replication.vault;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.vault.VaultException;
import org.springframework.vault.core.VaultKeyValueOperations;
import org.springframework.vault.core.VaultKeyValueOperationsSupport.KeyValueBackend;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultResponseSupport;

import com.gpb.replication.dto.SourceConnection;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class VaultSecretService {
    
    private final VaultTemplate vaultTemplate;
    private final VaultKeyValueOperations kvOperations;
    private final String connectionPath;
    
    public VaultSecretService(
            VaultTemplate vaultTemplate,
            @Value("${spring.cloud.vault.kv.backend:secret}") String backend,
            @Value("${replication.vault.connection-path:ord/src/connections}")
            String connectionPath) {

        this.vaultTemplate = vaultTemplate;
        this.kvOperations = vaultTemplate.opsForKeyValue(
                backend,
                KeyValueBackend.KV_2
        );
        this.connectionPath = normalizePath(connectionPath);
    }
    
    /**
     * Получить все секреты для конкретного сервиса
     * @param serviceName имя сервиса (добавляется к базовому пути)
     * @return Map с секретами или empty Map если секрет не найден
     */
    public Optional<SourceConnection> getServiceSecrets(String serviceName) {
        validateServiceName(serviceName);

        String path = buildPath(serviceName);

        log.debug("Reading source DB connection from Vault path: {}", path);

        try {
            VaultResponseSupport<SourceConnection> response =
                    kvOperations.get(path, SourceConnection.class);

            if (response == null || response.getData() == null) {
                log.warn(
                        "Source DB connection not found in Vault: {}",
                        serviceName
                );

                return Optional.empty();
            }

            return Optional.of(response.getData());

        } catch (VaultException e) {
            throw new VaultException(
                    "Failed to read source DB connection from Vault: "
                            + serviceName,
                    e
            );
        }
    }
    
    
    /**
     * Проверить существование секретов для сервиса
     * @param serviceName имя сервиса
     * @return true если секреты существуют
     */
    public boolean serviceSecretsExist(String serviceName) {
        return getServiceSecrets(serviceName).isPresent();
    }
    
    private String buildPath(String serviceName) {
        return connectionPath + "/" + serviceName;
    }

    private static void validateServiceName(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException(
                    "serviceName must not be blank"
            );
        }

        if (serviceName.contains("/")) {
            throw new IllegalArgumentException(
                    "serviceName must not contain '/'"
            );
        }
    }

    private static String normalizePath(String path) {

        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(
                    "Vault connection path must not be blank"
            );
        }

        return path
                .replaceAll("^/+", "")
                .replaceAll("/+$", "");
    }

    /**
     * Простая проверка подключения к Vault
     * @return true если подключение активно, false если нет
     */
    public boolean isVaultConnected() {
        try {
            vaultTemplate.opsForSys().health();

            log.debug("Vault connection check: SUCCESS");
            return true;

        } catch (VaultException e) {
            log.warn(
                    "Vault connection check: FAILED: {}",
                    e.getMessage()
            );

            return false;
        }
    }
}

