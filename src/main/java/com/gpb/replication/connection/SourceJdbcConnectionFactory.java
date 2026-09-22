package com.gpb.replication.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.UnaryOperator;

import org.springframework.stereotype.Component;

import com.gpb.replication.cef.SvoiLogger;
import com.gpb.replication.dto.SourceConnection;
import com.gpb.replication.exceptions.MetadataReplicationException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor 
public class SourceJdbcConnectionFactory {

    private final SvoiLogger svoiLogger;

    public Connection open(SourceConnection source) {
        return open(source, UnaryOperator.identity());
    }

    public Connection open(
            SourceConnection source,
            UnaryOperator<String> urlTransformer) {

        validate(source);

        SQLException lastException = null;

        for (int i = 0; i < source.getUrl().size(); i++) {

            String configuredUrl = source.getUrl().get(i);

            if (configuredUrl == null
                    || configuredUrl.isBlank()) {
                continue;
            }

            final String jdbcUrl;

            try {
                jdbcUrl = urlTransformer.apply(configuredUrl);
            } catch (RuntimeException e) {
                throw new MetadataReplicationException(
                        "Failed to build source JDBC URL. "
                                + "serviceName="
                                + source.getServiceName()
                                + ", urlIndex="
                                + i,
                        e
                );
            }

            try {

                Connection connection =
                        DriverManager.getConnection(
                                jdbcUrl,
                                source.getUsername(),
                                source.getPassword()
                        );

                log.debug(
                        "Source JDBC connection established: "
                                + "serviceName={}, urlIndex={}",
                        source.getServiceName(),
                        i
                );

                svoiLogger.logConnectToSource(
                    source.getPrimaryUrl(),
                    source.getPortFromUrl(),
                    source.getDbType(),
                    source.getUsername()
                );

                return connection;

            } catch (SQLException e) {

                lastException = e;

                log.warn(
                        "Failed to connect to source DB: "
                                + "serviceName={}, urlIndex={}",
                        source.getServiceName(),
                        i
                );

                svoiLogger.logDbConnectionError(
                    source.getHostFromUrl(),
                    source.getPortFromUrl(),
                    source.getDbType(),
                    source.getUsername(),
                    e
                );
            }
        }

        throw new MetadataReplicationException(
                "Unable to connect to source DB: "
                        + source.getServiceName(),
                lastException
        );
    }

    private void validate(SourceConnection source) {

        if (source == null) {
            throw new MetadataReplicationException(
                    "Source connection configuration is null"
            );
        }

        if (source.getUrl() == null
                || source.getUrl().isEmpty()) {

            throw new MetadataReplicationException(
                    "Source JDBC URL is not configured. serviceName="
                    + source.getServiceName()
            );
        }
    }
}