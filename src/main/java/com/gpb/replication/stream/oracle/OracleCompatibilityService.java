package com.gpb.replication.stream.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.gpb.replication.exceptions.MetadataReplicationException;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class OracleCompatibilityService {

    private static final Pattern ORACLE_MAINTAINED_PREDICATE =
            Pattern.compile(
                    "(?i)\\b([A-Za-z_][A-Za-z0-9_$#]*)"
                            + "\\s*\\.\\s*oracle_maintained"
                            + "\\s*=\\s*'N'"
            );

    public OracleCapabilities detect(
            Connection connection) {

        boolean oracleMaintained =
                supportsOracleMaintained(
                        connection
                );

        OracleCapabilities result =
                new OracleCapabilities(
                        oracleMaintained
                );

        log.info(
                "Oracle capabilities detected. "
                        + "oracleMaintained={}",
                result.oracleMaintainedSupported()
        );

        return result;
    }

    private boolean supportsOracleMaintained(
            Connection connection) {

        String sql = """
                SELECT u.oracle_maintained
                FROM dba_users u
                WHERE 1 = 0
                """;

        try (
                PreparedStatement statement =
                        connection.prepareStatement(sql);

                ResultSet ignored =
                        statement.executeQuery()
        ) {

            return true;

        } catch (SQLException e) {

            String message =
                    e.getMessage();

            if (e.getErrorCode() == 904
                    && message != null
                    && message
                            .toUpperCase(java.util.Locale.ROOT)
                            .contains("ORACLE_MAINTAINED")) {

                log.info(
                        "Oracle capability is not supported: "
                                + "DBA_USERS.ORACLE_MAINTAINED"
                );

                return false;
            }

            throw new MetadataReplicationException(
                    "Failed to detect Oracle capability: "
                            + "DBA_USERS.ORACLE_MAINTAINED",
                    e
            );
        }
    }

    public String applySchemaCompatibility(
            String sql,
            OracleCapabilities capabilities,
            String stage) {

        if (capabilities.oracleMaintainedSupported()) {
            return sql;
        }

        return replaceOracleMaintained(
                sql,
                stage
        );
    }

    private String replaceOracleMaintained(
            String sql,
            String stage) {

        Matcher matcher =
                ORACLE_MAINTAINED_PREDICATE.matcher(sql);

        StringBuffer result =
                new StringBuffer(
                        sql.length() + 512
                );

        int replacements = 0;

        while (matcher.find()) {

            String alias =
                    matcher.group(1);

            matcher.appendReplacement(
                    result,
                    Matcher.quoteReplacement(
                            legacySchemaPredicate(alias)
                    )
            );

            replacements++;
        }

        matcher.appendTail(result);

        if (replacements == 0) {

            throw new MetadataReplicationException(
                    "ORACLE_MAINTAINED predicate "
                            + "not found. stage=" + stage
            );
        }

        String effectiveSql =
                result.toString();

        log.debug(
                "Oracle legacy SQL prepared. stage={}\n{}",
                stage,
                effectiveSql
        );

        return effectiveSql;
    }

    private String legacySchemaPredicate(
            String alias) {

        return """
                %s.username NOT IN (
                    'SYS',
                    'SYSTEM',
                    'OUTLN',
                    'DBSNMP',
                    'SYSMAN',
                    'MDSYS',
                    'ORDSYS',
                    'ORDDATA',
                    'CTXSYS',
                    'XDB',
                    'WMSYS',
                    'OLAPSYS',
                    'OWBSYS',
                    'OWBSYS_AUDIT',
                    'APPQOSSYS',
                    'AUDSYS',
                    'GSMADMIN_INTERNAL',
                    'OJVMSYS',
                    'DVF',
                    'DVSYS'
                )
                """.formatted(alias);
    }
}
