package com.gpb.replication.cef;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.stereotype.Component;

import com.gpb.replication.cef.enums.SvoiSeverityEnum;
import com.gpb.replication.cef.model.Log;
import com.gpb.replication.cef.model.SvoiJournal;
import com.gpb.replication.cef.properties.LogsDatabaseProperties;
import com.gpb.replication.cef.properties.SysProperties;
import com.gpb.replication.cef.repository.LogRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Класс - логгер по стандартам СВОИ (CEF)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SvoiLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private final DataSourceProperties dataSourceProperties;

    public void sendInternal(String deviceEventClassID,
                             String name,
                             String message,
                             SvoiSeverityEnum severity) {

        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        send(deviceEventClassID, name, message, severity, journal);
    }

    public void sendKafkaMessage(
        String message, 
        String user,
        String ip,
        String dns,
        int port
    ) {
        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        String username = dataSourceProperties.getUsername();
        journal.setDhost(dns);
        journal.setDst(null);
        journal.setDuser(username);
        journal.setDpt(port);
        send("kafkaConsumer", "Consumer connected to Kafka", message, SvoiSeverityEnum.ONE, journal);
    }

    public void sendDbConnection(String message) {
        String url = dataSourceProperties.getUrl();
        
        String host;
        int port;
        if (url == null || url.isBlank()) {
            host = "UNDEFINED";
            port = -1;
        } else {
            String normalizedUrl = url;
            if (normalizedUrl.startsWith("jdbc:")) {
                normalizedUrl = normalizedUrl.substring("jdbc:".length());
            }
            
            URI uri = URI.create(normalizedUrl);

            host = uri.getHost();
            port = uri.getPort();
        }

        String username = dataSourceProperties.getUsername();

        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        journal.setDhost(host);
        journal.setDuser(username);
        journal.setDpt(port);
        send("dbConnection", "Database Connection", message, SvoiSeverityEnum.ONE, journal);
    }

    public void logConnectToSource(String sourceHost,
                                   int sourcePort,
                                   String dbType,
                                   String duser) {

        try {
            SvoiJournal journal = prepareJournalBase();
            String dst;
            String dhost;
            if (isIp(sourceHost)) {
                dst = sourceHost;
                dhost = resolveHost(sourceHost);
            } else {
                dst = resolveIp(sourceHost);
                dhost = sourceHost;
            }

            journal.setDhost(dhost);
            journal.setDst(dst);
            journal.setDvchost(dhost);
            journal.setDpt(sourcePort);
            journal.setDuser(duser);
            journal.setApp("JDBC");

            String message = String.format("connectTo%s dns: %s; ip: %s; port: %d;",
                    dbType, dhost, dst, sourcePort);

            send("connectToSource", "Database Connection", message,
                    SvoiSeverityEnum.ONE, journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании подключения к источнику {}", dbType, e);
        }
    }

    /** Ошибка подключения или авторизации к базе источника */
    public void logDbConnectionError(String sourceHost,
                                     int sourcePort,
                                     String dbType,
                                     String duser,
                                     Exception e) {
        try {
            SvoiJournal journal = prepareJournalBase();

            String dst;
            String dhost;
            if (isIp(sourceHost)) {
                dst = sourceHost;
                dhost = resolveHost(sourceHost);
            } else {
                dst = resolveIp(sourceHost);
                dhost = sourceHost;
            }

            journal.setDhost(dhost);
            journal.setDst(dst);
            journal.setDvchost(dhost);
            journal.setDpt(sourcePort);
            journal.setDuser(duser);
            journal.setApp("JDBC");

            String message = String.format(
                    "dbConnectionError connectTo%s user: %s; hostname: %s; port: %d; error: %s;",
                    dbType,
                    duser,
                    dhost,
                    sourcePort,
                    (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
            );

            send("dbConnectionError",
                    "DB Connection Error",
                    message,
                    SvoiSeverityEnum.FIVE,
                    journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании dbConnectionError", ex);
        }
    }

    public void sendApiRequest(
        String endpoint, 
        String message, 
        String user,
        String ip,
        String dns,
        int port
    ) {
        SvoiJournal journal = svoiJournalFactory.getJournalSource();
        journal.setShost(dns);
        journal.setSrc(ip);
        journal.setSuser(user);
        journal.setSpt(port);
        message += " " + endpoint;
        send("apiCall", "API Request", message, SvoiSeverityEnum.ONE, journal);
    }

    private SvoiJournal prepareJournalBase() {
        SvoiJournal journal = svoiJournalFactory.getJournalSource();

        HostInfo host = getLocalHostInfo();

        journal.setSrc(host.ip());
        journal.setShost(host.name());
        journal.setDhost(host.name());
        journal.setDst(host.ip());
        journal.setDvchost(host.name());
        journal.setDpt(sysProperties.getDpt());

        return journal;
    }

    private void send(String deviceEventClassID,
                      String name,
                      String message,
                      SvoiSeverityEnum severity,
                      SvoiJournal journal) {

        defaultArgs(journal, deviceEventClassID, name, message, severity);

        try (MDC.MDCCloseable a = MDC.putCloseable("host", journal.getHostForSvoi());
             MDC.MDCCloseable b = MDC.putCloseable("log_type", "audit_log")) {

            log.info(journalToString(journal));

        }
        if (!logsDatabaseProperties.isEnabled()) return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), FORMATTER);

            logRepository.save(new Log(
                    created,
                    journalToString(journal),
                    deviceEventClassID
            ));
        } catch (Exception e) {
            log.error("Ошибка при сохранении лога в БД", e);
        }
    }

    private void defaultArgs(SvoiJournal journal,
            String deviceEventClassID,
            String name,
            String message,
            SvoiSeverityEnum severity) {

        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        if (journal.getDuser() == null) {
            journal.setDuser(sysProperties.getUser());
        }
        if (journal.getSuser() == null) {
            journal.setSuser(sysProperties.getUser());
        }
        if (journal.getApp() == null) {
            journal.setApp("TCP");
        }
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);
        if (journal.getSpt() == null) {
            journal.setSpt(sysProperties.getDpt());
        }
        if (journal.getDpt() == null) {
            journal.setDpt(sysProperties.getDpt());
        }
    }

    private String journalToString(SvoiJournal journal) {
        return journal.toString();
    }

    private record HostInfo(String name, String ip) {}

    private HostInfo getLocalHostInfo() {
        try {
            InetAddress inet = InetAddress.getLocalHost();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        } catch (Exception e) {
            InetAddress inet = InetAddress.getLoopbackAddress();
            return new HostInfo(inet.getHostName(), inet.getHostAddress());
        }
    }

    private boolean isIp(String input) {
        if (input == null) return false;
        return input.contains(".") || input.contains(":");
    }

    private String resolveHost(String input) {
        try {
            return InetAddress.getByName(input).getHostName();
        } catch (Exception e) {
            return "UnableToResolve:" + input;
        }
    }

    private String resolveIp(String input) {
        try {
            return InetAddress.getByName(input).getHostAddress();
        } catch (Exception e) {
            return "UnableToResolve:" + input;
        }
    }

    private String getMacAddress() {
        List<String> macs = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                byte[] mac = interfaces.nextElement().getHardwareAddress();
                if (mac == null) continue;
                for (byte b : mac) macs.add(String.format("%02X", b));
            }
        } catch (Exception ignored) {
        }
        return String.join(":", macs);
    }
}
