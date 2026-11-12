package com.gpb.replication.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter 
@Setter 
public class SourceConnection implements Serializable {

    private static final String UNKNOWN_HOST = "unknown-host";
    private static final Pattern ORACLE_HOST = Pattern.compile(
            "\\(\\s*HOST\\s*=\\s*(\"[^\"]*\"|'[^']*'|[^()]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TCP_OPTIONS = Pattern.compile(
            "(?:^|,)\\s*TCP(?:IP)?\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern HOST_NAME = Pattern.compile("[\\p{L}\\p{N}_.-]+");
    private static final Pattern IPV6 = Pattern.compile(
            "[0-9a-fA-F:.]+(?:%[a-zA-Z0-9_.-]+)?");

    private String name;

    @JsonProperty ("service_name")
    private String serviceName;

    @JsonProperty("db_type")
    private String dbType;

    @JsonFormat(
        with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY
    )
    private List<String> url;

    private String username;

    @ToString.Exclude
    private String password;

    /**
     * Returns the first explicitly specified host in the primary JDBC URL.
     * IPv6 brackets are removed. No DNS, TNS alias or DSN lookup is performed.
     * The result does not identify which node the JDBC driver actually connects to.
     */
    @JsonIgnore
    public String getHostFromUrl() {
        String primaryUrl = getPrimaryUrl();
        if (primaryUrl == null || primaryUrl.isBlank()) {
            return UNKNOWN_HOST;
        }

        String jdbcUrl = primaryUrl.trim();
        String lower = jdbcUrl.toLowerCase(Locale.ROOT);
        if (lower.startsWith("jdbc:oracle:thin:")
                || lower.startsWith("jdbc:oracle:oci:")
                || lower.startsWith("jdbc:oracle:oci8:")) {
            int at = jdbcUrl.indexOf('@');
            return at < 0 ? UNKNOWN_HOST : oracleHost(jdbcUrl.substring(at + 1).trim());
        }

        // jConnect is used by both SAP ASE and SAP IQ; it does not require // or @.
        if (lower.startsWith("jdbc:sybase:tds:")) {
            String address = jdbcUrl.substring("jdbc:sybase:tds:".length());
            return hostFromAddress(address.startsWith("//") ? address.substring(2) : address, false);
        }

        if (lower.startsWith("jdbc:sqlanywhere:") || lower.startsWith("jdbc:ianywhere:")) {
            String properties = jdbcUrl.substring(jdbcUrl.indexOf(':', "jdbc:".length()) + 1);
            String host = connectionProperty(properties, false, "Host");
            if (host != null) {
                return hostFromAddress(host, true);
            }
            String links = connectionProperty(properties, false, "CommLinks", "LINKS");
            if (links != null) {
                Matcher tcp = TCP_OPTIONS.matcher(links);
                if (tcp.find()) {
                    return hostFromAddress(connectionProperty(tcp.group(1), false, "Host", "IP"), true);
                }
            }
            return UNKNOWN_HOST;
        }

        String[] prefixes = {"jdbc:postgresql://", "jdbc:sqlserver://",
                "jdbc:microsoft:sqlserver://", "jdbc:jtds:sqlserver://", "jdbc:jtds:sybase://"};
        for (String prefix : prefixes) {
            if (lower.startsWith(prefix)) {
                String address = jdbcUrl.substring(prefix.length());
                if (prefix.equals("jdbc:sqlserver://") || prefix.equals("jdbc:microsoft:sqlserver://")) {
                    int propertiesStart = address.indexOf(';');
                    String server = propertiesStart < 0 ? null
                            : connectionProperty(address.substring(propertiesStart + 1), true, "serverName");
                    if (server != null) {
                        return hostFromAddress(server, true);
                    }
                }
                return hostFromAddress(address, false);
            }
        }
        return UNKNOWN_HOST;
    }

    private static String oracleHost(String address) {
        if (address.startsWith("(")) {
            Matcher host = ORACLE_HOST.matcher(address);
            return host.find() ? hostFromAddress(unquote(host.group(1).trim()), true) : UNKNOWN_HOST;
        }
        int query = address.indexOf('?');
        if (query >= 0) {
            address = address.substring(0, query);
        }
        String lower = address.toLowerCase(Locale.ROOT);
        if (lower.startsWith("tcp:") || lower.startsWith("tcps:")) {
            address = address.substring(address.indexOf(':') + 1);
        } else if (lower.startsWith("ldap:") || lower.startsWith("ldaps:") || lower.contains("://")) {
            // A directory service address is not the database hostname.
            return UNKNOWN_HOST;
        } else if (!address.startsWith("//") && address.indexOf(':') < 0 && address.indexOf('/') < 0) {
            // @MY_TNS_ALIAS does not contain a hostname.
            return UNKNOWN_HOST;
        }
        return hostFromAddress(address.startsWith("//") ? address.substring(2) : address, false);
    }

    private static String hostFromAddress(String address, boolean allowBareIpv6) {
        if (address == null || address.isBlank()) {
            return UNKNOWN_HOST;
        }
        String value = address.trim();
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            if (end < 0 || (end + 1 < value.length() && ":/,;?\\#".indexOf(value.charAt(end + 1)) < 0)) {
                return UNKNOWN_HOST;
            }
            String host = value.substring(1, end);
            return isIpv6Literal(host) ? host : UNKNOWN_HOST;
        }

        int end = 0;
        while (end < value.length() && "/,;?\\#".indexOf(value.charAt(end)) < 0) {
            end++;
        }
        String endpoint = value.substring(0, end).trim();
        if (endpoint.indexOf('@') >= 0) {
            return UNKNOWN_HOST;
        }
        if (allowBareIpv6 && isIpv6Literal(endpoint)) {
            return endpoint;
        }
        int colon = endpoint.indexOf(':');
        String host = colon < 0 ? endpoint : endpoint.substring(0, colon);
        return HOST_NAME.matcher(host).matches() ? host : UNKNOWN_HOST;
    }

    private static boolean isIpv6Literal(String value) {
        return value.indexOf(':') != value.lastIndexOf(':') && IPV6.matcher(value).matches();
    }

    /** Reads top-level properties, skipping delimiters inside quoted values and TCPIP(...). */
    private static String connectionProperty(String properties, boolean sqlServer, String... names) {
        String result = null;
        int start = 0;
        while (start < properties.length()) {
            while (start < properties.length()
                    && (properties.charAt(start) == ';' || Character.isWhitespace(properties.charAt(start)))) {
                start++;
            }
            int equals = properties.indexOf('=', start);
            int separator = properties.indexOf(';', start);
            if (equals < 0) break;
            if (separator >= 0 && separator < equals) {
                start = separator + 1;
                continue;
            }
            String key = properties.substring(start, equals).trim();
            int valueStart = equals + 1;
            while (valueStart < properties.length() && Character.isWhitespace(properties.charAt(valueStart))) {
                valueStart++;
            }
            int end = valueStart;
            char quote = 0;
            if (end < properties.length()) {
                char first = properties.charAt(end);
                if (sqlServer && first == '{') quote = '}';
                if (!sqlServer && (first == '\'' || first == '"')) quote = first;
                if (quote != 0) end++;
            }
            int depth = 0;
            while (end < properties.length()) {
                char c = properties.charAt(end);
                if (quote != 0) {
                    if (c == quote) {
                        if (end + 1 < properties.length() && properties.charAt(end + 1) == quote) end++;
                        else quote = 0;
                    }
                } else if (!sqlServer && c == '(') {
                    depth++;
                } else if (!sqlServer && c == ')' && depth > 0) {
                    depth--;
                } else if (c == ';' && depth == 0) {
                    break;
                }
                end++;
            }
            if (quote != 0 || depth != 0) return null;
            for (String name : names) {
                if (name.equalsIgnoreCase(key)) result = unquote(properties.substring(valueStart, end).trim());
            }
            start = end + 1;
        }
        return result;
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')
                    || (first == '{' && last == '}')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    @JsonIgnore
    public int getPortFromUrl() {
        if (url == null) return -1;
        try {
            String urlFirst = url.get(0);
            Pattern pattern = Pattern.compile(":(\\d+)");
            Matcher matcher = pattern.matcher(urlFirst);
            return matcher.find() ? Integer.parseInt(matcher.group(1)) : 1521;
        } catch (Exception e) {
            return 1521;
        }
    }

    @JsonIgnore
    public String getPrimaryUrl() {

        if (url == null || url.isEmpty()) {
            return null;
        }

        return url.get(0);
    }
}
