package com.gpb.replication.cef;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.security.Principal;

@Aspect
@Component
@RequiredArgsConstructor
public class SvoiApiLogAspect {

    private final SvoiLogger svoiCustomLogger;

    @Before("@annotation(svoiApiLog)")
    public void logApiRequest(SvoiApiLog svoiApiLog) {
        HttpServletRequest request = resolveRequest();

        if (request == null) {
            svoiCustomLogger.sendApiRequest(
                    "UNDEFINED",
                    "Error while parsing HttpServletRequest",
                    "UNDEFINED",
                    "UNDEFINED",
                    "UNDEFINED",
                    -1
            );
            return;
        }

        String endpoint = request.getMethod() + " " + request.getRequestURI();
        String functionName = svoiApiLog.functionName();

        String username = resolveUsername(request);
        String clientIp = resolveClientIp(request);
        String clientHost = request.getRemoteHost();
        int clientPort = request.getRemotePort();

        svoiCustomLogger.sendApiRequest(
                endpoint,
                functionName,
                username,
                clientIp,
                clientHost,
                clientPort
        );
    }

    private HttpServletRequest resolveRequest() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();

        if (attributes == null) {
            return null;
        }

        return attributes.getRequest();
    }

    private String resolveUsername(HttpServletRequest request) {
        Principal principal = request.getUserPrincipal();

        if (principal == null || principal.getName() == null) {
            return "anonymous";
        }

        return principal.getName();
    }

    private String resolveClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");

        if (hasText(xForwardedFor)) {
            /*
             * X-Forwarded-For Для обработки заголовков NGinx
             */
            return xForwardedFor.split(",")[0].trim();
        }

        String xRealIp = request.getHeader("X-Real-IP");

        if (hasText(xRealIp)) {
            return xRealIp.trim();
        }

        String forwarded = request.getHeader("Forwarded");

        if (hasText(forwarded)) {
            String ipFromForwarded = extractIpFromForwardedHeader(forwarded);
            if (hasText(ipFromForwarded)) {
                return ipFromForwarded;
            }
        }

        return request.getRemoteAddr();
    }

    private String extractIpFromForwardedHeader(String forwarded) {
        /*
         * Forwarded: Для обработки заголовков NGinx
         */
        String[] parts = forwarded.split(";");

        for (String part : parts) {
            String trimmed = part.trim();

            if (trimmed.toLowerCase().startsWith("for=")) {
                String value = trimmed.substring(4).trim();

                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }

                if (value.startsWith("[") && value.endsWith("]")) {
                    value = value.substring(1, value.length() - 1);
                }

                return value;
            }
        }

        return null;
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}