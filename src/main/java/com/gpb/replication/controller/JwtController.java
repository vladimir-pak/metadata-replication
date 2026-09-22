package com.gpb.replication.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gpb.replication.cef.SvoiApiLog;
import com.gpb.replication.dto.RequestJwtDto;
import com.gpb.replication.jwt.JwtUtil;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/token")
@RequiredArgsConstructor
public class JwtController {

    private final JwtUtil jwtUtil;

    @Tag(name = "Get token", description = "Controller for getting token by secret + service")
    @PostMapping("create")
    @SvoiApiLog(functionName = "GeneratingJWT")
    public String generateToken(
            @RequestBody RequestJwtDto dto,
            HttpServletRequest httpServletRequest) {
        return jwtUtil.generateToken(
            dto.getSecret(),
            dto.getService()
        );
    }

    @Tag(name = "Revoke token", description = "Controller for revoking token by service")
    @DeleteMapping("revoke/{service}")
    @SvoiApiLog(functionName = "RevokingJWT")
    public ResponseEntity<String> revokeToken(@PathVariable String service) {
        boolean revoked = jwtUtil.revokeToken(service);

        if (!revoked) {
            return ResponseEntity
                    .notFound()
                    .build();
        }

        return ResponseEntity.ok(
                "Token for service " + service + " revoked"
        );
    }
}
