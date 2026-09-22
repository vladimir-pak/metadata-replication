package com.gpb.replication.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class MetadataHash {

    private MetadataHash() {
    }

    public static String sha256(String... values) {

        try {

            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            for (String value : values) {

                if (value == null) {
                    digest.update((byte) 0);
                } else {
                    digest.update(
                            value.getBytes(StandardCharsets.UTF_8)
                    );
                }

                /*
                 * Разделитель значений.
                 */
                digest.update((byte) 31);
            }

            return HexFormat
                    .of()
                    .formatHex(digest.digest());

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    e
            );
        }
    }
}