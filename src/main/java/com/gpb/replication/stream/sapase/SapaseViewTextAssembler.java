package com.gpb.replication.stream.sapase;

/** Assembles ordered syscomments fragments without changing SQL whitespace. */
public final class SapaseViewTextAssembler {

    private final StringBuilder text = new StringBuilder();
    private long previousPosition = -1;
    private boolean hidden;
    private boolean missingText;

    public void append(int high, int low, int status, String fragment) {
        // SYSCOM_TEXT_HIDDEN: obfuscated fragments must never be exposed as SQL.
        if ((status & 1) != 0) {
            hidden = true;
        }
        if (hidden) {
            return;
        }
        if (high < 0 || high > 32767 || low < 0 || low > 32767) {
            throw new IllegalStateException("Invalid SAP ASE syscomments position");
        }
        long position = (long) high * 32768 + low;
        if ((previousPosition < 0 && position > 1)
                || (previousPosition >= 0 && position != previousPosition + 1)) {
            throw new IllegalStateException(
                    "Missing or duplicate SAP ASE syscomments fragment at " + position
            );
        }
        previousPosition = position;
        if (fragment == null) {
            missingText = true;
        } else {
            text.append(fragment);
        }
    }

    public String definition() {
        return previousPosition >= 0 && !hidden && !missingText
                ? text.toString() : null;
    }
}
