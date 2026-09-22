package com.gpb.replication.cef.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SvoiOutcomeEnum {
    SUCCESS("success"),
    FAIL("fail");

    private final String value;
}
