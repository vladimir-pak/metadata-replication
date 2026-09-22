package com.gpb.replication.cef;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SvoiApiLog {

    /**
     * Наименование функции/действия.
     * Например: "Retrieving latest version cache"
     */
    String functionName();

    /**
     * Username пока заглушка.
     * Позже можно заменить на SecurityContext.
     */
    String username() default "username";
}