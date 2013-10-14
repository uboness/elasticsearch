package org.elasticsearch.rest.support;

import org.elasticsearch.rest.RestRequest;

import java.lang.annotation.*;

/**
 *
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Param {

    public enum Type { BOOLEAN, NUMBER, STRING, TIME, ENUM }

    String name();
    Type type();
    String description();
    String defaultsTo();
    String[] options() default {};
}
