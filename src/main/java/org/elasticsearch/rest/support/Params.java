package org.elasticsearch.rest.support;

import java.lang.annotation.*;

/**
 *
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Params {

    Param[] value();

}
