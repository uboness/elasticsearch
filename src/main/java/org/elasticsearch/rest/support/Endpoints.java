package org.elasticsearch.rest.support;

import org.elasticsearch.rest.RestRequest;

import java.lang.annotation.*;

/**
 *
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Endpoints {

    Endpoint[] value();

}
