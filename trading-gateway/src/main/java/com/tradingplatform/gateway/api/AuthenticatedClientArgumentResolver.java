package com.tradingplatform.gateway.api;

import com.tradingplatform.gateway.auth.AuthenticatedClient;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/**
 * Lets a controller declare {@code AuthenticatedClient} as a parameter, so no handler can read an
 * account id from the request without the interceptor having established it first.
 */
public class AuthenticatedClientArgumentResolver implements HandlerMethodArgumentResolver {

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return AuthenticatedClient.class.equals(parameter.getParameterType());
    }

    @Override
    public Object resolveArgument(
            MethodParameter parameter,
            ModelAndViewContainer modelAndViewContainer,
            NativeWebRequest request,
            WebDataBinderFactory binderFactory) {
        Object client = request.getAttribute(
                ClientAuthenticationInterceptor.CLIENT_ATTRIBUTE, RequestAttributes.SCOPE_REQUEST);
        if (client == null) {
            throw new IllegalStateException(
                    "handler requires an authenticated client but the path is not intercepted");
        }
        return client;
    }
}
