package com.tradingplatform.gateway.config;

import java.util.List;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Registers {@link ProtobufHttpMessageConverter} for {@code /protobuf/orders}.
 *
 * <p>Checked directly rather than assumed: having {@code protobuf-java} on the classpath does
 * <em>not</em> auto-register this converter in Spring Boot 3.5.9 — a real {@code MockMvc} request
 * with {@code Content-Type: application/x-protobuf} against an unconfigured context comes back 415
 * (verified via {@code OrderProtobufControllerTest} before this class existed). Spring Web ships the
 * converter itself; it only needs registering.
 */
@Configuration
public class ProtobufConfiguration implements WebMvcConfigurer {

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(new ProtobufHttpMessageConverter());
    }
}
