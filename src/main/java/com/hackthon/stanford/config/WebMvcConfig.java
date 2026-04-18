package com.hackthon.stanford.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    /** Long SSE streams (local agent + DeepChatBI proxy); avoids default ~30s async timeout. */
    private static final long ASYNC_REQUEST_TIMEOUT_MS = 30L * 60L * 1000L;

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(ASYNC_REQUEST_TIMEOUT_MS);
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/dashboard.html")
                .addResourceLocations("classpath:/");
    }
}
