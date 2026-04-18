package com.hackthon.stanford.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * 确认请求是否进入内嵌 Tomcat（若此处无日志，说明 8080 上不是本应用或请求未到达 JVM）。
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HttpRequestLogFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        log.info("HTTP <-- {} {} remote={} contentLength={}",
                request.getMethod(),
                request.getRequestURI(),
                request.getRemoteAddr(),
                request.getContentLengthLong());
        try {
            filterChain.doFilter(request, response);
        } finally {
            log.info("HTTP --> {} {} status={}",
                    request.getMethod(),
                    request.getRequestURI(),
                    response.getStatus());
        }
    }
}
