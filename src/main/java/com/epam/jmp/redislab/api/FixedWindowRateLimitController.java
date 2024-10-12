package com.epam.jmp.redislab.api;

import com.epam.jmp.redislab.service.RateLimitService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
@RequestMapping("/api/v1/ratelimit/fixedwindow")
public class FixedWindowRateLimitController {

    private final RateLimitService rateLimitService;

    public FixedWindowRateLimitController(RateLimitService rateLimitService) {
        this.rateLimitService = rateLimitService;
    }

    @PostMapping
    public ResponseEntity<String> shouldRateLimit(@RequestBody RateLimitRequest rateLimitRequest) {
        if (!isValid(rateLimitRequest.getDescriptors())) {
            return ResponseEntity.badRequest().body("Invalid request");
        }
        if (rateLimitService.shouldLimit(rateLimitRequest.getDescriptors())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
        }
        return ResponseEntity.ok().build();
    }

    public boolean isValid(Set<RequestDescriptor> descriptors) {
        return descriptors.stream().anyMatch(descriptor ->
              descriptor.getAccountId().isPresent() && !descriptor.getAccountId().get().isEmpty() ||
                    descriptor.getClientIp().isPresent() && !descriptor.getClientIp().get().isEmpty() ||
                    descriptor.getRequestType().isPresent() && !descriptor.getRequestType().get().isEmpty()
        );
    }
}
