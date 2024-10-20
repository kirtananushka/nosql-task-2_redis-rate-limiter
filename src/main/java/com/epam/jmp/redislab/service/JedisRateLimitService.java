package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.args.ExpiryOption;

import java.time.LocalTime;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class JedisRateLimitService implements RateLimitService {

   private final Set<RateLimitRule> rules;
   private final JedisCluster cluster;

   public JedisRateLimitService(Set<RateLimitRule> rules, JedisCluster cluster) {
      this.rules = rules;
      this.cluster = cluster;
   }

   @Override
   public boolean shouldLimit(Set<RequestDescriptor> descriptors) {
      return descriptors.stream()
            .anyMatch(this::isRateLimitExceeded);
   }

   private boolean isRateLimitExceeded(RequestDescriptor descriptor) {
      return findMatchingRule(descriptor)
            .map(rule -> isRequestLimitExceeded(descriptor, rule))
            .orElse(true);
   }

   private boolean isRequestLimitExceeded(RequestDescriptor descriptor, RateLimitRule rule) {
      long currentRequestNumber = incrementAndGetRequestCount(descriptor, rule.getTimeInterval());
      return rule.hasExceededLimit(currentRequestNumber);
   }

   private Optional<RateLimitRule> findMatchingRule(RequestDescriptor descriptor) {
      return rules.stream()
            .filter(rule -> isDescriptorMatchingRule(descriptor, rule))
            .findFirst();
   }

   private boolean isDescriptorMatchingRule(RequestDescriptor descriptor, RateLimitRule rule) {
      return isFieldMatchingOrUnique(descriptor.getAccountId(), rule.getAccountId())
            && isFieldMatchingOrUnique(descriptor.getClientIp(), rule.getClientIp())
            && isFieldMatchingOrUnique(descriptor.getRequestType(), rule.getRequestType());
   }

   private boolean isFieldMatchingOrUnique(Optional<String> descriptorField, Optional<String> ruleField) {
      return doFieldsMatch(descriptorField, ruleField) || isFieldUniqueWhenRuleEmpty(descriptorField, ruleField);
   }

   private boolean doFieldsMatch(Optional<String> descriptorField, Optional<String> ruleField) {
      return !descriptorField.isPresent() && !ruleField.isPresent()
            || descriptorField.isPresent() && ruleField.isPresent() && descriptorField.equals(ruleField);
   }

   private boolean isFieldUniqueWhenRuleEmpty(Optional<String> descriptorField, Optional<String> ruleField) {
      return ruleField.isPresent() && ruleField.get().isEmpty()
            && descriptorField.isPresent()
            && rules.stream()
            .noneMatch(r -> descriptorField.get().equals(r.getAccountId().orElse(""))
                  || descriptorField.get().equals(r.getClientIp().orElse(""))
                  || descriptorField.get().equals(r.getRequestType().orElse("")));
   }

   private long incrementAndGetRequestCount(RequestDescriptor requestDescriptor, RateLimitTimeInterval timeInterval) {
      String redisKey = generateRedisKey(requestDescriptor, timeInterval);
      long requestCount = cluster.incr(redisKey);
      long keyTtl = TimeUnit.HOURS.toSeconds(1);
      cluster.expire(redisKey, keyTtl, ExpiryOption.NX);
      return requestCount;
   }

   private String generateRedisKey(RequestDescriptor descriptor, RateLimitTimeInterval timeInterval) {
      String accountId = descriptor.getAccountId().orElse("");
      String clientIp = descriptor.getClientIp().orElse("");
      String requestType = descriptor.getRequestType().orElse("");

      return String.format("requestDescriptor:%s:%s:%s:%s_%s",
            accountId, clientIp, requestType, timeInterval.name(), getCurrentTime(timeInterval));
   }

   private Integer getCurrentTime(RateLimitTimeInterval timeInterval) {
      switch (timeInterval) {
         case MINUTE:
            return LocalTime.now().getMinute();
         case HOUR:
            return LocalTime.now().getHour();
         default:
            throw new IllegalArgumentException("Unsupported time interval: " + timeInterval);
      }
   }
}
