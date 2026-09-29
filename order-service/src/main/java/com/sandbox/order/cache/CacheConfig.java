package com.sandbox.order.cache;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class CacheConfig {

	/** Injected into {@link RedisAvailability} so its tests can move time without sleeping. */
	@Bean
	Clock clock() {
		return Clock.systemUTC();
	}

}
