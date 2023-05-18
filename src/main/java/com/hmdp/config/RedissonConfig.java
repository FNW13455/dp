package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Ivan Fioon
 * @create 2023/4/1 - 12:36
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){
        //配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.147.129:6379").setPassword("abc123");
        return Redisson.create(config);
    }
}
