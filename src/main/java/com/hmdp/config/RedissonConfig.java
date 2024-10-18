package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author yanghe
 * @create 2024-09-08-21:23
 * @description: TODO
 */
@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient(){
        // 配置
        Config config = new Config();
        config.useSingleServer().
                setAddress("redis://192.168.131.100:6379").setPassword("662524");
        // 创建redissonClient 对象
        return Redisson.create(config);
    }
}
