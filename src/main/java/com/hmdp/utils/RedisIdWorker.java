package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author yanghe
 * @create 2024-09-07-20:05
 * @description: TODO
 */
@Component
public class RedisIdWorker {

    public static final long BEGIN_TIMESTAMP = 1725148800;
    public static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyprefix){
        // 1. 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long second = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = second - BEGIN_TIMESTAMP;

        // 2. 生成序列号
        // 2.1 获取当前日期yyyy:MM:dd
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2 以"icr:" + keyprefix + date，作为key实现自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyprefix + date);

        // 3. 拼接
        return timeStamp<< COUNT_BITS | count;
    }
}
