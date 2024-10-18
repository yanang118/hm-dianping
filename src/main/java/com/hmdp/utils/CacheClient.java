package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author yanghe
 * @create 2024-09-07-17:27
 * @description: TODO
 */
@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);



    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 方法1：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    //  方法2：将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 封装
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    // 方法3：根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit){
        // 1. 根据KEY从redis查找商户,采用string数据类型
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在redis
        if (StrUtil.isNotBlank(shopJson)) {
            // 存在
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断是否为空字符串
        if (shopJson != null) {
            return null;
        }
        // 3.不存在，则查找数据库
        R r = dbFallback.apply(id);
        // 4. 数据库不存在
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 5.存在，写回Redis
        this.set(key,r,time, unit);
        // 6.返回
        return r;
    }

    // 方法4：根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        // 1. 根据KEY从redis查找商户,采用string数据类型
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在redis
        if (StrUtil.isBlank(shopJson)) {
            // 不存在
            return null;
        }
        // 3.命中，把json对象反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            // 4.1 未过期
            return r;
        }
        // 4.2 过期缓存重建
        // 4.2.1获取互斥锁
        if (trylock(LOCK_SHOP_KEY+id)) {
            // 获取锁成功
            CACHE_REBUILD_EXECUTOR.submit(()->{

                try {
                    // 缓存重建
                    R newR = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(LOCK_SHOP_KEY+id);
                }
            });
        }
        // 6.返回过期的商户信息
        return r;
    }


    // 方法5：根据指定的key查询缓存，并反序列化为指定类型，需要利用互斥锁解决缓存击穿问题
    public <R,ID> R queryWithPassMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        // 1. 根据KEY从redis查找商户,采用string数据类型
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在redis
        if (StrUtil.isNotBlank(shopJson)) {
            // 存在
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断是否为空字符串
        if (shopJson != null) {
            return null;
        }
        // 3.实现缓存重构
        // 3.1 获取锁
        String lockKey = LOCK_SHOP_KEY +id;
        R r = null;
        try {
            boolean islock = trylock(lockKey);
            // 3.2判断锁是否获取成功
            if(!islock){
                // 3.3失败，休眠重试
                Thread.sleep(100);
                return queryWithPassMutex(keyPrefix,id,type,dbFallback,time,unit);
            }
            // 3.4 成功根据id 查数据库

            r = dbFallback.apply(id);
            // 模拟数据重构延迟
//            Thread.sleep(200);
            // 4. 数据库不存在
            if (r == null) {
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 5.存在，写回Redis
            this.set(key,r,time,unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unlock(lockKey);
        }

        // 6.返回
        return r;
    }
    private boolean trylock(String key){
        // 采用setnx,value任意，同时设置过期时间，防止线程意外导致锁无法释放，避免死锁
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // Boolean是一个包装类，采用BooleanUtil的工具类来转化为bool数据类型，可以防止空指针异常。
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        // 手动释放锁
        stringRedisTemplate.delete(key);
    }
}
