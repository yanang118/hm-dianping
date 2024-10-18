package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author yang
 * @since 2024
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        // 缓存穿透
         Shop shop1 = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        // 互斥锁解决缓存击穿
        Shop shop2 = cacheClient.queryWithPassMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("商户不存在");
        }
        return Result.ok(shop);

    }
//    // queryWithPassThrough缓存穿透，无效的KEY产生空对象
//    public Shop queryWithPassThrough(Long id){
//        // 1. 根据KEY从redis查找商户,采用string数据类型
//        String key = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在redis
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 存在
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 判断是否为空字符串
//        if (shopJson != null) {
//            return null;
//        }
//        // 3.不存在，则查找数据库
//        Shop shop = getById(id);
//        // 4. 数据库不存在
//        if (shop == null) {
//            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//
//            return null;
//        }
//        // 5.存在，写回Redis
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        // 6.返回
//        return shop;
//    }
//    // queryWithPassMutex,互斥锁解决缓存击穿（热key)
//    public Shop queryWithPassMutex(Long id){
//        // 1. 根据KEY从redis查找商户,采用string数据类型
//        String key = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在redis
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 存在
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        // 判断是否为空字符串
//        if (shopJson != null) {
//            return null;
//        }
//        // 3.实现缓存重构
//        // 3.1 获取锁
//        String lockKey = LOCK_SHOP_KEY +id;
//        Shop shop = null;
//        try {
//            boolean islock = trylock(lockKey);
//            // 3.2判断锁是否获取成功
//            if(!islock){
//                // 3.3失败，休眠重试
//                Thread.sleep(100);
//                queryWithPassMutex(id);
//            }
//            // 3.4 成功根据id 查数据库
//
//            shop = getById(id);
//            // 模拟数据重构延迟
////            Thread.sleep(200);
//            // 4. 数据库不存在
//            if (shop == null) {
//                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//
//                return null;
//            }
//            // 5.存在，写回Redis
//            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            unlock(lockKey);
//        }
//
//        // 6.返回
//        return shop;
//    }
//
//    // queryWithLogicalExpire,逻辑过期解决缓存击穿
//    public Shop queryWithLogicalExpire(Long id){
//        // 1. 根据KEY从redis查找商户,采用string数据类型
//        String key = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在redis
//        if (StrUtil.isBlank(shopJson)) {
//            // 不存在
//            return null;
//        }
//        // 3.命中，把json对象反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        // 4.判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())){
//            // 4.1 未过期
//            return shop;
//        }
//        // 4.2 过期缓存重建
//        // 4.2.1获取互斥锁
//        if (trylock(LOCK_SHOP_KEY+id)) {
//            // 获取锁成功
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//
//                try {
//                    // 缓存重建
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    unlock(LOCK_SHOP_KEY+id);
//                }
//            });
//        }
//        // 6.返回过期的商户信息
//        return shop;
//    }
//
//    // 把包装了逻辑过期时间的shop（redisData）保存到redis中
//    public void saveShop2Redis(Long id,Long expireSecond) throws InterruptedException {
//        // 1.查询商户数据
//        Shop shop = getById(id);
//        // 模拟延迟
//        Thread.sleep(200);
//        // 2.封装逻辑过期数据
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSecond));
//        // 3.写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
//    }
//
//    private boolean trylock(String key){
//        // 采用setnx,value任意，同时设置过期时间，防止线程意外导致锁无法释放，避免死锁
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        // Boolean是一个包装类，采用BooleanUtil的工具类来转化为bool数据类型，可以防止空指针异常。
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unlock(String key){
//        // 手动释放锁
//        stringRedisTemplate.delete(key);
//    }

    @Override
    public Result update(Shop shop) {

        Long id = shop.getId();
        if (id == null) {
            return Result.fail("商户id为空！");
        }
        // 先更新数据库
        updateById(shop);
        // 后删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否根据坐标查询
        if(x==null || y==null){
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = (current)*SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.根据距离查询redis
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        // 4.结果解析
        // 4.1 查询为空
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // 4.2 没有下一页
        if (list.size()<= from){
            return Result.ok(Collections.emptyList());
        }
        // 4.3截取form~end,获取id 和 距离
        List<Long> ids = new ArrayList<>(current);
        Map<String,Distance> distanceMap = new HashMap<>(current);
        list.stream().skip(from).forEach(
                result->{
                    // 保存id到ids
                    ids.add(Long.valueOf(result.getContent().getName()));
                    // 保存距离到distanceMap，key为商户名称
                    String mapKey = result.getContent().getName();
                    Distance distance = result.getDistance();
                    distanceMap.put(mapKey,distance);
                }
        );
        // 5.根据id查询商户，注意排序问题
        String strId = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + strId + ")").list();
        // 6.为每个商户的距离赋值
        for (Shop shop : shops) {
            Distance distance = distanceMap.get(shop.getId().toString());
            shop.setDistance(distance.getValue());
        }

        return Result.ok(shops);
    }
}
