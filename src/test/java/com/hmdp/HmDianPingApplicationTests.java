package com.hmdp;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService executorService = Executors.newFixedThreadPool(300);

    @Test
    void testSaveShop() throws InterruptedException {

        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
    }

    @Test
    void testIdWorker() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("order = " + id);
            }
            countDownLatch.countDown();
        };
        long start = System.currentTimeMillis();
        for (int i = 0; i <300 ; i++) {
            executorService.submit(task);
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end-start));
    }

    @Test
    void loadShopdata(){

        // 1.获取商户集合
        List<Shop> shops = shopService.list();
        // 2.将商户分组
        Map<Long, List<Shop>> shopsGroup = shops.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3.分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : shopsGroup.entrySet()) {
            // 3.1获取key
            Long shopType = entry.getKey();
            String key = SHOP_GEO_KEY + shopType;
            // 3.2获取同一类型的商户
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> geoLocationList = new ArrayList<>(value.size());
            for (Shop shop : value) {
                RedisGeoCommands.GeoLocation<String> location = new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                );
                geoLocationList.add(location);
            }
            // 3.3按批写入redis
            stringRedisTemplate.opsForGeo().add(key,geoLocationList);
        }

    }


}
