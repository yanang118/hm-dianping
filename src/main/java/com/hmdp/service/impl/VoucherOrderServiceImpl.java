package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 杨佳佳
 * @since 2024-9-1
 * */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private SeckillVoucherServiceImpl seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    IVoucherOrderService proxy;

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }
    private class VoucherOrderHandle implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 1.从stream消息队列获取订单，XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断消息是否为空
                    if (records == null ||records.isEmpty()){
                        continue;
                    }
                    // 3.解析数据,count=1，故而records只有1个元素
                    MapRecord<String, Object, Object> record = records.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.处理订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ack
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
                } catch (Exception e) {
                    log.error("消息队列处理异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true){
                try {
                    // 1.从stream消息队列获取订单，XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 2.判断pendinglist是否为空,空代表没有未处理的消息，直接break
                    if (records == null ||records.isEmpty()){
                        break;
                    }
                    // 3.解析数据,count=1，故而records只有1个元素
                    MapRecord<String, Object, Object> record = records.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.处理订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ack
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
                } catch (Exception e) {
                    log.error("pendinglist处理异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }
//    private BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1024*1024);
//    private class VoucherOrderHandle implements Runnable{
//
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    // 1.从阻塞队列获取订单
//                    VoucherOrder voucherOrder = blockingQueue.take();
//                    // 2.处理订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("阻塞队列处理异常",e);
//                }
//            }
//        }
//    }
    private void handleVoucherOrder(VoucherOrder voucherOrder){
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        // 2.创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 3.尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 4.判断是否获得锁成功
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }
        try {
            //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        // 2.执行结果判断，返回1，2 则失败
        int r = result.intValue();
        if (r!=0) {
            return Result.fail(r==1?"库存不足":"不可重复下单");
        }
        // 作为主线程返回代理对象给其他线程
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回订单id
        return Result.ok(orderId);
    }
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠卷
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断活动是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("活动尚未开始！");
//        }
//        // 3.判断活动是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("活动已结束！");
//        }
//        // 4.判断库存是否充足
//        if (voucher.getStock()<1) {
//            return Result.fail("库存不足！");
//        }
//        Long id = UserHolder.getUser().getId();
//
//        // 创建锁,以"order:"+id作为key,每个业务下的每个用户对应一个锁
//        //SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate,"order:"+id);
//        RLock lock = redissonClient.getLock("lock:order:" + id);
//        // 获取锁
//        boolean success = lock.tryLock();
//        if (!success) {
//            return Result.fail("不可重复下单");
//        }
//        try {
//            // 获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单逻辑
        Long id = voucherOrder.getUserId();

        int count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count>0) {
            log.error("不可重复购买！");
            return ;
        }
        // 5.扣减库存
        boolean success = seckillVoucherService.update().
                setSql("stock = stock -1").
                eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).
                update();
        if (!success) {
            log.error("库存不足！");
            return ;
        }
        save(voucherOrder);
    }
}
