--参数列表
--1.1 优惠卷id
local voucherId = ARGV[1]
--1.2 用户id
local userId = ARGV[2]
--1.3 订单id
local orderId = ARGV[3]

-- 2.key
local stockKey = "seckill:stock:"..voucherId
local orderKey = "seckill:order:"..voucherId

-- 3.脚本判断
if (tonumber(redis.call("get", stockKey))<=0) then
    -- 3.1库存不足返回1
    return 1
end

-- 3.2 判断是否下单,sismember orderKey userId
if redis.call("sismember", orderKey, userId) == 1 then
    return 2
end

-- 3.3扣减库存
redis.call("incrby",stockKey,-1)
-- 3.4订单存入set集合
redis.call("sadd",orderKey,userId)
-- 3.5发布到stream
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0