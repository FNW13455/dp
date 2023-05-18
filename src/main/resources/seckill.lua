--lua脚本实现redis秒杀资格判断
--1、参数列表
--1.1 优惠券id
local voucherId = ARGV[1]
--1.2 用户id
local userId = ARGV[2]
--1.3 订单id
local orderId = ARGV[3]

--2.数据key
--2.1 库存key 这里需要注意key一定写对
local stockKey = 'seckill:stock:' .. voucherId
--2.2 订单key 该key是一个set保存下单了该优惠券的用户Id
local orderKey = "seckill:order:" .. voucherId

--3. 脚本业务
--3.1 判断库存是否充足
if(tonumber(redis.call('get',stockKey)) <= 0) then
    --3.2 库存不足返回1
    return 1
end
--3.2 库存充足 判断用户是否下单
if(redis.call('sismember',orderKey,userId) == 1) then
    --3.3 存在 说明该用户已经下过单 返回2
    return 2
end
--3.4 扣库存
redis.call('incrby',stockKey,-1)
--3.5 下单（保存用户）
redis.call('sadd',orderKey,userId)
--3.6 有下单资格 发送消息到队列中 XADD stream.orders * k1 v1 k2 v2  *是消息的id 不确定 由Redis生成
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0