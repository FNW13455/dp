package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
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
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    //加载lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //阻塞队列
    //private BlockingQueue<VoucherOrder> ordersTasks = new ArrayBlockingQueue<>(1024*1024);

    private IVoucherOrderService proxy;

    @PostConstruct
    private void init(){
        //此处用于提交下单任务 项目启动后开始执行
        //SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                try{
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams:.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())

                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1获取失败 没消息 下一次循环
                        continue;
                    }
                    //2.2.获取成功 可以下单
                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                }catch(Exception e){
                    //出异常 代表消息没有被确认 需要去pending-list里处理
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try{
                    //1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams:.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息获取是否成功
                    if(list == null || list.isEmpty()){
                        //2.1获取失败 pending-list中没消息 结束循环
                        break;
                    }
                    //2.2.获取成功 可以下单
                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.创建订单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                }catch(Exception e){
                    //出异常 代表消息没有被确认 需要去pending-list里处理
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        //利用阻塞队列 线程池提交任务
        /*@Override
        public void run() {
            while(true){
                try{
                    //1.获取消息队列中的消息
                    VoucherOrder voucherOrder = ordersTasks.take();
                    handleVoucherOrder(voucherOrder);
                    //2.判断消息获取是否成功
                    //2.1获取失败 没消息 下一次循环
                    //3.获取成功红 可以下单
                    //ACK确认
                }catch(Exception e){
                    log.error("处理订单异常", e);
                }
            }
        }*/
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //判断是否在秒杀活动设定时间内
        /*//查询优惠券
        Long voucherId = voucherOrder.getVoucherId();
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始结束
        LocalDateTime now = LocalDateTime.now();
        if(voucher.getBeginTime().isAfter(now)){
            log.error("秒杀尚未开始");
            return;
        }
        if(voucher.getEndTime().isBefore(now)){
            log.error("秒杀已经结束");
            return;
        }*/
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean isLock = lock.tryLock();
        if(!isLock){
            log.error("不允许重复下单！");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户 订单id
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        //2.判断返回是否为0
        int r = result.intValue();
        //2.1 不为0 没有购买资格
        if(r != 0){
            return Result.fail(r == 1? "库存不足" : "不能重复下单");
        }

        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //3.返回订单id
        return Result.ok(orderId);
    }
    /**
     * 使用Redis判断秒杀资格 利用阻塞队列实现下单
     * @param voucherId
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        //2.判断返回是否为0
        int r = result.intValue();
        //2.1 不为0 没有购买资格
        if(r != 0){
            return Result.fail(r == 1? "库存不足" : "不能重复下单");
        }
        //2.2 为0 有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        ordersTasks.add(voucherOrder);
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //3.返回订单id
        return Result.ok(orderId);
    }*/

    /**
     * 利用Redisson获取分布式锁实现下单功能
     *
     * @param voucherOrder
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始结束
        LocalDateTime now = LocalDateTime.now();
        if(voucher.getBeginTime().isAfter(now)){
            return Result.fail("秒杀尚未开始！");
        }
        if(voucher.getEndTime().isBefore(now)){
            return Result.fail("秒杀已经结束！");
        }
        //判断库存是否充足
        if(voucher.getStock()<1){
            return Result.fail("库存不足！");
        }

        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //尝试获取锁
        boolean isLock = lock.tryLock();
        if(!isLock){
            return Result.fail("不允许重复下单！");
        }

        try {
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
        //先加锁再执行开启事务的方法保证不会在锁释放且事务未提交的时候有其他线程进行操作
        //若将锁加在createVoucherOrder的方法上则任意对象调用都会被所住 而在同步代码块里传入对象则只会锁住这个对象保证这个对象只下一单
//        synchronized (userId.toString().intern()) { //必须保证用户id一样锁一样加intern()方法是字符串的规范表示
//                                                    // 否则每调用一次方法创建一个新对象即使id一样转成字符串也是不同的
//            //事务注解加在了createVoucherOrder上，而调用时其实省去了this，是该类的真实对象，spring事务是利用代理对象完成的因此此处会导致
//            //事务失效 需要用代理对象进行方法调用
//            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }
    }*/
    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        //一人一单 判断订单是否存在
        Long userId = UserHolder.getUser().getId();
            //查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                //用户已经购买过
                return Result.fail("用户已购买过该券！");
            }
            //扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                return Result.fail("库存不足！");
            }
            //创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            //返回订单id
            return Result.ok(orderId);
        }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单 判断订单是否存在
        Long userId = voucherOrder.getUserId();
        //查询订单
        long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            //用户已经购买过
            log.error("用户已购买过该券！");
        }
        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)/*eq("stock",voucher.getStock())*/ //where条件 cas
                .update();
        if (!success) {
            log.error("库存不足！");
        }
        //创建订单

        save(voucherOrder);

    }
}
