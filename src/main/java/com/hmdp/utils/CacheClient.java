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
 * @author Ivan Fioon
 * @create 2023/3/30 - 0:10
 */
@Slf4j
@Component
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //解决缓存穿透的工具函数
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallBack
                                            ,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1、从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3、存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断是否命中空值
        if(json!=null){
            //返回错误信息
            return null;
        }
        //4、不存在 根据id查数据库 getByid是Mybatisplus中封装的方法因为这个类继承了ServiceImpl
        R r = dbFallBack.apply(id);
        //5、不存在 返回错误 为解决缓存穿透，不存在时将空对象写入缓存中
        if(r == null){
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6、存在 写入Redis 写入时设置有效时间为数据一致性兜底
        this.set(key, r, time, unit);
        //7、返回
        return r;
    }

    private static final ExecutorService CACHE_REBULID_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 逻辑过期解决缓存击穿
     */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id, Class<R> type,Function<ID,R> dbFallback
            ,Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1、从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if(StrUtil.isBlank(json)){
            //3、命中但为空字符串，则直接返回空
            return null;
        }
        //4、命中缓存则先反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5、判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期 直接返回商铺信息
            return r;
        }
        //5.2过期需要进行缓存重建
        //6、缓存重建
        String lockey = LOCK_SHOP_KEY + id;
        //6.1获取互斥锁
        boolean isLock = tryLock(lockey);
        //6.2判断获得锁是否成功
        if(isLock){
            //6.3成功 开启独立线程完成缓存重建
            CACHE_REBULID_EXECUTOR.submit(()->{
                try {
                    //查数据库
                    R r1 = dbFallback.apply(id);

                    //写入Redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockey);
                }
            });
        }

        //6.4返回过期的商铺信息
        return r;

    }

    /**
     * 利用Redis中的set nx 设计一个简单的互斥锁 获取锁 释放锁
     */

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
