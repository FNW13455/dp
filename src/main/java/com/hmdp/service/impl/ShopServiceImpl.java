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
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.beans.propertyeditors.CurrencyEditor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    /**
     * 添加商铺缓存 前端发送请求携带商户id
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);
        //代替缓存预热
        this.saveShop2Redis(id,2000L);
        //利用互斥锁解决缓存击穿

        //利用逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);
        if(shop == null){
            return Result.fail("店铺信息不存在！");
        }
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBULID_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 逻辑过期解决缓存击穿
     */
    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1、从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3、未命中缓存，则直接返回空
            return null;
        }
        //4、命中缓存则先反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5、判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期 直接返回商铺信息
            return shop;
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
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockey);
                }
            });
        }

        //6.4返回过期的商铺信息
        return shop;

    }

    /**
     * 利用互斥锁解决缓存击穿
     */
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1、从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3、存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否命中空值
        if(shopJson!=null){
            //返回错误信息
            return null;
        }
        //4、不存在即未命中缓存 实现缓存重建 getByid是Mybatisplus中封装的方法因为这个类继承了ServiceImpl
        //4.1尝试获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if(!isLock){
                //4.3失败 休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4 成功，看是否命中缓存
            String shopJson1 = stringRedisTemplate.opsForValue().get(key);

            if(StrUtil.isNotBlank(shopJson1)){
                //存在，直接返回
                Shop shop1 = JSONUtil.toBean(shopJson1, Shop.class);
                return shop1;
            }
            //判断是否命中空值
            if(shopJson1!=null){
                //返回错误信息
                return null;
            }
            //未命中缓存
            shop = getById(id);
            //5、不存在 返回错误 为解决缓存穿透，不存在时将空对象写入缓存中
            if(shop == null){
                //将空值写入Redis
                stringRedisTemplate.opsForValue().set(key,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6、存在 写入Redis 写入时设置有效时间为数据一致性兜底
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7、释放互斥锁
            unlock(lockKey);
        }
        //8、返回
        return shop;
    }

    /**
     * 解决了缓存穿透的查询逻辑
     */
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        //1、从Redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2、判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3、存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否命中空值
        if(shopJson!=null){
            //返回错误信息
            return null;
        }
        //4、不存在 根据id查数据库 getByid是Mybatisplus中封装的方法因为这个类继承了ServiceImpl
        Shop shop = getById(id);
        //5、不存在 返回错误 为解决缓存穿透，不存在时将空对象写入缓存中
        if(shop == null){
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(key,"", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6、存在 写入Redis 写入时设置有效时间为数据一致性兜底
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7、返回
        return shop;
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

    /**
     * 向Redis中写商铺数据
     */
    public void saveShop2Redis(Long id, Long expireSeconds){
        //1、查询商铺数据
        Shop shop = getById(id);
        //2、封装为RedisData
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3、写入Reids
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 更新商铺数据
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("商铺id不能为空！");
        }
        String key = CACHE_SHOP_KEY + id;
        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(key);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null){
            //不需要查询 按原始数据库查询
            Page<Shop> page = query().eq("type_id",typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = SHOP_GEO_KEY + typeId;
        //3.查询redis 按照距离排序 分页 结果：shopId distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        //4.解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();//从0到end的集合
        if(list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        List<Long> shopIds = new ArrayList<>(list.size());
        Map<String,Distance> shopDistance = new HashMap<>(list.size());
        //截取从from到end的结果
        list.stream().skip(from).forEach(o -> {
            String shopId = o.getContent().getName();
            shopIds.add(Long.valueOf(shopId));
            Distance distance = o.getDistance();
            shopDistance.put(shopId,distance);
        });
        //5.根据id查询shop
        String shopIdStr = StrUtil.join(",", shopIds);
        List<Shop> shops = query().in("id", shopIds).last("ORDER BY FIELD(id," + shopIdStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(shopDistance.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
