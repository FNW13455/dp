package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    @DisplayName("测试向Redis存储商铺数据")
    public void saveData(){
        shopService.saveShop2Redis(1L, 20L);
    }

    private ExecutorService es = Executors.newFixedThreadPool(500);
    @Test
    @DisplayName("测试ID生成")
    public void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = ()->{
            for(int i=0;i<100;i++){
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        for(int i=0;i<300;i++){
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));

    }
    @Test
    void loadShopData(){
        //查店铺信息
        List<Shop> shopList = shopService.list();
        //把店铺分组 按type分组 key为店铺类型 list为该类型下面所有用户
        Map<Long,List<Shop>> map = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> shops = entry.getValue();
            String key = SHOP_GEO_KEY + typeId;
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());

            for (Shop shop : shops) {
                //stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(),shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),new Point(shop.getX(),shop.getY())));
            }

            stringRedisTemplate.opsForGeo().add(key,locations);
        }
    }

    @Test
    void testHyperLogLog(){
        String[] values = new String[1000];
        int j = 0;
        for(int i=0; i<1000000; i++){
            j = i%1000;
            values[j] = "user_" + i;
            if(j == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hl2",values);
            }
        }
        Long hl2 = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("*****************************" +
                "\n"+ hl2 + "\n" + "**************************");
    }

}
