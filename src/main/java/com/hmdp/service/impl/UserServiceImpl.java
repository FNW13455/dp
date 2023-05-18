package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.BitFieldArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 发送短信验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {

        //1、校验手机号码 用到正则表达式的校验工具类
        if(RegexUtils.isPhoneInvalid(phone)){
            //2、不符合，返回错误信息
            return Result.fail("手机号码格式不正确");
        }
        //3、符合，生成验证码 利用hutool工具类生成6位随机数
        String code = RandomUtil.randomNumbers(6);
        //4、保存验证码到session Redis
        //session.setAttribute("code",code);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5、发送验证码
        log.info("发送验证码成功，验证码{}",code);
        //6、返回ok
        return Result.ok();
    }

    /**
     * 实现登录功能
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1、校验手机号和验证码 每个请求单独校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号码格式错误");
        }
        //2、校验验证码 获取session中的验证码以及表单中的验证码看二者是否一致 从Redis中获取
        //Object cacheCode = session.getAttribute("code");
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.equals(code)){
            //3、不一致 报错
            return Result.fail("验证码错误");
        }
        //4、一致，根据手机号查询用户
        User user = query().eq("phone",phone).one();
        //5、判断用户是否存在
        if(user == null){
            //6、不存在，创建新用户并且保存
            user = createUserWithPhone(phone);
        }

        //7、保存用户信息到session中 保存到Redis中 声明token 将User对象转为hash存储 返回token
        String token = UUID.randomUUID().toString(true);
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        //把UserDTO对象转换位map以便存储到Redis中，
        // 因为用到的是stringRedisTemplate键值默认都是String后面的操作是将UserDto中的对象中所有属性住全部转化String类型
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                                    .setFieldValueEditor((fieldName,fieldValue)-> fieldValue.toString()));

        String tokenKey = "login:token:" + token;
        //存储对象
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //设置有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.SECONDS);
        //返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //判断当前时间是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //写入redis
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //获取日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //判断当前时间是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //获取本月截止今天为止的签到记录 返回10进制数字
        List<Long> result = stringRedisTemplate.opsForValue().
                bitField(key,
                        BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                                .valueAt(0));
        if(result == null || result.isEmpty()){
            //无签到结果
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num == 0){
            return Result.ok(0);
        }
        int count = 0;
        while(true){
            if((num & 1) == 0){
                break;
            }else{
                count++;

            }
            num >>>= 1;
        }
        return Result.ok(count);
    }

    /**
     * 根据手机号创建用户 只需要给手机号和随机昵称
     * @param phone
     * @return
     */
    private User createUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
