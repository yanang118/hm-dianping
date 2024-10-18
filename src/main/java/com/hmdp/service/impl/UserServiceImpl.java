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
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //  验证手机号
        if (RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("无效手机号");
        }
        //  生成校验码
        String code = RandomUtil.randomNumbers(6);
        // 保存到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        // 发送验证码
        log.debug("发送验证码：{}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("无效手机号");
        }
        // 2.校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();

        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("验证码不正确");
        }
        // 3.查询用户
        User user = query().eq("phone",phone).one(); //mybatis-plus
        // 4.判断用户是否存在
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // 5.用户保存到redis
        // 5.1 保存到redis的key
        String token = UUID.randomUUID().toString();
        // 5.2 将user转为hashmap
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 采用的是StringRedisTemplate，因此存入的map的值应该为string，原先的fieldValue有一个id为long型，应改为string
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create().
                        setIgnoreNullValue(true).
                        setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        // 5.3 存入
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL,TimeUnit.SECONDS);

        // 6.token 返回
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        save(user);
        return user;

    }
    public void generateToken() {

        String[] phoneNumbers = new String[1000];
        String prefix = "1345678";
        for (int i = 0; i < 1000; i++) {
            String suffixPhone = String.valueOf(9000 + i);
            phoneNumbers[i] = prefix+suffixPhone;
        }
        for(String phone : phoneNumbers){
            //一致根据手机号查用户
            User user = query().eq("phone", phone).one();
            if (user == null) {
                user = createUserWithPhone(phone);
            }
            //7.保存用户信息到redis----------------
            //7.1 随机生成Token作为登录令牌
            String token = UUID.randomUUID().toString();

            String filePath = "C:\\Users\\yanang\\Desktop\\hm_dianping\\output.txt";
            String content = token+'\n';
            // C:\Users\yanang\Desktop\hm_dianping
            try (FileWriter fileWriter = new FileWriter(filePath, true);
                 BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                // 写入内容
                bufferedWriter.write(content);
                // 确保内容都已写入文件
                bufferedWriter.flush();
            }  catch (IOException e) {
                throw new RuntimeException(e);
            }
            //7.2 将User对象转为Hash存储
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                    CopyOptions.create().setIgnoreNullValue(true)
                            .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
            //7.3 存储
            stringRedisTemplate.opsForHash().putAll("login:token:"+token,userMap);
            //7.4设置token有效期
            String tokenKey = LOGIN_USER_KEY+token;
            stringRedisTemplate.expire(tokenKey,999999999,TimeUnit.MINUTES);
        }
    }

    @Override
    public Result sign() {
        // 1.user
        Long userId = UserHolder.getUser().getId();
        // 2.date
        LocalDateTime now = LocalDateTime.now();
        // 3.key
        String suffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + suffix;
        // 4.本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.签到
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1.user
        Long userId = UserHolder.getUser().getId();
        // 2.date
        LocalDateTime now = LocalDateTime.now();
        // 3.key
        String suffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + suffix;
        // 4.本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.获取本月迄今为止的签到记录，返回的是一个十进制数long
        //  BITFIELD sign:1010:202409 GET u14 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create().
                        get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (result==null||result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num==null ){
            return Result.ok(0);
        }
        // 6.循环遍历得出连续签到的天数
        int count =0;
        while (true){
            // 6.1 让num 与1 做“与” 运算
            if((num&1)==0){
                break;
            }else{
                count++;
            }
            // 逻辑右移（>>>）,算术右移（>>
            num >>>= 1;
        }
        return Result.ok(count);
    }
}
