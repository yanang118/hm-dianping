package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private IFollowService followService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog==null) {
            return Result.fail("笔记不存在!");
        }
        queryBlogUser(blog);
        // 加载blog时需要显示是否已经点赞了，即修改blog的islike字段
        isBlogLiked(blog);
        return Result.ok(blog);
    }



    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result likeBlog(Long id) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2. 判断当前用户是否点赞过
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // 3.点赞过了，再次点赞即为取消
        if (score!=null) {
            // 3.1点赞数-1
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();
            // 3.2从map集合移除
            if (success) {
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }else{
            // 3.1点赞数+1
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.2从map集合add
            if (success) {
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 1.查询blog的点赞用户id,top5
        String key = BLOG_LIKED_KEY+id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        // 2.根据ids解析为User
        if (top5==null||top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //  先将Set<String>变为List<Long>。
        //  map 是一个中间操作，用于将流中的每个元素应用给定的函数，并返回一个新的流。
        //  Long::valueOf即(str -> Long.valueOf(str))
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);

        // 3.根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> userDTOS = userService.query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean success = save(blog);
        if (!success) {
            return Result.fail("保存笔记失败！");
        }
        //3.查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();

        // 4.推送
        for (Follow follow : follows) {
            // 4.1 获取粉丝id，作为key
            Long userId = follow.getUserId();
            String key = FEED_KEY + userId;
            // 4.2 将blogID 存入到 follow的redis信箱
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {

        // 1.获取当前用户的信箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY+ userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.
                opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 2.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 3.解析数据
        // TypedTuple的数据为 “value，score"
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime  = 0;
        int newOffset = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // id
            ids.add(Long.valueOf(typedTuple.getValue()));
            long time  = typedTuple.getScore().longValue();
            if(time == minTime){
                newOffset++;
            }else{
                minTime = time;
                newOffset = 1;
            }
        }
        // 特殊判断：如果本次查询的数据最后的时间minTime 与 上次查询的最小时间max一致的话,newOffset += offset
        newOffset = minTime==max?newOffset+offset:newOffset;
        // 4.根据博客的id(ids)查blog
        String strId = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD (id," + strId + ")").list();

        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlogLiked(blog);
        }

        // 5.数据封装返回
        ScrollResult result = new ScrollResult();
        result.setList(blogs);
        result.setOffset(newOffset);
        result.setMinTime(minTime);

        return Result.ok(result);
    }

    private void isBlogLiked(Blog blog) {
        // 1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 未登录无需查询
            return;
        }
        Long userId = user.getId();
        // 2. 判断当前用户是否点赞过
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        // 3.修改blog islike属性
        blog.setIsLike(score!=null);
    }

}
