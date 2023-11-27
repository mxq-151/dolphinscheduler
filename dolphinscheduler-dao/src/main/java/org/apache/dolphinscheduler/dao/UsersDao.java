package org.apache.dolphinscheduler.dao;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UsersDao {

    @Autowired
    private UserMapper userMapper;

    public User queryUserbyId(int userId) {
        //get user object from userId
        return userMapper.selectById(userId);
    }
}
