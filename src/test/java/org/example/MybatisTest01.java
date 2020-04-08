package org.example;

import org.apache.example.Blog;
import org.apache.example.BlogMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MybatisTest01 {
  public static void main(String[] args) throws Exception {
//    String resource = "mybatis-config.xml";
//    InputStream inputStream = Resources.getResourceAsStream(resource);
//    SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
//    SqlSession sqlSession = sqlSessionFactory.openSession();
//    BlogMapper mapper = sqlSession.getMapper(BlogMapper.class);
//    Blog blog = mapper.selectBlog(101);
////    Blog blog = sqlSession.selectOne("org.apache.example.BlogMapper.selectBlog", 101);
//    System.out.println(blog);
    List<Blog> blogs = new ArrayList<>();
    Blog blog = new Blog();
    Long aa = 2438393L;
    blog.setAge(aa);
    blogs.add(blog);
    Blog blog1 = new Blog();
    blog1.setAge(aa);
    blogs.add(blog1);
    List<Long> list = blogs.stream().map(Blog::getAge).collect(Collectors.toList());
    System.out.println(list.size());



  }
}
