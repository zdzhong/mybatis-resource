package org.apache.example;

public class Blog {
  private Integer id;

  private String content;

  @Override
  public String toString() {
    return "Blog{" +
      "id=" + id +
      ", content='" + content + '\'' +
      '}';
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }
}
