package edu.jhu.cs.damsl.utils.hw1;

import java.lang.annotation.*;

public class HW1 {
  @Documented
  public @interface CS316Todo {
    int exercise() default 1;
  }

  @Documented
  public @interface CS416Todo {
    int exercise() default 1;
  }
}