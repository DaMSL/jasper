package edu.jhu.cs.damsl.utils.hw2;

import java.lang.annotation.*;

public class HW2 {
  @Documented
  public @interface CS316Todo {
    int exercise() default 1;
    String methods () default "";
  }

  @Documented
  public @interface CS416Todo {
    int exercise() default 1;
    String methods () default "";
  }
}