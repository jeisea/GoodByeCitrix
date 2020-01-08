package bytebuddy;

import net.bytebuddy.implementation.bind.annotation.RuntimeType;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class QueryInterceptor {

  @RuntimeType
  public static Object interceptFindBy(String propertyName, Object arg) {
    Map<TestKey, TestVal> fakeStore = new HashMap<>();
    TestKey key = new TestKey("hello");
    TestVal val = new TestVal(1L);
    fakeStore.put(key, val);

    for (TestKey testKey : fakeStore.keySet()) {
      try {
        Object result = testKey.getClass().getMethod("get" + propertyName).invoke(testKey);
        if (result.equals(arg)) {
          return fakeStore.get(testKey);
        }
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        e.printStackTrace();
      }
    }

    return null;
  }

//  It's unable to delegate to this function when I have the @Origin method in. That would let us get the methodName
//  so we could get spring data jpa syntax findByX instead of having to pass in the property name
//  IDKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK AHHHHHHHHHHHHHHHHHHHHHHHHH
//  @RuntimeType
//  public static Object interceptFindBy(@Origin Method method, Object arg) {
//    Map<TestKey, TestVal> fakeStore = new HashMap<>();
//    TestKey key = new TestKey("hello");
//    TestVal val = new TestVal(1L);
//    fakeStore.put(key, val);
//
//    String propertyName = method.getName().substring("findBy".length());
//
//    for (TestKey testKey : fakeStore.keySet()) {
//      try {
//        Object result = testKey.getClass().getMethod("get" + propertyName).invoke(testKey);
//        if (result.equals(arg)) {
//          return fakeStore.get(testKey);
//        }
//      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
//        e.printStackTrace();
//      }
//    }
//
//    return null;
//  }
}
