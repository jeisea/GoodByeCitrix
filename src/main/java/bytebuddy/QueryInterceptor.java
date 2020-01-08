package bytebuddy;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Origin;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class QueryInterceptor {

  public static TestVal interceptFindBy(
      @Origin Method method,
      @Argument(0) String arg
  ) throws Exception {
    Map<TestKey, TestVal> fakeStore = new HashMap<>();
    TestKey key = new TestKey("hello");
    TestVal val = new TestVal(1L);
    fakeStore.put(key, val);

    String propertyName = method.getName().substring("findBy".length());

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
}
