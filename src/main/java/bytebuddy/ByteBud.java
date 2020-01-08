package bytebuddy;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

public class ByteBud {

  public static void main(String[] args) {
    ByteBuddyAgent.install();
    new ByteBuddy()
        .redefine(StringLongStateStore.class)
        .method(ElementMatchers.nameStartsWith("findBy"))
        .intercept(MethodDelegation.to(QueryInterceptor.class))
        .make()
        .load(
            StringLongStateStore.class.getClassLoader(),
            ClassReloadingStrategy.fromInstalledAgent());

    StringLongStateStore store = new StringLongStateStore();

    System.out.println(store.findBy("Key", "hello"));
  }

}
