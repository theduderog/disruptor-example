import java.util.concurrent.*;

public class ArrayQueueTest {

  public static void main(String [] args) throws InterruptedException {

    ArrayBlockingQueue<DisruptorTest.LongEvent> q = new ArrayBlockingQueue<>(1024);

    //Setup consumer
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    executorService.submit(() -> {
      DisruptorTest.LongEvent event = null;
      while (true) {
        try {
//          System.out.println("About to poll on size " + q.size());
          event = q.poll(500, TimeUnit.MILLISECONDS);
          if (event == null) {
            System.out.println("TIMED OUT");
          }
          else {
            System.out.println("got event " + event.value);
          }
        } catch (InterruptedException e) {
          System.out.println("Interrupted");
          e.printStackTrace();
        }
      }
    });

    //Producer
    for (long l = 0; true; l++)
    {
      DisruptorTest.LongEvent event = new DisruptorTest.LongEvent();
      event.set(l);
//      System.out.println("Addding event " + l);
      q.put(event);
      if (l % 10 == 0) {
        Thread.sleep(1000);
      }
    }


  }

}
