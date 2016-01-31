import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * TODO
 * - Shutdown logic - https://groups.google.com/forum/#!topic/lmax-disruptor/URytxjgyYKo
 * - Use countdown latch??
 */
public class DisruptorTest {

  static public class LongEvent
  {
    public long value;
    public CountDownLatch complete;

    public void set(long value)
    {
      this.value = value;
    }

    public void setComplete(CountDownLatch complete) {
      this.complete = complete;
    }
  }

  static public class LongEventFactory implements EventFactory<LongEvent>
  {
    public LongEvent newInstance()
    {
      return new LongEvent();
    }
  }

  static public class LongEventHandler implements EventHandler<LongEvent>, TimeoutHandler
  {
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch)
    {
      System.out.println(String.format("Thread: %s, event: %s, seq %s, endOfBatch %s", Thread.currentThread().getId(), event, sequence, endOfBatch));
      event.complete.countDown();
    }

    @Override
    public void onTimeout(long l) throws Exception {
      System.out.println("TIMEOUT!!!");
    }
  }

  static public class LongEventProducerWithTranslator
  {
    private final RingBuffer<LongEvent> ringBuffer;

    public LongEventProducerWithTranslator(RingBuffer<LongEvent> ringBuffer)
    {
      this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorTwoArg<LongEvent, CountDownLatch, String> TRANSLATOR =
        new EventTranslatorTwoArg<LongEvent, CountDownLatch, String>()
        {
          public void translateTo(LongEvent event, long sequence, CountDownLatch complete, String bb)
          {
            event.set(Long.parseLong(bb));
            event.setComplete(complete);
          }
        };

    public void onData(String data, CountDownLatch complete)
    {
      ringBuffer.publishEvent(TRANSLATOR, complete, data);
    }
  }

  public static void main(String [] args) throws InterruptedException {
    // Executor that will be used to construct new threads for consumers
    Executor executor = Executors.newFixedThreadPool(1);

    // The factory for the event
    LongEventFactory factory = new LongEventFactory();

    // Specify the size of the ring buffer, must be power of 2.
    int bufferSize = 1024;

    // Construct the Disruptor
    Disruptor<LongEvent> disruptor = new Disruptor<>(
        LongEvent::new,
        bufferSize,
        executor,
        ProducerType.SINGLE,
        new TimeoutBlockingWaitStrategy(500, TimeUnit.MILLISECONDS));

    // Connect the handler
    disruptor.handleEventsWith(new LongEventHandler());

    // Start the Disruptor, starts all threads running
    disruptor.start();

    // Get the ring buffer from the Disruptor to be used for publishing.
    RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

    LongEventProducerWithTranslator producer = new LongEventProducerWithTranslator(ringBuffer);

    for (long l = 0; true; l++)
    {
      CountDownLatch complete = new CountDownLatch(1);
      producer.onData(Long.toString(l), complete);
      if (l % 100 == 99) {
        System.out.println("Waiting");
        complete.await();
        System.out.println("Done");
      }
      if (l % 100 == 0) {
        Thread.sleep(1000);
      }
    }
  }

}
