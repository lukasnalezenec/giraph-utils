package com.nalezen.gcmonitoring;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;


/**
 * Installs Garbage collector event that increments counter for each collection.
 * Works both in Giraph and MapReduce for Java version > 7u4
 * In Giraph Call this in WorkerContext.preApplication().
 *
 * Based on blogpost by Jack Shirazi
 *
 * */
public class GCMonitoring {

  private static final Logger LOG = Logger.getLogger(GCMonitoring.class);

  private static boolean handlerInstalled = false;
  private static final String COUNTER_GROUP = "Garbage collections";

  private static int longPauses;
  private long longPauseLimit = 1000;

  protected GCMonitoring() {}


  /**
   * Installs Garbage collector event that increments counter for each collection.
   * */
  public static void install(final TaskInputOutputContext context) {

    if (handlerInstalled) {
      LOG.debug("GC handler already installed");
      return;
    }

    synchronized (GCMonitoring.class) {

      if (handlerInstalled) {
        return;
      }

      LOG.debug("installing GC handler");

      handlerInstalled = true;
      GCMonitoring instance = new GCMonitoring();

      List<GarbageCollectorMXBean> gcbeans =
        java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();

      for (GarbageCollectorMXBean gcbean : gcbeans) {
        LOG.debug("Registering to GC bean:" + gcbean);
        NotificationEmitter emitter = (NotificationEmitter) gcbean;

        NotificationListener listener = makeListener(instance, context);

        emitter.addNotificationListener(listener, null, null);
      }
    }
  }

  private static NotificationListener makeListener(
          final GCMonitoring instance,
          final TaskInputOutputContext context) {
    return new NotificationListener() {

      @Override
      public void handleNotification(
              Notification notification, Object handback) {

        String type =
          GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION;

        if (notification.getType().equals(type)) {
          CompositeData userData = (CompositeData) notification.getUserData();
          GarbageCollectionNotificationInfo info =
                  GarbageCollectionNotificationInfo.from(userData);

          instance.monitorGC(info, context);
        }
      }
    };
  }

  protected void monitorGC(
          GarbageCollectionNotificationInfo info,
          TaskInputOutputContext context) {
    String gctype = info.getGcAction();

    if ("end of minor GC".equals(gctype)) {
      gctype = "Young Gen GC";
    } else if ("end of major GC".equals(gctype)) {
      gctype = "Old Gen GC";
    }

    long duration = info.getGcInfo().getDuration();

    if (duration > longPauseLimit) {
      long durationSec = duration / 1000;
      String durationStr =
              durationSec >= 10 ? ">=10" : Long.toString(durationSec);
      String counterName = "Long GC pause " + durationStr + " sec";
      context.getCounter(COUNTER_GROUP, counterName).increment(1);
      longPauses++;

      LOG.info(MessageFormat.format(" ---[ Long GC pause: ]--- {0} ms {1}-{2}-{3} "
              , duration
              , info.getGcAction()
              , info.getGcCause()
              , info.getGcName()));

      Map<String, MemoryUsage> membefore = info.getGcInfo().getMemoryUsageBeforeGc();
      Map<String, MemoryUsage> mem = info.getGcInfo().getMemoryUsageAfterGc();

      for (Map.Entry<String, MemoryUsage> entry : mem.entrySet()) {
        String name = entry.getKey();
        MemoryUsage memdetail = entry.getValue();

        long memCommitted = memdetail.getCommitted();
        long memMax = memdetail.getMax();
        long memUsed = memdetail.getUsed();
        MemoryUsage before = membefore.get(name);
        long beforepercent = -1;
        long percent = -1;
        if (before.getCommitted() != 0) {
          beforepercent = ((before.getUsed()*1000L) / before.getCommitted());
          percent = ((memUsed*1000L) / before.getCommitted()); //>100% when it gets expanded
        }

        LOG.info(name + (memCommitted==memMax?"(fully expanded)":"(still expandable)") +
                 "used: "+(beforepercent/10)+"."+(beforepercent%10)+"%->"+
                 (percent/10)+"."+(percent%10)+"%("+((memUsed/1048576)+1)+"MB) / ");
      }
    }

    String causeCounterName = gctype + " : " + info.getGcCause();
    String durationCounterName = gctype + " Duration";

    context.getCounter(COUNTER_GROUP, causeCounterName).increment(1);
    context.getCounter(COUNTER_GROUP, durationCounterName).increment(duration);
  }

  public static int getLongPauses() {
    return longPauses;
  }
}
