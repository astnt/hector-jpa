/**
 * 
 */
package com.datastax.hectorjpa.service;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs all writes and deletes in a worker thread each
 * 
 * @author Todd Nine
 * 
 */
public class AsyncInMemoryIndexingService extends InMemoryIndexingService {

  private static final Logger logger = LoggerFactory
      .getLogger(AsyncInMemoryIndexingService.class);

  /**
   * Currently hard coded to allow 1000 pending audits
   */
  private LinkedBlockingQueue<IndexAudit> auditQueue = new LinkedBlockingQueue<IndexAudit>(
      1000);

  /**
   * Currently hard coded to allow 1000 pending audits
   */
  private LinkedBlockingQueue<IndexAudit> deleteQueue = new LinkedBlockingQueue<IndexAudit>(
      1000);

  public AsyncInMemoryIndexingService() {

    Thread audit = new Thread(new AuditThread());
    audit.setDaemon(true);
    audit.start();

    Thread delete = new Thread(new DeleteThread());
    delete.setDaemon(true);
    delete.start();
  }

  @Override
  public void audit(IndexAudit audit) {
    auditQueue.offer(audit);
  }

  @Override
  public void delete(IndexAudit audit) {
    deleteQueue.offer(audit);
  }

  private class AuditThread implements Runnable {

    @Override
    public void run() {
      try {

        while (true) {
          IndexAudit audit = auditQueue.take();

          auditInternal(audit);

        }

      } catch (Throwable t) {
        logger.error("Unable to processess audit from queue", t);
        // swallow and continue
      }

    }

  }

  private class DeleteThread implements Runnable {

    @Override
    public void run() {
      try {

        while (true) {
          IndexAudit audit = auditQueue.take();

          deleteInternal(audit);
        }
      } catch (Throwable t) {
        logger.error("Unable to processess audit from queue", t);
        // swallow and continue
      }

    }

  }

}
