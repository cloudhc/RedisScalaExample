import java.util.Calendar

import com.redis._

/**
  * Created by bast on 2016-04-12.
  */
object RedisScalaPractice extends App {
  val TOTAL_OP = 50000
  val clients = new RedisClientPool("localhost", 6379)

  println(s"database name : ${clients.database}")

  // set
  def set(msgs: List[String]) = {
    clients.withClient {
      client => {
        var i = Integer.parseInt(msgs.head)
        msgs.foreach { v =>
          client.set("key-%d".format(i), s"${v} is new value." * 100)
          i += 1
        }
      }
    }
  }

  // get
  def get(idx: Int, last: Int) = {
    clients.withClient {
      client => {
        (idx until last).foreach { i =>
          client.get("key-%d".format(i))
        }
      }
    }
  }

  def makeWorker(smsgs: List[String]): Thread = {
    return new Thread(new Runnable {
      override def run(): Unit = {
        set(smsgs)
      }
    })
  }

  def readWorker(idx: Int, len: Int): Thread = {
    return new Thread(new Runnable {
      override def run(): Unit = {
        get(idx, len)
      }
    })
  }

  val l = (0 until TOTAL_OP).map(_.toString).toList

  // push key lists
  val now = Calendar.getInstance().getTimeInMillis()
  val lists = l.grouped(1000).toList

//  set(l)
//  get(Integer.parseInt(l.head), l.size)
  lists.foreach(makeWorker(_).start())
  lists.foreach { l =>
      val first = Integer.parseInt(l.head)
      val last = first + l.size
      readWorker(first, last).start()
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      val elapsed = Calendar.getInstance().getTimeInMillis() - now

      println(s"process # per second : ${TOTAL_OP / elapsed * 1000f}")
      println(s"elapsed time ${elapsed / 1000f} sec")

      clients.close
    }
  })
}

