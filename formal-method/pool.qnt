module connection_pool {
  pure val MAX_CONNECTIONS = 5
  pure val CLIENTS =
    Set("alice", "bob", "charlie", "dave", "erin", "frank")
  pure val CONNECTIONS = Set("satu", "dua", "tiga", "empat", "lima")

  var idle: Set[str]
  var borrowed: Set[(str, str)]
  var waiters: List[str]

  action init = all {
    idle' = CONNECTIONS,
    borrowed' = Set(),
    waiters' = [],
  }

  def is_borrowed(client: str): bool =
    borrowed.filter(((c, _)) => c == client).size() > 0

  def is_waiting(client: str): bool =
    waiters.select(c => c == client).length() > 0

  action acquire = {
    nondet client = CLIENTS.oneOf()
    nondet conn = idle.oneOf()

    all {
      idle.size() > 0,
      waiters.length() == 0,
      not(is_borrowed(client)),
      idle' = idle.exclude(Set(conn)),
      borrowed' = borrowed.union(Set((client, conn))),
      waiters' = waiters,
    }
  }

  action enqueue = {
    nondet client = CLIENTS.oneOf()

    all {
      idle.size() == 0,
      not(is_borrowed(client)),
      not(is_waiting(client)),
      idle' = idle,
      borrowed' = borrowed,
      waiters' = waiters.append(client),
    }
  }

  action grant = {
    nondet conn = idle.oneOf()

    all {
      idle.size() > 0,
      waiters.length() > 0,
      idle' = idle.exclude(Set(conn)),
      borrowed' = borrowed.union(Set((waiters.head(), conn))),
      waiters' = waiters.tail(),
    }
  }

  action release = {
    nondet pair = borrowed.oneOf()

    all {
      borrowed.size() > 0,
      idle' = idle.union(Set(pair._2)),
      borrowed' = borrowed.exclude(Set(pair)),
      waiters' = waiters,
    }
  }

  action step = any { acquire, enqueue, grant, release }

  val still_bellow_max_conn = CONNECTIONS.size() <= MAX_CONNECTIONS
  val noDoubleBorrow =
    borrowed.map(((_, conn)) => conn).size() == borrowed.size()

  temporal noStarvation =
    CLIENTS.forall( =>
      waiting.contains(c) leadsTo borrowed.contains(c)
    )
}
