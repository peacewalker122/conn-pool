----------------------------- MODULE connection_pool -----------------------------

EXTENDS Naturals, FiniteSets, Sequences

MAX_CONNECTIONS == 5

CLIENTS ==
    {"alice", "bob", "charlie", "dave", "erin", "frank"}

CONNECTIONS ==
    {"satu", "dua", "tiga", "empat", "lima"}


VARIABLES
    idle,
    borrowed,
    waiters

vars == <<idle, borrowed, waiters>>


(*
 * Initial state
 *)
Init ==
    /\ idle = CONNECTIONS
    /\ borrowed = {}
    /\ waiters = <<>>


(*
 * State predicates
 *)

IsBorrowed(client) ==
    \E pair \in borrowed :
        pair[1] = client


IsWaiting(client) ==
    \E i \in 1..Len(waiters) :
        waiters[i] = client


(*
 * client acquires directly when:
 *
 * - a connection is idle
 * - nobody is already waiting
 * - client does not already own a connection
 *)
Acquire ==
    \E client \in CLIENTS :
        \E conn \in idle :
            /\ Len(waiters) = 0
            /\ ~IsBorrowed(client)

            /\ idle' =
                idle \ {conn}

            /\ borrowed' =
                borrowed \cup {<<client, conn>>}

            /\ UNCHANGED waiters


(*
 * No connection available:
 * put client into the FIFO waiting queue.
 *)
Enqueue ==
    \E client \in CLIENTS :
        /\ idle = {}
        /\ ~IsBorrowed(client)
        /\ ~IsWaiting(client)

        /\ waiters' =
            Append(waiters, client)

        /\ UNCHANGED <<idle, borrowed>>


(*
 * Give an available connection to the
 * first client in the waiting queue.
 *)
Grant ==
    \E conn \in idle :
        /\ Len(waiters) > 0

        /\ idle' =
            idle \ {conn}

        /\ borrowed' =
            borrowed \cup {
                <<Head(waiters), conn>>
            }

        /\ waiters' =
            Tail(waiters)


(*
 * Any borrower may release its connection.
 *)
Release ==
    \E pair \in borrowed :
        /\ idle' =
            idle \cup {pair[2]}

        /\ borrowed' =
            borrowed \ {pair}

        /\ UNCHANGED waiters


Next ==
    \/ Acquire
    \/ Enqueue
    \/ Grant
    \/ Release


(*
 * Type invariant
 *)
TypeOK ==
    /\ idle \subseteq CONNECTIONS
    /\ borrowed \subseteq CLIENTS \X CONNECTIONS
    /\ waiters \in Seq(CLIENTS)


(*
 * Safety:
 *
 * A connection cannot belong to
 * multiple borrowers simultaneously.
 *)
NoDoubleBorrow ==
    Cardinality(
        {pair[2] : pair \in borrowed}
    )
    =
    Cardinality(borrowed)


(*
 * More useful than:
 *
 *     Cardinality(CONNECTIONS) <= MAX_CONNECTIONS
 *
 * because that original property is just a constant fact.
 *)
BelowMaxConnections ==
    Cardinality(idle)
    +
    Cardinality(
        {pair[2] : pair \in borrowed}
    )
    <= MAX_CONNECTIONS


(*
 * Liveness / starvation freedom:
 *
 * Every waiting client eventually becomes
 * a borrower.
 *
 *     Waiting(c) ~> Borrowed(c)
 *)
NoStarvation ==
    \A client \in CLIENTS :
        IsWaiting(client)
        ~>
        IsBorrowed(client)


(*
 * Fairness assumptions
 *
 * If Grant remains possible, eventually
 * some Grant occurs.
 *
 * If Release remains possible, eventually
 * some Release occurs.
 *)
Fairness ==
    /\ WF_vars(Grant)
    /\ WF_vars(Release)


Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ Fairness


=============================================================================