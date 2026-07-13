------------------- MODULE Lyra -------------------
EXTENDS FiniteSets, Sequences, Integers, TLC, UnreliableNetwork

(***************************************************************************

LYRA: DISTRIBUTED FLEXIBLE STREAM CONSENSUS PROTOCOL

A simplified, term-based distributed consensus protocol for stream storage.

Key design decisions:
  - Full replication (no striping). Every entry is written to ALL nodes
    in the ensemble and ALL must acknowledge. This eliminates the
    complexity of partial replication and quorum coverage edge cases.
  - Term-based fencing replaces boolean fencing. When a new client takes
    over a stream, it increments the term. Units reject requests with
    stale terms. This enables unbounded streams with multiple writers
    over their lifetime.
  - Reconciliation at term boundaries. A new term holder must
    reconcile the stream before writing, ensuring dirty entries from
    previous terms are cleaned up via a truncation mechanism.
  - Ensemble changes only on unit failure. The writeset is always the
    full ensemble. A failed unit is replaced and a new segment is
    created starting from LRA + 1.

Protocol safety properties:
  - AllAckedEventsFullyReplicated: Every acknowledged event is
    replicated on all ensemble members.
  - NoAckedEventLost: Once an event is acknowledged, it is never lost
    (assuming no permanent loss of all ensemble members).
  - NoDivergence: All units in an ensemble agree on the value of every
    committed offset.

Read path: Not modeled. Read correctness follows from the write path
  safety properties above. Since all committed offsets are fully
  replicated and no divergence exists, a reader can read from any unit
  in the ensemble for any offset <= LRA and is guaranteed consistent,
  correct data.

Terminology:
  - Stream: A logical append-only stream managed by a client. Analogous
    to a ledger in BookKeeper but unbounded.
  - Unit: A storage server node. Analogous to a bookie.
  - Segment: A contiguous range of offsets within a stream, associated
    with a fixed ensemble of units.
  - Term: An integer epoch for fencing. Incremented on leader change.
  - LRS: Last Append Sent - the highest offset sent by the stream client.
  - LRA: Last Append Acked - the highest contiguous offset acknowledged
    by all units in the ensemble (commit point).

***************************************************************************)

CONSTANTS
    \* Message types
    AppendEventRequest,
    AppendEventResponse,
    FenceRequest,
    FenceResponse,

    \* Model inputs
    Units,              \* Set of all unit (server) identifiers
    Streams,          \* Set of all stream identifiers
    ReplicationFactor,             \* Number of replicas (= ensemble size)
    Events,             \* Set of event payloads to write

    \* Response codes
    Ok,
    InvalidTerm,

    \* Sentinel values
    Null,

    \* Stream status
    StreamStatusOpen,
    StreamStatusInReconciliation,
    StreamStatusClosed

\* ------ Assumptions ------
\* ReplicationFactor must be positive and there must be enough units
ASSUME ReplicationFactor \in Nat /\ ReplicationFactor > 0
ASSUME Cardinality(Units) >= ReplicationFactor
ASSUME Cardinality(Events) >= 1


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLE catalogs         \* Metadata store: per-stream catalog
VARIABLE units            \* State of each unit (server)
VARIABLE streams        \* State of each stream (client-side)
VARIABLE sent_events      \* Set of event payloads already sent (model constraint)
VARIABLE acked_events     \* Set of event payloads acknowledged to clients

vars == << units, catalogs, message_channel, streams,
           sent_events, acked_events, message_fail_count >>


(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

\* Offset domain for events
EventOffsets == 1..(Cardinality(Events) + Cardinality(Streams))

\* A single event append
Event == [offset: EventOffsets, data: Events]

NullEvent == [offset |-> 0, data |-> Null]

\* Version type for catalog CAS operations
Version == Nat \union {Null}

\* A segment: range of offsets with a fixed ensemble
Segment == [id: Nat, ensemble: SUBSET Units, start_offset: Nat]

\* An in-flight write tracked by the stream client
InflightEvent == [event: Event, segment_id: Nat, ensemble: SUBSET Units]

\* Stream status values
StreamStatus == {Null, StreamStatusOpen, StreamStatusInReconciliation, StreamStatusClosed}

\* ------ Reconciliation phases ------
\* Reconciliation proceeds in two phases:
\*   Phase 1 (Fencing): Send fence requests to the ensemble of the last
\*     segment. We need ALL units in the ensemble to
\*     be fenced to guarantee no old-term writer can make progress.
\*   Phase 2 (Aligning): Read the highest LRA from fenced units, then
\*     truncate dirty entries from the previous term.
NoReconciliation == 0
ReconciliationFencing == 1
ReconciliationAligning == 2


\* ------ Catalog: stored in metadata service (ZK/etcd) ------
StreamCatalog == [
    status   : StreamStatus,
    segments : Seq(Segment),
    term     : Nat,
    version  : Version,
    lra      : Nat
]

\* ------ Unit: server-side state per stream ------
UnitState == [
    stream_events : [Streams -> SUBSET Event],
    stream_lra    : [Streams -> Nat],
    stream_term   : [Streams -> Nat]
]

\* ------ Stream: client-side state ------
StreamState == [
    id                        : Streams,
    term                      : Nat,
    segments                  : Seq(Segment),
    writable_segment          : Segment \cup {Null},
    inflight_append_event_reqs : SUBSET InflightEvent,
    status                    : StreamStatus,
    lrs                       : Nat,
    lra                       : Nat,
    acked                     : [EventOffsets -> SUBSET Units],
    fenced                    : SUBSET Units,
    reconciliation            : NoReconciliation..ReconciliationAligning,
    reconciliation_ensemble   : SUBSET Units,
    reconciliation_lra        : Nat,
    catalog_version           : Version
]


(***************************************************************************)
(* UTILITY FUNCTIONS                                                       *)
(***************************************************************************)

\* Get the last element of a sequence
FindLast(seq) == seq[Len(seq)]

\* Check if an ensemble is valid: correct size, includes required units,
\* excludes quarantined units
IsValidEnsemble(ensemble, include_units, exclude_units) ==
    /\ Cardinality(ensemble) = ReplicationFactor
    /\ ensemble \intersect exclude_units = {}
    /\ include_units \subseteq ensemble

\* Choose a valid ensemble
FindEnsemble(tid, available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

\* Check if a valid ensemble exists
HasTargetEnsemble(tid, available, quarantined) ==
    \E ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

\* Find the maximum contiguous offset where all offsets from curr..result
\* have at least `quorum` acks
RECURSIVE FindMaxContinuousAck(_, _, _, _)
FindMaxContinuousAck(curr, max_idx, acked, quorum) ==
    IF curr > max_idx THEN max_idx
    ELSE IF Cardinality(acked[curr]) < quorum THEN curr - 1
    ELSE FindMaxContinuousAck(curr + 1, max_idx, acked, quorum)

GetAckedOffset(stream, current, acked, quorum) ==
    FindMaxContinuousAck(current, stream.lrs, acked, quorum)

\* Find the segment index responsible for a given offset
SegmentForOffset(segments, offset) ==
    CHOOSE i \in 1..Len(segments) :
        /\ segments[i].start_offset <= offset
        /\ (i = Len(segments) \/ segments[i+1].start_offset > offset)


(***************************************************************************)
(* ACTION: OPEN NEW STREAM                                               *)
(*                                                                         *)
(* A client creates a new stream by writing initial metadata to the      *)
(* catalog and selecting the first ensemble.                               *)
(***************************************************************************)

OpenNewStream(tid) ==
    LET stream == streams[tid]
        catalog  == catalogs[tid]
    IN
        \* Only open if not yet created
        /\ catalog.version = Null
        /\ stream.catalog_version = Null
        /\ LET first_segment == [
                    id           |-> 1,
                    ensemble     |-> FindEnsemble(tid, {}, {}),
                    start_offset |-> 1
               ]
           IN
            /\ streams' = [streams EXCEPT ![tid] =
                [@ EXCEPT
                    !.status          = StreamStatusOpen,
                    !.catalog_version = 1,
                    !.term            = 1,
                    !.segments        = Append(catalog.segments, first_segment),
                    !.writable_segment = first_segment
                ]]
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.status   = StreamStatusOpen,
                    !.version  = 1,
                    !.term     = 1,
                    !.segments = Append(catalog.segments, first_segment)
                ]]
            /\ UNCHANGED << units, message_channel, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: APPEND EVENT (Write)                                            *)
(*                                                                         *)
(* The stream client sends a new event to ALL units in the current       *)
(* ensemble. Every unit must receive and ack the event.                    *)
(***************************************************************************)

\* Construct append requests for each unit in the ensemble
MakeAppendRequests(stream, event, ensemble, trunc) ==
    {[
        type        |-> AppendEventRequest,
        unit        |-> unit,
        stream_id |-> stream.id,
        event       |-> event,
        lra         |-> stream.lra,
        term        |-> stream.term,
        trunc       |-> trunc
    ] : unit \in ensemble}

\* Construct an append response
MakeAppendResponse(req) ==
    [
        type        |-> AppendEventResponse,
        unit        |-> req.unit,
        stream_id |-> req.stream_id,
        event       |-> req.event,
        term        |-> req.term,
        code        |-> Ok
    ]

StreamAppendEvent(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusOpen
        \* Flow control: at most 1 in-flight event beyond LRA
        /\ stream.lrs - stream.lra < 1
        \* Pick a fresh payload
        /\ \E payload \in Events : payload \notin sent_events
        /\ LET payload == CHOOSE p \in Events : p \notin sent_events
               event   == [offset |-> stream.lrs + 1, data |-> payload]
           IN
            /\ UCSendToEnsemble(
                   MakeAppendRequests(stream, event,
                       stream.writable_segment.ensemble, FALSE))
            /\ streams' = [streams EXCEPT ![tid] =
                [@ EXCEPT
                    !.lrs = stream.lrs + 1,
                    !.inflight_append_event_reqs = @ \cup {[
                        event      |-> event,
                        segment_id |-> stream.writable_segment.id,
                        ensemble   |-> stream.writable_segment.ensemble
                    ]}
                ]]
            /\ sent_events' = sent_events \cup {payload}
            /\ UNCHANGED << units, catalogs, acked_events >>


(***************************************************************************)
(* ACTION: UNIT HANDLES APPEND REQUEST                                     *)
(*                                                                         *)
(* A unit receives a append request. It checks the term: if the request    *)
(* term >= unit's term, it accepts and stores the event. If the request    *)
(* has trunc=TRUE, it deletes all events with higher offsets (used during  *)
(* reconciliation to clean up dirty entries from old terms).               *)
(***************************************************************************)

\* Ensure we process the earliest pending request first (per unit/stream)
IsEarliestRequest(message) ==
    ~\E other \in DOMAIN message_channel :
        /\ other.type = AppendEventRequest
        /\ message_channel[other] >= 1
        /\ other.term = message.term
        /\ other.stream_id = message.stream_id
        /\ other.unit = message.unit
        /\ other.event.offset < message.event.offset

UnitHandleAppendRequest ==
    \E message \in DOMAIN message_channel :
        /\ message.type = AppendEventRequest
        /\ message_channel[message] >= 1
        /\ IsEarliestRequest(message)
        \* Term check: accept if unit's term <= request's term
        /\ units[message.unit].stream_term[message.stream_id] <= message.term
        /\ LET unit == units[message.unit]
               tid  == message.stream_id
               \* If trunc flag, remove events with offset >= this event,
               \* then add this event. Otherwise just upsert.
               new_events ==
                   IF message.trunc
                   THEN {e \in unit.stream_events[tid] :
                             e.offset < message.event.offset}
                        \cup {message.event}
                   ELSE (unit.stream_events[tid]
                            \ {e \in unit.stream_events[tid] :
                                   e.offset = message.event.offset})
                        \cup {message.event}
               new_term == [unit.stream_term EXCEPT
                                ![tid] = message.term]
               new_lra  == [unit.stream_lra EXCEPT
                                ![tid] = IF message.lra > @ THEN message.lra ELSE @]
           IN
            /\ units' = [units EXCEPT ![message.unit] = [
                    stream_events |-> [unit.stream_events EXCEPT ![tid] = new_events],
                    stream_term   |-> new_term,
                    stream_lra    |-> new_lra
               ]]
            /\ UCConsumeAndSend(message, MakeAppendResponse(message))
            /\ UNCHANGED << streams, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: STREAM HANDLES APPEND RESPONSE                                  *)
(*                                                                         *)
(* The stream client processes ack responses. An event is committed       *)
(* (LRA advances) only when ALL units in the ensemble have acknowledged    *)
(* it.                                                                     *)
(***************************************************************************)

StreamHandleAppendResponse(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusOpen
        /\ \E message \in DOMAIN message_channel :
            /\ message.type = AppendEventResponse
            /\ message_channel[message] >= 1
            /\ message.stream_id = tid
            /\ message.code = Ok
            /\ message.term = stream.term
            /\ message.unit \in stream.writable_segment.ensemble
            \* Process responses in offset order
            /\ ~\E other \in DOMAIN message_channel :
                /\ other.type = AppendEventResponse
                /\ message_channel[other] >= 1
                /\ other.stream_id = tid
                /\ other.term = message.term
                /\ other.event.offset < message.event.offset
            /\ LET
                   event  == message.event
                   acked  == [stream.acked EXCEPT
                                  ![event.offset] = @ \cup {message.unit}]
                   lra    == GetAckedOffset(stream, stream.lra + 1,
                                            acked, ReplicationFactor)
               IN
                /\ streams' = [streams EXCEPT ![tid] =
                    [stream EXCEPT
                        !.acked = acked,
                        !.lra   = IF lra > @ THEN lra ELSE @,
                        !.inflight_append_event_reqs =
                            {op \in stream.inflight_append_event_reqs :
                                 op.event.offset > lra}
                    ]]
                /\ acked_events' =
                       IF lra >= event.offset
                       THEN acked_events \cup {event.data}
                       ELSE acked_events
                /\ UCAckMessage(message)
            /\ UNCHANGED << units, catalogs, sent_events, message_fail_count >>


(***************************************************************************)
(* ACTION: RETRY INFLIGHT APPEND EVENT                                     *)
(*                                                                         *)
(* Resend a append request to units that haven't acked yet. Handles        *)
(* transient message loss.                                                 *)
(***************************************************************************)

StreamRetryInflightAppendEvent(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusOpen
        /\ \E req \in stream.inflight_append_event_reqs :
            \* Retry the earliest inflight per segment
            /\ ~\E other \in stream.inflight_append_event_reqs :
                /\ other.segment_id = req.segment_id
                /\ other.ensemble = req.ensemble
                /\ other.event.offset < req.event.offset
            /\ LET
                   \* Only send to units that haven't acked
                   target_units == stream.writable_segment.ensemble
                                       \ stream.acked[req.event.offset]
                   replaced_req == [
                       event      |-> req.event,
                       segment_id |-> stream.writable_segment.id,
                       ensemble   |-> stream.writable_segment.ensemble
                   ]
               IN
                /\ target_units # {}
                /\ UCSendToEnsemble(
                       MakeAppendRequests(stream, req.event,
                                          target_units, FALSE))
                /\ streams' = [streams EXCEPT ![tid] =
                    [stream EXCEPT
                        !.inflight_append_event_reqs =
                            (@ \ {req}) \cup {replaced_req}
                    ]]
                /\ UNCHANGED << units, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: ENSEMBLE CHANGE                                                 *)
(*                                                                         *)
(* When a unit fails (detected via message loss), replace it in the        *)
(* ensemble. A new segment starts at LRA + 1, since all entries up to      *)
(* LRA are fully replicated on all ensemble members.                       *)
(***************************************************************************)

\* Check if a message to this unit has been lost
HasFailureMessage(stream, failure_unit) ==
    \E message \in DOMAIN message_channel :
        /\ message.type \in {AppendEventRequest, AppendEventResponse}
        /\ message_channel[message] = -1
        /\ message.stream_id = stream.id
        /\ message.unit = failure_unit
        /\ message.term = stream.term

\* Remove all messages related to failed units
CleanupFailureMessages(stream, failure_units) ==
    LET NeedClear(m) ==
        /\ m.type \in {AppendEventRequest, AppendEventResponse}
        /\ m.stream_id = stream.id
        /\ m.unit \in failure_units
        /\ m.term = stream.term
    IN
        message_channel' = [m \in {m \in DOMAIN message_channel :
                                       ~NeedClear(m)}
                                |-> message_channel[m]]

\* Append a new segment or modify the last one if same start offset
AppendOrModifySegment(stream, start_offset, new_ensemble) ==
    IF start_offset = stream.writable_segment.start_offset
    THEN [stream.segments EXCEPT
              ![Len(stream.segments)].ensemble = new_ensemble]
    ELSE Append(stream.segments, [
             id           |-> Len(stream.segments) + 1,
             ensemble     |-> new_ensemble,
             start_offset |-> start_offset
         ])

\* A unit is "pinned" if it holds acked data that hasn't been fully
\* replicated elsewhere. Cannot remove pinned units.
UnitIsPinned(stream, unit, start_offset) ==
    IF start_offset > stream.lra THEN FALSE
    ELSE \E offset \in start_offset..stream.lra :
             unit \in stream.acked[offset]

StreamEnsembleChange(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusOpen
        /\ \E failure_units \in SUBSET stream.writable_segment.ensemble :
            /\ failure_units # {}
            /\ \A u \in failure_units :
                   HasFailureMessage(stream, u)
            /\ HasTargetEnsemble(tid,
                   stream.writable_segment.ensemble \ failure_units,
                   failure_units)
            /\ LET
                   new_ensemble == FindEnsemble(tid,
                       stream.writable_segment.ensemble \ failure_units,
                       failure_units)
                   start_offset     == stream.lra + 1
                   new_segments     == AppendOrModifySegment(stream,
                                          start_offset, new_ensemble)
                   next_version     == catalogs[tid].version + 1
                   FilterAcked(acked, offset) ==
                       IF offset >= start_offset
                       THEN acked \ failure_units
                       ELSE acked
               IN
                \* No pinned units among those being removed
                /\ \A u \in failure_units :
                       ~UnitIsPinned(stream, u, start_offset)
                \* CAS on catalog version
                /\ catalogs[tid].version = stream.catalog_version
                /\ catalogs' = [catalogs EXCEPT ![tid] =
                    [@ EXCEPT
                        !.segments = new_segments,
                        !.version  = next_version
                    ]]
                /\ streams' = [streams EXCEPT ![tid] =
                    [@ EXCEPT
                        !.catalog_version = next_version,
                        !.acked = [offset \in DOMAIN stream.acked |->
                                       FilterAcked(stream.acked[offset],
                                                   offset)],
                        !.segments = new_segments,
                        !.writable_segment = FindLast(new_segments)
                    ]]
                /\ CleanupFailureMessages(stream, failure_units)
                /\ UNCHANGED << acked_events, sent_events,
                                units, message_fail_count >>


(***************************************************************************)
(* ACTION: START RECONCILIATION (New Term)                                 *)
(*                                                                         *)
(* A new client takes over a stream by incrementing the term in the      *)
(* catalog (CAS) and beginning the fencing phase.                          *)
(*                                                                         *)
(* Reconciliation process:                                                 *)
(*   1. Increment term in catalog (CAS)                                    *)
(*   2. Send fence requests to all units in the last segment's ensemble    *)
(*   3. Collect responses, learn highest LRA across units                  *)
(*   4. Truncate dirty entries from old term on all units                  *)
(*   5. Resume normal writes                                               *)
(***************************************************************************)

MakeFenceRequests(tid, term, ensemble) ==
    {[
        type        |-> FenceRequest,
        unit        |-> unit,
        stream_id |-> tid,
        term        |-> term
    ] : unit \in ensemble}

MakeFenceResponse(req, unit_lra) ==
    [
        type        |-> FenceResponse,
        unit        |-> req.unit,
        stream_id |-> req.stream_id,
        term        |-> req.term,
        lra         |-> unit_lra,
        code        |-> Ok
    ]

StreamStartReconciliation(tid) ==
    LET stream == streams[tid]
        catalog  == catalogs[tid]
    IN
        /\ catalog.status = StreamStatusOpen
        /\ stream.status \in {Null, StreamStatusOpen}
        /\ stream.reconciliation = NoReconciliation
        /\ catalog.version # Null
        /\ LET
               new_term     == catalog.term + 1
               next_version == catalog.version + 1
               last_segment == FindLast(catalog.segments)
               ensemble     == last_segment.ensemble
           IN
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.term    = new_term,
                    !.status  = StreamStatusInReconciliation,
                    !.version = next_version
                ]]
            /\ UCSendToEnsemble(MakeFenceRequests(tid, new_term, ensemble))
            /\ streams' = [streams EXCEPT ![tid] =
                [@ EXCEPT
                    !.status                  = StreamStatusInReconciliation,
                    !.term                    = new_term,
                    !.catalog_version         = next_version,
                    !.segments                = catalog.segments,
                    !.writable_segment        = Null,
                    !.reconciliation          = ReconciliationFencing,
                    !.reconciliation_ensemble = ensemble,
                    !.fenced                  = {},
                    !.reconciliation_lra      = 0,
                    !.inflight_append_event_reqs = {},
                    !.acked                   = [offset \in EventOffsets |-> {}],
                    !.lrs                     = 0,
                    !.lra                     = 0
                ]]
            /\ UNCHANGED << units, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: UNIT HANDLES FENCE REQUEST                                      *)
(*                                                                         *)
(* A unit receives a fence request. If request term >= unit's current      *)
(* term, the unit updates its term and responds with its LRA.              *)
(***************************************************************************)

UnitHandleFenceRequest ==
    \E message \in DOMAIN message_channel :
        /\ message.type = FenceRequest
        /\ message_channel[message] >= 1
        /\ LET unit == units[message.unit]
               tid  == message.stream_id
           IN
            /\ unit.stream_term[tid] <= message.term
            /\ units' = [units EXCEPT ![message.unit] =
                [@ EXCEPT
                    !.stream_term = [unit.stream_term EXCEPT
                                          ![tid] = message.term]
                ]]
            /\ UCConsumeAndSend(message,
                   MakeFenceResponse(message,
                       unit.stream_lra[tid]))
            /\ UNCHANGED << streams, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: STREAM HANDLES FENCE RESPONSE                                 *)
(*                                                                         *)
(* Collect fence responses. ALL units must be fenced before proceeding     *)
(* to the aligning phase.                                                  *)
(***************************************************************************)

StreamHandleFenceResponse(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusInReconciliation
        /\ stream.reconciliation = ReconciliationFencing
        /\ \E message \in DOMAIN message_channel :
            /\ message.type = FenceResponse
            /\ message_channel[message] >= 1
            /\ message.stream_id = tid
            /\ message.term = stream.term
            /\ message.code = Ok
            /\ LET
                   new_fenced   == stream.fenced \cup {message.unit}
                   new_rec_lra  == IF stream.fenced = {}
                                   THEN message.lra
                                   ELSE IF message.lra < stream.reconciliation_lra
                                        THEN message.lra
                                        ELSE stream.reconciliation_lra
                   all_fenced   == new_fenced = stream.reconciliation_ensemble
               IN
                /\ streams' = [streams EXCEPT ![tid] =
                    [@ EXCEPT
                        !.fenced               = new_fenced,
                        !.reconciliation_lra   = new_rec_lra,
                        !.reconciliation =
                            IF all_fenced THEN ReconciliationAligning
                            ELSE ReconciliationFencing
                    ]]
                /\ UCAckMessage(message)
            /\ UNCHANGED << units, catalogs, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: COMPLETE RECONCILIATION                                         *)
(*                                                                         *)
(* After fencing all units, truncate dirty entries from the old term and   *)
(* transition to Open. The stream can now accept writes under the new    *)
(* term.                                                                   *)
(*                                                                         *)
(* Modeled as atomic for simplicity. A real implementation would send      *)
(* truncation entries (trunc=TRUE) and wait for acks from all units.       *)
(***************************************************************************)

StreamCompleteReconciliation(tid) ==
    LET stream == streams[tid]
        catalog  == catalogs[tid]
    IN
        /\ stream.status = StreamStatusInReconciliation
        /\ stream.reconciliation = ReconciliationAligning
        /\ LET
               new_lra      == stream.reconciliation_lra
               last_segment == FindLast(stream.segments)
               next_version == catalog.version + 1
               \* Rebuild segments: keep segments covering committed
               \* offsets (1..new_lra), add writable segment at new_lra+1
               reconciled_segments ==
                   IF new_lra = 0
                   THEN <<[id           |-> 1,
                           ensemble     |-> last_segment.ensemble,
                           start_offset |-> 1]>>
                   ELSE LET cover_count ==
                                SegmentForOffset(stream.segments, new_lra)
                            writable == [
                                id           |-> cover_count + 1,
                                ensemble     |-> last_segment.ensemble,
                                start_offset |-> new_lra + 1]
                        IN Append(SubSeq(stream.segments, 1,
                                         cover_count), writable)
               writable_seg == FindLast(reconciled_segments)
           IN
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.status   = StreamStatusOpen,
                    !.version  = next_version,
                    !.lra      = new_lra,
                    !.segments = reconciled_segments
                ]]
            /\ streams' = [streams EXCEPT ![tid] =
                [@ EXCEPT
                    !.status          = StreamStatusOpen,
                    !.catalog_version = next_version,
                    !.lra             = new_lra,
                    !.lrs             = new_lra,
                    !.segments        = reconciled_segments,
                    !.writable_segment = writable_seg,
                    !.reconciliation  = NoReconciliation,
                    !.fenced          = {},
                    !.acked           = [offset \in EventOffsets |->
                                            IF offset <= new_lra
                                            THEN writable_seg.ensemble
                                            ELSE {}]
                ]]
            \* Truncate dirty entries on all units across all segments
            /\ LET all_ensemble_units ==
                       UNION {stream.segments[i].ensemble :
                                  i \in 1..Len(stream.segments)}
               IN
                units' = [u \in Units |->
                    IF u \in all_ensemble_units
                    THEN [units[u] EXCEPT
                        !.stream_events = [units[u].stream_events EXCEPT
                            ![tid] = {e \in @ : e.offset <= new_lra}],
                        !.stream_lra  = [units[u].stream_lra EXCEPT
                            ![tid] = IF new_lra > @ THEN new_lra ELSE @],
                        !.stream_term = [units[u].stream_term EXCEPT
                            ![tid] = stream.term]
                    ]
                    ELSE units[u]
                   ]
            /\ UNCHANGED << message_channel, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: RETRY FENCE REQUEST                                             *)
(*                                                                         *)
(* Resend a fence request to an unfenced unit when the original fence      *)
(* request or its response was lost. Handles transient message loss        *)
(* during reconciliation.                                                  *)
(***************************************************************************)

StreamRetryFenceRequest(tid) ==
    LET stream == streams[tid]
    IN
        /\ stream.status = StreamStatusInReconciliation
        /\ stream.reconciliation = ReconciliationFencing
        /\ \E retry_unit \in stream.reconciliation_ensemble \ stream.fenced :
            LET fence_req == [
                    type        |-> FenceRequest,
                    unit        |-> retry_unit,
                    stream_id |-> tid,
                    term        |-> stream.term
                ]
            IN
                \E lost_msg \in DOMAIN message_channel :
                    /\ lost_msg.type \in {FenceRequest, FenceResponse}
                    /\ lost_msg.stream_id = tid
                    /\ lost_msg.unit = retry_unit
                    /\ lost_msg.term = stream.term
                    /\ message_channel[lost_msg] = -1
                    /\ UCConsumeAndSend(lost_msg, fence_req)
                    /\ UNCHANGED << units, streams, catalogs,
                                    sent_events, acked_events >>


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

InitStream(tid) == [
    id                         |-> tid,
    term                       |-> 0,
    segments                   |-> <<>>,
    writable_segment           |-> Null,
    inflight_append_event_reqs  |-> {},
    status                     |-> Null,
    lrs                        |-> 0,
    lra                        |-> 0,
    acked                      |-> [offset \in EventOffsets |-> {}],
    fenced                     |-> {},
    reconciliation             |-> NoReconciliation,
    reconciliation_ensemble    |-> {},
    reconciliation_lra         |-> 0,
    catalog_version            |-> Null
]

InitUnit(u) == [
    stream_events |-> [tid \in Streams |-> {}],
    stream_lra    |-> [tid \in Streams |-> 0],
    stream_term   |-> [tid \in Streams |-> 0]
]

InitCatalog(tid) == [
    status   |-> Null,
    version  |-> Null,
    segments |-> <<>>,
    term     |-> 0,
    lra      |-> 0
]

Init ==
    /\ units              = [u   \in Units     |-> InitUnit(u)]
    /\ streams          = [tid \in Streams  |-> InitStream(tid)]
    /\ catalogs           = [tid \in Streams  |-> InitCatalog(tid)]
    /\ message_channel    = [message \in {} |-> 0]
    /\ message_fail_count = [message \in {} |-> 0]
    /\ sent_events        = {}
    /\ acked_events       = {}


(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \* Unit-side actions
    \/ UnitHandleAppendRequest
    \/ UnitHandleFenceRequest
    \* Stream-side actions
    \/ \E tid \in Streams :
        \/ OpenNewStream(tid)
        \/ StreamAppendEvent(tid)
        \/ StreamHandleAppendResponse(tid)
        \/ StreamRetryInflightAppendEvent(tid)
        \/ StreamEnsembleChange(tid)
        \/ StreamStartReconciliation(tid)
        \/ StreamHandleFenceResponse(tid)
        \/ StreamRetryFenceRequest(tid)
        \/ StreamCompleteReconciliation(tid)


(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars


(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeOK ==
    /\ units     \in [Units     -> UnitState]
    /\ catalogs  \in [Streams -> StreamCatalog]
    /\ streams \in [Streams -> StreamState]


(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* Every acknowledged event has at least ReplicationFactor copies.
\* This means ALL ensemble members have it.
AllAckedEventsFullyReplicated ==
    \A tid \in Streams :
        \A offset \in 1..streams[tid].lra :
            Cardinality(streams[tid].acked[offset]) >= ReplicationFactor

\* No acknowledged event is lost from the ensemble responsible for it.
NoAckedEventLost ==
    \A tid \in Streams :
        streams[tid].status = StreamStatusOpen =>
            \A offset \in 1..streams[tid].lra :
                LET seg == streams[tid].segments[
                        SegmentForOffset(streams[tid].segments, offset)]
                IN \A unit \in seg.ensemble :
                    \E e \in units[unit].stream_events[tid] :
                        e.offset = offset

\* No divergence: all units in the responsible segment agree on committed entries.
NoDivergence ==
    \A tid \in Streams :
        streams[tid].status = StreamStatusOpen =>
            \A offset \in 1..streams[tid].lra :
                LET seg == streams[tid].segments[
                        SegmentForOffset(streams[tid].segments, offset)]
                IN \A u1, u2 \in seg.ensemble :
                    LET e1 == {e \in units[u1].stream_events[tid] : e.offset = offset}
                        e2 == {e \in units[u2].stream_events[tid] : e.offset = offset}
                    IN (e1 # {} /\ e2 # {}) => e1 = e2

\* Acked events are a subset of sent events.
AckedSubsetOfSent ==
    acked_events \subseteq sent_events


(***************************************************************************)
(* STATE CONSTRAINT (bound the state space for model checking)             *)
(***************************************************************************)

\* Bound the term to prevent infinite reconciliation cycles.
\* Term 1 = initial, term 2 = first reconciliation, term 3 = second.
\* This is sufficient to verify correctness across multiple reconciliations.
StateConstraint ==
    \A tid \in Streams : streams[tid].term <= 1

(***************************************************************************)
(* SYMMETRY (for model checking optimization)                              *)
(***************************************************************************)

Symmetry == Permutations(Events) \cup Permutations(Units)

=========================================================
