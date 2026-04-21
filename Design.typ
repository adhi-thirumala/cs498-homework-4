= /co-area-drivers: Graph vs. Relational

== 1. SQL Implementation

With normalized tables `drivers(driver_id)`, `areas(area_id)`, and
`trips(trip_id, driver_id, area_id, ...)`, the equivalent is a self-join
on `trips`:

- Select `t1` rows where `t1.driver_id = :driver_id`.
- Join `t2` on `t2.area_id = t1.area_id` with `t2.driver_id <> :driver_id`.
- `GROUP BY t2.driver_id`, aggregate `COUNT(DISTINCT t1.area_id)` as
  `shared_areas`, and `ORDER BY shared_areas DESC`.

The planner uses an index seek on `trips(driver_id)` to find the anchor's
areas, a hash/merge join on `area_id` to gather co-drivers, then a hash
aggregate.

== 2. Why the Graph DB Is More Natural

Neo4j uses _index-free adjacency_: each node stores direct pointers to its
incident relationships, so traversing `Driver→TRIP→Area→TRIP→Driver` is
pointer-chasing in O(degree) per hop, independent of total graph size. The
Cypher pattern mirrors the traversal, so after the initial anchor lookup
there are no B-tree probes or intermediate row materializations. A
relational engine must execute an index seek or hash join per hop; cost
scales with the size of `trips`, not just the local neighborhood.

== 3. Where Neo4j Underperforms

The taxi data is skewed: a few Areas (O'Hare, Loop) absorb most trips.
Traversing into those super-nodes explodes intermediate results into
millions of edges per hop, turning pointer-chasing into a bottleneck. A
columnar or well-indexed relational store handles this as a bulk hash
aggregation with vectorized scans and parallelism — much faster than
per-edge traversal across a dense hub.
