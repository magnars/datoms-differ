# datoms-differ

Find the diff (as datoms) between two txes (of entity maps).

*What the what?*

Yeah, good question. Ahem. Basically, it's a way of keeping track of changes in
data via a datomic/datascript-like API and data structure.

*What's it for then?*

One use case is keeping clients in sync with a backend. The
client is initialized with a datascript db when connecting, and is fed updates
in the form of tx-data when the backend changes.

Or you might just be looking to diff complex structures of data with a convenient RDF-style output.

*So what does this do again?*

You write transactions to assert information about entities from a blank slate.
Then datoms-differ finds out what's changed since the last time. It's a differ
that takes the old data plus a datascript-like transaction of entity maps, and
outputs a set of tx-data additions and retractions.

*What the what?*

Yeah, still a good question.

## Install

Add `[datoms-differ "2025-09-16"]` to `:dependencies` in your `project.clj`.

## API

Require `[datoms-differ.api]`.

### `(create-conn schema)`

Takes a datascript schema and creates a "connection" (really, an atom with an
empty db). A datascript schema might look like this:

```
(def schema
  {:route/number {:db/unique :db.unique/identity}
   :route/vessels {:db/valueType :db.type/ref
                   :db/cardinality :db.cardinality/many}
   :vessel/imo {:db/unique :db.unique/identity}})
```

Like datascript, datoms-differ only cares about `:db.unique/identity`,
`:db.type/ref`, `:db.cardinality/many` and `:db/isComponent`.

### `(transact! conn source entity-maps)`

Takes a connection, a keyword source identifier and a list of entity maps, and
transacts them into the connection.

- `conn` - the atom you created with `create-conn`
- `source` - you can put data into the connection from multiple sources, but
  since datoms-differ will retract missing values, it will only retract values
  previously asserted by the same source.
- `entity-maps` - a list of entity maps that are asserted by this source. Example:

```
[{:route/number "100"
  :route/name "Stavanger-Tau"
  :route/vessels [{:vessel/imo "123"
                   :vessel/mmsi "456"}]}
 {:vessel/imo "123"
  :vessel/name "MF Hardanger"}]
```

Note that every entity map needs to contain one and only one attribute that is
marked as `:db.unique/identity` in the schema.

Another interesting note about the above example is that since `:route/vessels`
is marked as `:db.type/ref`, and `:vessel/imo` is `:db.unique/identity`, there
will only be two entities as a result of this transaction. The route and one
vessel with all three asserted attributes (imo, mmsi and name).

The returned value from `transact!` has `:db-after`, `:db-before`, but most
interestingly it has a `:tx-data` list of datoms. **This is the diff! Here it
is!**

## Updating multiple sources at once

If multiple sources has changed at the same time, you can use
`transact-sources!` to assert all transactions at once.

### `(transact-sources! conn source->entity-maps)`

Takes a connection and a map from source identifier to a list of entity maps,
and transacts them all into the connection.

In addition to being more efficient, and simpler to work with when you have
multiple sources updating at once, this also avoids an issue where old data +
partial new data would make a conflict, where the conflict is resolved in
another part of the new data (not yet asserted).

## Exporting to datascript

There's also some tools for exporting to datascript. This lets you create a
datascript db in the client, and then keep it up to date with new txes.

Use `[datoms-differ.api]` with `(export-db db)` that gives you a string that
can be read by clojurescript (when datascript is loaded) to create a datascript
db.

If you want to reduce the amount of bytes sent over the wire, you can also use
`(prune-diffs schema tx-data)` to remove retractions of values that are later
asserted. This optimalisation is possible since datascript doesn't have a notion
of history.

## Contribute

Yes, please do. And add tests for your feature or fix, or I'll
certainly break it later.

#### Running the tests

`bin/kaocha` will run all tests.

`bin/kaocha --watch` will run all the tests indefinitely. It sets up a
watcher on the code files. If they change, only the relevant tests will be
run again.

## License

Copyright © (iterate inc 2017) Magnar Sveen

Distributed under the Eclipse Public License, the same as Clojure.
