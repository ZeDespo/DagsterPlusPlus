# dagster_plus_plus

A utility kit for Dagster to solve some problems I encountered.

- Resources to access persistent data stores.
    - Postgres (with cache via unlogged table).
    - DuckDB  (with cache via temporary table).
- An IO manager that... 
    - writes inputs / outputs to a database rather than the file system.
    - allows non-partitioned assets to provide downstream output to partitioned assets.
    - allows partitioned assets to provide donwstream output to non-partitioned assets 
      as a mapping where the key is the partition key and the value is the materialized asset value.

## Inspiration 

Dagster is pretty good at data orchestration; however, the default behaviors of some 
of their parts are questionable. The I/O manager that comes default is strict, and relies 
on the file system by default. There aren't any "common sense" resources included, 
which can lead to developers copying and pasting the same code over and over. The 
type system (while greatly appreciated for correctness) can be challenging to work 
around and make generic pipelines. And finally, there isn't an easy way to just pass 
data around from assets in one repo to another.

So I made this swiss army knife of readily accessible utilities that can let me 
break dagster however I see fit. 

## Getting started

### Installing dependencies

**task**

It's essentially a modern day `make`. 

Install it with this one liner: 
`sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin`

Then, run `task dev:install`. 

## Running Tests 

`task dev:test`
