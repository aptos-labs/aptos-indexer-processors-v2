pub mod bq_analytics;
pub mod db;
pub mod utils;

// Need to use this for because schema.rs uses the macros and is autogenerated
#[macro_use]
extern crate diesel;

// for parquet_derive
extern crate canonical_json;
extern crate parquet;
extern crate parquet_derive;

#[path = "db/schema.rs"]
pub mod schema;

pub mod config;
pub mod parsing;
pub mod processors;
pub mod steps;

pub mod parquet_processors;
