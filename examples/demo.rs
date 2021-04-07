extern crate shed;

use anyhow;
use async_stream::stream;
use bincode::{DefaultOptions, Options};
use derive_manifold::Manifold;
use futures;
use futures::{pin_mut, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use shed::{Config, Encodable, History, Manifold, ManifoldAdapter, Pipe, Shed, Source, Store};
use sled::IVec;
use std::pin::Pin;

#[derive(Serialize, PartialEq, Deserialize, Debug)]
struct Test1 {
    a: u64,
    b: i16,
}

#[derive(Serialize, PartialEq, Deserialize, Debug)]
struct Test2 {
    s: String,
    weee: u128,
}

#[derive(Serialize, PartialEq, Deserialize, Debug)]
struct Test3 {
    a: i32,
    huh: String,
}

#[derive(Manifold, Debug)]
enum TestManifold {
    #[prefix = "test1/foo"]
    Test1Item((u64, Test1)),
    #[prefix = "test2/bar"]
    Test2Item((u64, Test2)),
    #[prefix = "test3/baz"]
    Test3Item((u64, Test3)),
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // run put_get demo
    put_get().await;

    // run manifold demo
    manifold().await;

    // run history demo
    history().await
}

async fn put_get() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let config = Config::new().set_dir("/tmp/shed_putget");
    let shed = Shed::new("demo", &config);
    let tree = shed.clone().tree;
    let prefix = b"putgettest";
    let s = stream! {
        for i in 0..300 {
            yield (i + 100, Test1{ a:i, b: 42});
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    };
    // create a stream by subscribing to the prefix
    let getter: Pipe<u64, Test1> = shed.pipe_subscribe(prefix, History::None).unwrap();

    // create the writer and spawn it to pump the data into the DB
    let put = shed.pipe_put(prefix.to_vec(), s);
    tokio::spawn(put);

    // Let's see how we did
    // Extract the contents of the getter stream into a Vec
    let g_results = getter
        .take(100)
        .map(|a| a.unwrap())
        .collect::<Vec<_>>()
        .await;
    // Use a range directly on the DB to get our comparison data.
    println!("Comparing getter results");
    let r_results = tree
        .range(IVec::from(prefix)..)
        .take(100)
        .flatten()
        .map(|(k, v)| {
            (
                u64::decode(&k[prefix.len()..]).unwrap(),
                Test1::decode(v).unwrap(),
            )
        });
    // Compare
    if g_results
        .iter()
        .zip(r_results)
        .all(|((k1, v1), (k2, v2))| *k1 == k2 && *v1 == v2)
    {
        println!("We have a match!");
    } else {
        println!("D:")
    }
    Ok(())
}

async fn history() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let config = Config::new().set_dir("/tmp/shed_history");
    let shed = Shed::new("demo", &config);
    let tree = shed.clone().tree;
    let prefix = b"history_demo/";
    // create and spawn a writer for some "historical" data
    let s = stream! {
        for i in 0..100 {
            yield (i, Test1{ a:i, b: 42});
        }
    };
    let put = shed.pipe_put(prefix.to_vec(), s);
    put.await.unwrap();

    // create a stream by subscribing to the prefix
    let getter: Pipe<u64, Test1> = shed.pipe_subscribe(prefix, History::All).unwrap();

    // create the writer and spawn it to pump the data into the DB
    let t = stream! {
        for i in 100..200 {
            //tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            yield (i, Test1{ a:i, b: 42});
        }
    };
    let put2 = shed.pipe_put(prefix.to_vec(), t);
    tokio::spawn(put2);

    println!("Comparing history results");
    // Let's see how we did
    // Extract the contents of the getter stream into a Vec
    let g_results = getter
        .take(200)
        .enumerate()
        .map(|(i, a)| {
            let b = a.unwrap();
            println!("{} {:?}", i, b);
            b
        })
        .collect::<Vec<_>>()
        .await;
    // Use a range directly on the DB to get our comparison data.
    let r_results = tree
        .range(IVec::from(prefix)..)
        .take(200)
        .flatten()
        .map(|(k, v)| {
            (
                u64::decode(&k[prefix.len()..]).unwrap(),
                Test1::decode(v).unwrap(),
            )
        });
    // Compare
    if g_results
        .iter()
        .zip(r_results)
        .all(|((k1, v1), (k2, v2))| *k1 == k2 && *v1 == v2)
    {
        println!("We have a match!");
    } else {
        println!("D:")
    }
    Ok(())
}

async fn manifold() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let config = Config::new().set_dir("/tmp/shed_manifold");
    let shed = Shed::new("demo", &config);
    let prefix1 = b"test1/foo";
    let prefix2 = b"test2/bar";
    let prefix3 = b"test3/baz";
    let s = stream! {
        for i in 0..20u64 {
            yield (i + 100, Test1{ a:i, b: 42});
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    let t = stream! {
        for i in 0..20u64 {
            yield (i, Test2{ s: (i*355).to_string(), weee: i as u128 * 1_000_000_000});
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    let u = stream! {
        for i in 0..20u64 {
            yield (i, Test3{ a: i as i32 * 3, huh: i.to_string()});
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };

    // create a stream by subscribing to the prefix
    let mut mani = shed.manifold_subscribe::<TestManifold>();

    // create the writers and spawn it to pump the data into the DB
    let s = shed.pipe_put(prefix1.to_vec(), s);
    let t = shed.pipe_put(prefix2.to_vec(), t);
    let u = shed.pipe_put(prefix3.to_vec(), u);
    tokio::spawn(s);
    tokio::spawn(t);
    tokio::spawn(u);

    let mut i = 60;
    while let Some(foo) = mani.next().await {
        if let Ok(m) = foo {
            match m {
                TestManifold::Test1Item((key, Test1 { a, b })) => {
                    println!("key={} a={}, b={}", key, a, b)
                }
                TestManifold::Test2Item((key, Test2 { s, weee })) => {
                    println!("key={} s={}, weee={}", key, s, weee)
                }
                TestManifold::Test3Item((key, Test3 { a, huh })) => {
                    println!("key={} a={}, huh={}", key, a, huh)
                }
            }
        }
        i -= 1;
        if i < 1 {
            break;
        }
    }
    Ok(())
}
