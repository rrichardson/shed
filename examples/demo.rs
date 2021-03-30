extern crate shed;

use derive_manifold::Manifold;

use futures;
use shed::{ Shed, Pipe, Encodable, Manifold, Store, Source, Config };
use futures::{ StreamExt, Stream, pin_mut };
use serde::{Serialize, Deserialize };
use async_stream::stream;
use sled::IVec;
use anyhow;
use std::pin::Pin;

#[derive(Manifold)]
enum Sauce {
    #[prefix="test1/foo"]
    Test1Item(Test1),
    #[prefix="test2/bar"]
    Test2Item(Test2),
    #[prefix="test3/baz"]
    Test3Item(Test3)
}

#[derive(Serialize, Deserialize, Debug)]
struct Test1 {
    a: u64,
    b: i16,
}

#[derive(Serialize, Deserialize)]
struct Test2{
    s: String,
    weee: u128,
}

#[derive(Serialize, Deserialize)]
struct Test3 {
    a: i32,
    huh: String,
}

fn test_mani<M: Manifold>() -> String {
    format!("If it compiles, it works")
}

#[test]
fn lick_sauce() {
    let _mani = test_mani::<Sauce>();
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let config = Config::new().set_dir("/tmp/shed_demo");
    let shed = Shed::new("demo", &config);
    let db = shed.clone().tree;
    let s = stream! {
        for i in 0..300 {
            yield (i + 100, Test1{ a:i, b: 42});
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    };
    let put = shed.pipe_put(b"test1/foo".to_vec(), s);
    let res = put.await;
    db.flush()?;
    //tokio::spawn(put);
    let mut i = 5;
    let start = IVec::from(b"test/foo");
    let mut rng = db.range(start..);
    while let Some(Ok(item)) = rng.next() {
        //let k = u64::decode(&item.0[9..])?;
        //let v = Test1::decode(item.1)?;
        println!("{:?} == {:?}", item.0, item.1);
        i -= 1;
        if i == 0 { break; }
    }

    //println!("{} {:?}", test_mani::<Sauce>(), res);
    Ok(())
}
