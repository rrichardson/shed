use futures;
use derive_manifold::Manifold;
use shed::{ Pipe, Encodable, Manifold, Store, Source };
use futures::{ StreamExt, Stream };
use serde::{Serialize, Deserialize };
use anyhow;
use std::pin::Pin;

#[derive(Manifold)]
enum Sauce {
    #[prefix="test1/baz"]
    Test1Item(Test1),
    #[prefix="test2/baz"]
    Test2Item(Test2),
    #[prefix="test3/baz"]
    Test3Item(Test3)
}

#[derive(Serialize, Deserialize)]
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
