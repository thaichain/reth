extern crate test;

use itertools::Itertools;
use reth_interfaces::{
    db::{tables, DbCursorRO, DbCursorRW, DbTx, DbTxMut},
    test_utils::generators::{random_header, random_header_range},
};
use reth_primitives::H256;

use crate::test_utils::TestStageDB;

const NUM_HEADERS: u64 = 100000;

fn init_db() -> TestStageDB {
    let db = TestStageDB::default();
    let head = random_header(0, None);
    let initial_db_headers = random_header_range(1..NUM_HEADERS, head.hash());
    db.commit(|tx| {
        let mut cursor = tx.cursor_mut::<tables::Headers>()?;
        cursor.append((head.number, head.hash()).into(), head.clone().unseal())?;
        initial_db_headers
            .into_iter()
            .try_for_each(|h| cursor.append((h.number, h.hash()).into(), h.unseal()))
    })
    .unwrap();
    db
}

#[bench]
fn bench_linear_put(b: &mut test::Bencher) {
    b.iter(|| {
        let db = init_db();
        let headers = random_header_range(NUM_HEADERS..NUM_HEADERS * 2, H256::zero());

        db.commit(|tx| {
            for header in headers {
                tx.put::<tables::Headers>((header.number, header.hash()).into(), header.unseal())
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();
    });
}

#[bench]
fn bench_linear_append(b: &mut test::Bencher) {
    b.iter(|| {
        let db = init_db();
        let headers = random_header_range(NUM_HEADERS..NUM_HEADERS * 2, H256::zero());

        db.commit(|tx| {
            let mut cursor = tx.cursor_mut::<tables::Headers>().unwrap();
            for header in headers {
                cursor.append((header.number, header.hash()).into(), header.unseal()).unwrap();
            }
            Ok(())
        })
        .unwrap();
    });
}

#[bench]
fn bench_reverse_put(b: &mut test::Bencher) {
    b.iter(|| {
        let db = init_db();
        let headers = random_header_range(NUM_HEADERS..NUM_HEADERS * 2, H256::zero());

        db.commit(|tx| {
            for chunk in &headers.into_iter().rev().chunks(100) {
                let mut chunk: Vec<_> = chunk.collect();
                chunk.reverse();
                for header in chunk {
                    tx.put::<tables::Headers>(
                        (header.number, header.hash()).into(),
                        header.unseal(),
                    )
                    .unwrap();
                }
            }

            Ok(())
        })
        .unwrap();
    });
}

#[bench]
fn bench_reverse_insert(b: &mut test::Bencher) {
    b.iter(|| {
        let db = init_db();
        let headers = random_header_range(NUM_HEADERS..NUM_HEADERS * 2, H256::zero());

        db.commit(|tx| {
            for chunk in &headers.into_iter().rev().chunks(100) {
                let mut chunk: Vec<_> = chunk.collect();
                chunk.reverse();
                let mut cursor = tx.cursor_mut::<tables::Headers>().unwrap();
                for header in chunk {
                    cursor.insert((header.number, header.hash()).into(), header.unseal()).unwrap();
                }
            }

            Ok(())
        })
        .unwrap();
    });
}

#[bench]
fn bench_reverse_seek_insert(b: &mut test::Bencher) {
    b.iter(|| {
        let db = init_db();
        let last = db.query(|tx| tx.cursor::<tables::Headers>()?.last()).unwrap().unwrap();
        let headers = random_header_range(NUM_HEADERS..NUM_HEADERS * 2, H256::zero());

        db.commit(|tx| {
            for chunk in &headers.into_iter().rev().chunks(100) {
                let mut chunk: Vec<_> = chunk.collect();
                chunk.reverse();
                let mut cursor = tx.cursor_mut::<tables::Headers>().unwrap();
                cursor.seek_exact(last.0).unwrap();
                for header in chunk {
                    cursor.insert((header.number, header.hash()).into(), header.unseal()).unwrap();
                }
            }

            Ok(())
        })
        .unwrap();
    });
}
