use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledStore};
use tempfile::TempDir;

fn bench_kv_write(c: &mut Criterion) {
    c.bench_function("KVS Write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                KvStore::open(&temp_dir.path()).unwrap()
            },
            |mut store| {
                for i in 1..(1 << 3) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_sled_write(c: &mut Criterion) {
    c.bench_function("Sled Write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                SledStore::open(&temp_dir.path()).unwrap()
            },
            |mut store| {
                for i in 1..(1 << 3) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_kv_write, bench_sled_write);
criterion_main!(benches);
