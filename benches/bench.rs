use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledStore};
use tempfile::TempDir;

fn bench_kv_set(c: &mut Criterion) {
    c.bench_function("KVS set", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                KvStore::open(&temp_dir.path()).unwrap()
            },
            |mut store| {
                for i in 1..(1 << 10) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_kv_get(c: &mut Criterion) {
    c.bench_function("KVS get", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut store = KvStore::open(&temp_dir.path()).unwrap();
                for i in 1..(1 << 10) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
                store
            },
            |mut store| {
                for i in 1..(1 << 10) {
                    assert_eq!(
                        "value".to_string(),
                        store.get(format!("key{}", i)).unwrap().unwrap()
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_sled_set(c: &mut Criterion) {
    c.bench_function("Sled set", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                SledStore::open(&temp_dir.path()).unwrap()
            },
            |mut store| {
                for i in 1..(1 << 10) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_sled_get(c: &mut Criterion) {
    c.bench_function("Sled get", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let mut store = SledStore::open(&temp_dir.path()).unwrap();
                for i in 1..(1 << 10) {
                    store.set(format!("key{}", i), "value".to_string()).unwrap();
                }
                store
            },
            |mut store| {
                for i in 1..(1 << 10) {
                    assert_eq!(
                        "value".to_string(),
                        store.get(format!("key{}", i)).unwrap().unwrap()
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    bench_kv_set,
    bench_kv_get,
    bench_sled_set,
    bench_sled_get
);
criterion_main!(benches);
