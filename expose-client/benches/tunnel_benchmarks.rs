use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn benchmark_frame_cloning(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_cloning");

    for size in [1024usize, 10_240, 102_400, 1_024_000] {
        let data = vec![0u8; size];

        group.bench_with_input(BenchmarkId::new("vec_clone", size), &size, |b, _| {
            b.iter(|| {
                let cloned = data.clone();
                black_box(cloned);
            })
        });

        group.bench_with_input(BenchmarkId::new("bytes_clone", size), &size, |b, _| {
            let bytes = Bytes::from(data.clone());
            b.iter(|| {
                let cloned = bytes.clone();
                black_box(cloned);
            })
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_frame_cloning);
criterion_main!(benches);
