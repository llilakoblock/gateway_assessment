use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lob_processor::{basic::BasicProcessor, improved::ImprovedProcessor};
use std::fs;
use std::io::Write;
use std::path::Path;
use tempfile::tempdir;

struct TestDataGenerator {
    num_securities: u64,
    num_incrementals: u64,
}

impl TestDataGenerator {
    fn new(num_securities: u64, num_incrementals: u64) -> Self {
        Self {
            num_securities,
            num_incrementals,
        }
    }

    fn generate_snapshot_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = fs::File::create(path)?;

        for sec_id in 1..=self.num_securities {
            // Snapshot header
            file.write_all(&1000u64.to_le_bytes())?;
            file.write_all(&0u64.to_le_bytes())?;
            file.write_all(&sec_id.to_le_bytes())?;

            // 5 bid and ask levels
            for i in 0..5 {
                let bid_price = 100.0 - (i as f64) * 0.01;
                let ask_price = 100.0 + (i as f64) * 0.01;

                file.write_all(&bid_price.to_bits().to_le_bytes())?;
                file.write_all(&(100 * (5 - i) as u64).to_le_bytes())?;

                file.write_all(&ask_price.to_bits().to_le_bytes())?;
                file.write_all(&(100 * (5 - i) as u64).to_le_bytes())?;
            }
        }

        Ok(())
    }

    fn generate_incremental_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = fs::File::create(path)?;

        for msg_idx in 1..=self.num_incrementals {
            let security_id = (msg_idx % self.num_securities) + 1;
            let num_updates = 3u64;

            file.write_all(&(1000 + msg_idx).to_le_bytes())?;
            file.write_all(&msg_idx.to_le_bytes())?;
            file.write_all(&security_id.to_le_bytes())?;
            file.write_all(&num_updates.to_le_bytes())?;

            for i in 0..num_updates {
                let side = (i % 2) as u8;
                let price = if side == 0 {
                    99.99 - (i as f64) * 0.01
                } else {
                    100.01 + (i as f64) * 0.01
                };
                let qty = if i == 0 { 0 } else { 200 + i * 50 };

                file.write_all(&[side])?;
                file.write_all(&price.to_bits().to_le_bytes())?;
                file.write_all(&qty.to_le_bytes())?;
            }
        }

        Ok(())
    }
}

fn bench_implementations(c: &mut Criterion) {
    let temp_dir = tempdir().expect("Failed to create temp directory");

    let test_cases = vec![
        ("small", 10, 500),
        ("medium", 25, 5000),
        ("large", 100, 50_000),
    ];

    for (name, num_securities, num_incrementals) in test_cases {
        let snapshot_path = temp_dir.path().join(format!("snapshot_{}.bin", name));
        let incremental_path = temp_dir.path().join(format!("incremental_{}.bin", name));

        let generator = TestDataGenerator::new(num_securities, num_incrementals);
        generator
            .generate_snapshot_file(&snapshot_path)
            .expect("Failed to generate snapshot");
        generator
            .generate_incremental_file(&incremental_path)
            .expect("Failed to generate incrementals");

        let mut group = c.benchmark_group(format!("orderbook_{}", name));

        group.bench_with_input(
            BenchmarkId::new("Improved", name),
            &(&snapshot_path, &incremental_path),
            |b, (snap, inc)| {
                let processor = BasicProcessor::new();
                b.iter(|| {
                    std::hint::black_box(
                        processor
                            .process_files(snap.to_str().unwrap(), inc.to_str().unwrap())
                            .unwrap(),
                    )
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("HFT", name),
            &(&snapshot_path, &incremental_path),
            |b, (snap, inc)| {
                let processor = ImprovedProcessor::new();
                b.iter(|| {
                    std::hint::black_box(
                        processor
                            .process_files(snap.to_str().unwrap(), inc.to_str().unwrap())
                            .unwrap(),
                    )
                });
            },
        );

        group.finish();
    }
}

fn bench_single_operations(c: &mut Criterion) {
    use lob_processor::basic::Basic;
    use lob_processor::improved::ImprovedSide;
    use lob_processor::{BookSide, Side};

    let mut group = c.benchmark_group("single_operations");

    let updates: Vec<(Side, f64, u64)> = (0..100)
        .map(|i| {
            let side = if i % 3 == 0 { Side::B } else { Side::A };
            let price = 100.0 + (i as f64 % 10.0) * 0.01;
            let qty = if i % 10 == 0 { 0 } else { 100 + i };
            (side, price, qty)
        })
        .collect();

    group.bench_function("Improved_updates", |b| {
        b.iter(|| {
            let mut book_side = Basic::new(true);
            for (_side, price, qty) in &updates {
                if *qty == 0 {
                    book_side.remove_l(*price);
                } else {
                    book_side.update_l(*price, *qty);
                }
            }
            std::hint::black_box(book_side.get_l());
        });
    });

    group.bench_function("HFT_updates", |b| {
        b.iter(|| {
            let mut book_side = ImprovedSide::new(true);
            for (_side, price, qty) in &updates {
                if *qty == 0 {
                    book_side.remove_l(*price);
                } else {
                    book_side.update_l(*price, *qty);
                }
            }
            std::hint::black_box(book_side.get_l());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_implementations, bench_single_operations);
criterion_main!(benches);
