use anyhow::{bail, Result};
use fnv::FnvHashMap;
use lob_processor::{BookSide, Lob};
use std::env;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        bail!("Usage: {} <snapshot.bin> <incremental.bin>", args[0]);
    }

    let processor = lob_processor::improved::ImprovedProcessor::new();
    let books = processor.process_files(&args[1], &args[2])?;

    print_order_books(&books);

    Ok(())
}

fn print_order_books<T>(books: &FnvHashMap<u64, Lob<T>>)
where
    T: BookSide,
{
    let mut sorted_ids: Vec<_> = books.keys().copied().collect();
    sorted_ids.sort();

    for id in sorted_ids {
        let book = &books[&id];
        println!("sec id {}", book.security_id);

        println!("bids:");
        for level in book.bids.get_l() {
            println!("  {:.2} --- {}", level.price, level.quantity);
        }

        println!("asks:");
        for level in book.asks.get_l() {
            println!("  {:.2} --- {}", level.price, level.quantity);
        }

        println!();
    }
}
