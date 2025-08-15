use crate::*;
use anyhow::{bail, Context, Result};
use core_affinity;
use crossbeam::channel::Receiver;
use fnv::FnvHashMap;
use memmap2::Mmap;
use std::fs::File;
use std::ptr;

const MAX_LEVELS: usize = 32;

// align to 32 bytes
#[repr(C, align(32))]
#[derive(Clone)]
pub struct ImprovedSide {
    prices: [f64; MAX_LEVELS],
    qtys: [Qty; MAX_LEVELS],
    count: usize,
    is_b: bool,
}

impl ImprovedSide {
    pub fn new(is_bid: bool) -> Self {
        Self {
            prices: [0.0; MAX_LEVELS],
            qtys: [0; MAX_LEVELS],
            count: 0,
            is_b: is_bid,
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    fn find_position(&self, price: f64) -> Result<usize, usize> {
        use std::arch::x86_64::*;

        if self.count == 0 {
            return Err(0);
        }

        unsafe {
            // fill avx with price
            let price_vec = _mm256_set1_pd(price);
            let mut i = 0;

            // chekc 4 prices
            while i + 4 <= self.count {
                // load 4 from vec
                let prices = _mm256_loadu_pd(&self.prices[i]);

                // compare
                let eq_mask = _mm256_cmp_pd(prices, price_vec, _CMP_EQ_OQ);
                // compare mask 4 bits
                let eq_bits = _mm256_movemask_pd(eq_mask);

                if eq_bits != 0 {
                    // found our position by zeros
                    return Ok(i + eq_bits.trailing_zeros() as usize);
                }

                // where to insert
                let cmp_mask = if self.is_b {
                    // for bids: greater comparsion
                    _mm256_cmp_pd(price_vec, prices, _CMP_GT_OQ)
                } else {
                    // for asks: lesser comparsion
                    _mm256_cmp_pd(price_vec, prices, _CMP_LT_OQ)
                };

                let cmp_bits = _mm256_movemask_pd(cmp_mask);
                if cmp_bits != 0 {
                    // found where to insert
                    return Err(i + cmp_bits.trailing_zeros() as usize);
                }

                i += 4;
            }

            // handle remaining elements
            while i < self.count {
                if price == self.prices[i] {
                    return Ok(i);
                }
                if self.is_b {
                    if price > self.prices[i] {
                        return Err(i);
                    }
                } else if price < self.prices[i] {
                    return Err(i);
                }
                i += 1;
            }

            Err(self.count)
        }
    }
}

impl BookSide for ImprovedSide {
    #[inline(always)]
    fn update_l(&mut self, price: f64, qty: Qty) {
        // fast path
        if self.count > 0 && self.prices[0] == price {
            self.qtys[0] = qty;
            return;
        }

        match self.find_position(price) {
            Ok(pos) => {
                //  just update quantity
                self.qtys[pos] = qty;
            }
            Err(pos) => {
                //new price level
                if self.count < MAX_LEVELS {
                    if pos < self.count {
                        unsafe {
                            // shift everything to the right
                            ptr::copy(
                                self.prices.as_ptr().add(pos),
                                self.prices.as_mut_ptr().add(pos + 1),
                                self.count - pos,
                            );
                            ptr::copy(
                                self.qtys.as_ptr().add(pos),
                                self.qtys.as_mut_ptr().add(pos + 1),
                                self.count - pos,
                            );
                        }
                    }

                    self.prices[pos] = price;
                    self.qtys[pos] = qty;
                    self.count += 1;
                }
            }
        }
    }

    #[inline(always)]
    fn remove_l(&mut self, price: f64) {
        // fast path
        if self.count > 0 && self.prices[0] == price {
            self.count -= 1;
            if self.count > 0 {
                unsafe {
                    // shift everything to the left
                    ptr::copy(
                        self.prices.as_ptr().add(1),
                        self.prices.as_mut_ptr(),
                        self.count,
                    );
                    ptr::copy(
                        self.qtys.as_ptr().add(1),
                        self.qtys.as_mut_ptr(),
                        self.count,
                    );
                }
            }
            return;
        }

        if let Ok(pos) = self.find_position(price) {
            self.count -= 1;
            if pos < self.count {
                unsafe {
                    // shift everything after it to the left
                    ptr::copy(
                        self.prices.as_ptr().add(pos + 1),
                        self.prices.as_mut_ptr().add(pos),
                        self.count - pos,
                    );
                    ptr::copy(
                        self.qtys.as_ptr().add(pos + 1),
                        self.qtys.as_mut_ptr().add(pos),
                        self.count - pos,
                    );
                }
            }
        }
    }

    fn get_l(&self) -> Vec<Level> {
        let mut result = Vec::with_capacity(self.count);
        for i in 0..self.count {
            result.push(Level {
                price: self.prices[i],
                quantity: self.qtys[i],
            });
        }
        result
    }
}

pub struct ImprovedProcessor;

impl ImprovedProcessor {
    #[inline(always)]
    pub fn new() -> Self {
        Self
    }

    pub fn process_files(
        &self,
        snapshot_path: &str,
        incremental_path: &str,
    ) -> Result<FnvHashMap<SecurityId, Lob<ImprovedSide>>> {
        let snapshot_file = File::open(snapshot_path)?;
        let incremental_file = File::open(incremental_path)?;

        let snapshot_mmap = unsafe { Mmap::map(&snapshot_file)? };
        let incremental_mmap = unsafe { Mmap::map(&incremental_file)? };

        // fnv hasher is faster
        let mut books = FnvHashMap::with_capacity_and_hasher(
            (snapshot_mmap.len() / SNAPSHOT_SIZE).min(1024),
            Default::default(),
        );
        let mut offset = 0;
        let mut max_snapshot_seq = 0u64;

        // everything inline for speed
        while offset + SNAPSHOT_SIZE <= snapshot_mmap.len() {
            unsafe {
                let ptr = snapshot_mmap.as_ptr().add(offset);

                // read without structs (faster, no prefetching needed cpu handles it better)
                let _timestamp = ptr::read_unaligned(ptr as *const u64);
                let seq_no = ptr::read_unaligned(ptr.add(8) as *const u64);
                let security_id = ptr::read_unaligned(ptr.add(16) as *const u64);

                max_snapshot_seq = max_snapshot_seq.max(seq_no);

                let mut book = Lob::new(
                    security_id,
                    ImprovedSide::new(true),
                    ImprovedSide::new(false),
                );
                book.last_update_seq = Some(seq_no);

                let mut pos = 24;

                //batch insert them
                let mut bid_prices: [f64; 5] = [0.0; 5];
                let mut bid_quantities: [u64; 5] = [0; 5];
                let mut bid_count = 0;

                let mut ask_prices: [f64; 5] = [0.0; 5];
                let mut ask_quantities: [u64; 5] = [0; 5];
                let mut ask_count = 0;

                for _ in 0..5 {
                    let bid_price_bits = ptr::read_unaligned(ptr.add(pos) as *const u64);
                    let bid_qty = ptr::read_unaligned(ptr.add(pos + 8) as *const u64);
                    let ask_price_bits = ptr::read_unaligned(ptr.add(pos + 16) as *const u64);
                    let ask_qty = ptr::read_unaligned(ptr.add(pos + 24) as *const u64);

                    pos += 32;

                    // skip empty levels
                    if bid_price_bits != 0 && bid_qty != 0 {
                        bid_prices[bid_count] = f64::from_bits(bid_price_bits);
                        bid_quantities[bid_count] = bid_qty;
                        bid_count += 1;
                    }

                    if ask_price_bits != 0 && ask_qty != 0 {
                        ask_prices[ask_count] = f64::from_bits(ask_price_bits);
                        ask_quantities[ask_count] = ask_qty;
                        ask_count += 1;
                    }
                }

                // batch copy all bids at once
                if bid_count > 0 {
                    ptr::copy_nonoverlapping(
                        bid_prices.as_ptr(),
                        book.bids.prices.as_mut_ptr(),
                        bid_count,
                    );
                    ptr::copy_nonoverlapping(
                        bid_quantities.as_ptr(),
                        book.bids.qtys.as_mut_ptr(),
                        bid_count,
                    );
                    book.bids.count = bid_count;
                }

                // same for asks
                if ask_count > 0 {
                    ptr::copy_nonoverlapping(
                        ask_prices.as_ptr(),
                        book.asks.prices.as_mut_ptr(),
                        ask_count,
                    );
                    ptr::copy_nonoverlapping(
                        ask_quantities.as_ptr(),
                        book.asks.qtys.as_mut_ptr(),
                        ask_count,
                    );
                    book.asks.count = ask_count;
                }

                books.insert(security_id, book);
            }

            offset += SNAPSHOT_SIZE;
        }

        offset = 0;

        while offset + INCREMENTAL_HEADER_SIZE <= incremental_mmap.len() {
            unsafe {
                let ptr = incremental_mmap.as_ptr().add(offset);

                let _timestamp = ptr::read_unaligned(ptr as *const u64);
                let seq_no = ptr::read_unaligned(ptr.add(8) as *const u64);
                let security_id = ptr::read_unaligned(ptr.add(16) as *const u64);
                let num_updates = ptr::read_unaligned(ptr.add(24) as *const u64);

                let mut pos = 32;

                //sanity check
                if offset + pos + (num_updates as usize * INCREMENTAL_SIZE) > incremental_mmap.len()
                {
                    bail!("Not enough data for updates");
                }

                if seq_no > max_snapshot_seq {
                    if let Some(book) = books.get_mut(&security_id) {
                        // hot path
                        for _ in 0..num_updates {
                            let side = *ptr.add(pos);
                            let price_bits = ptr::read_unaligned(ptr.add(pos + 1) as *const u64);
                            let price = f64::from_bits(price_bits);
                            let qty = ptr::read_unaligned(ptr.add(pos + 9) as *const u64);

                            match side {
                                0 => {
                                    // b update
                                    if qty == 0 {
                                        book.bids.remove_l(price);
                                    } else {
                                        book.bids.update_l(price, qty);
                                    }
                                }
                                1 => {
                                    // a update
                                    if qty == 0 {
                                        book.asks.remove_l(price);
                                    } else {
                                        book.asks.update_l(price, qty);
                                    }
                                }
                                _ => {
                                    //never happen with valid data
                                    debug_assert!(false, "Invalid side: {}", side);
                                }
                            }

                            pos += INCREMENTAL_SIZE;
                        }

                        book.last_update_seq = Some(seq_no);
                    } else {
                        // cold path new book
                        let mut book = Lob::new(
                            security_id,
                            ImprovedSide::new(true),
                            ImprovedSide::new(false),
                        );

                        for _ in 0..num_updates {
                            let side = *ptr.add(pos);
                            let price_bits = ptr::read_unaligned(ptr.add(pos + 1) as *const u64);
                            let price = f64::from_bits(price_bits);
                            let qty = ptr::read_unaligned(ptr.add(pos + 9) as *const u64);

                            book.update(Side::from_u8(side).context("Invalid side")?, price, qty);

                            pos += INCREMENTAL_SIZE;
                        }

                        book.last_update_seq = Some(seq_no);
                        books.insert(security_id, book);
                    }
                } else {
                    // skip old
                    pos += num_updates as usize * INCREMENTAL_SIZE;
                }

                offset += pos;
            }
        }

        Ok(books)
    }

    pub fn process_stream(
        &self,
        receiver: Receiver<StreamMessage>,
        processor_core: usize,
    ) -> Result<FnvHashMap<SecurityId, Lob<ImprovedSide>>> {
        // pin this thread, same parsing logic,  no additional calls to parsing logic in separate method for both parses, redundant but faster
        core_affinity::set_for_current(core_affinity::CoreId { id: processor_core });

        let mut books = FnvHashMap::with_capacity_and_hasher(1024, Default::default());
        let mut in_snapshot_phase = true;
        let mut max_snapshot_seq = 0u64;

        loop {
            match receiver.recv() {
                Ok(StreamMessage::Data(msg_type, data)) => match msg_type {
                    MessageType::Snapshot if in_snapshot_phase => unsafe {
                        let ptr = data.as_ptr();

                        let _timestamp = ptr::read_unaligned(ptr as *const u64);
                        let seq_no = ptr::read_unaligned(ptr.add(8) as *const u64);
                        let security_id = ptr::read_unaligned(ptr.add(16) as *const u64);

                        max_snapshot_seq = max_snapshot_seq.max(seq_no);

                        let mut book = Lob::new(
                            security_id,
                            ImprovedSide::new(true),
                            ImprovedSide::new(false),
                        );
                        book.last_update_seq = Some(seq_no);

                        let mut bid_count = 0;
                        let mut ask_count = 0;
                        let mut pos = 24;

                        for _ in 0..5 {
                            let bid_price_bits = ptr::read_unaligned(ptr.add(pos) as *const u64);
                            let bid_qty = ptr::read_unaligned(ptr.add(pos + 8) as *const u64);
                            let ask_price_bits =
                                ptr::read_unaligned(ptr.add(pos + 16) as *const u64);
                            let ask_qty = ptr::read_unaligned(ptr.add(pos + 24) as *const u64);

                            if bid_price_bits != 0 && bid_qty != 0 {
                                book.bids.prices[bid_count] = f64::from_bits(bid_price_bits);
                                book.bids.qtys[bid_count] = bid_qty;
                                bid_count += 1;
                            }

                            if ask_price_bits != 0 && ask_qty != 0 {
                                book.asks.prices[ask_count] = f64::from_bits(ask_price_bits);
                                book.asks.qtys[ask_count] = ask_qty;
                                ask_count += 1;
                            }

                            pos += 32;
                        }

                        book.bids.count = bid_count;
                        book.asks.count = ask_count;

                        books.insert(security_id, book);
                    },
                    MessageType::Incremental if !in_snapshot_phase => unsafe {
                        let ptr = data.as_ptr();

                        let _timestamp = ptr::read_unaligned(ptr as *const u64);
                        let seq_no = ptr::read_unaligned(ptr.add(8) as *const u64);
                        let security_id = ptr::read_unaligned(ptr.add(16) as *const u64);
                        let num_updates = ptr::read_unaligned(ptr.add(24) as *const u64);

                        if seq_no > max_snapshot_seq {
                            let mut pos = 32;

                            if let Some(book) = books.get_mut(&security_id) {
                                for _ in 0..num_updates {
                                    let side = *ptr.add(pos);
                                    let price_bits =
                                        ptr::read_unaligned(ptr.add(pos + 1) as *const u64);
                                    let price = f64::from_bits(price_bits);
                                    let qty = ptr::read_unaligned(ptr.add(pos + 9) as *const u64);

                                    if side == 0 {
                                        if qty == 0 {
                                            book.bids.remove_l(price);
                                        } else {
                                            book.bids.update_l(price, qty);
                                        }
                                    } else if side == 1 {
                                        if qty == 0 {
                                            book.asks.remove_l(price);
                                        } else {
                                            book.asks.update_l(price, qty);
                                        }
                                    }

                                    pos += INCREMENTAL_SIZE;
                                }

                                book.last_update_seq = Some(seq_no);
                            } else {
                                let mut book = Lob::new(
                                    security_id,
                                    ImprovedSide::new(true),
                                    ImprovedSide::new(false),
                                );

                                for _ in 0..num_updates {
                                    let side = *ptr.add(pos);
                                    let price_bits =
                                        ptr::read_unaligned(ptr.add(pos + 1) as *const u64);
                                    let price = f64::from_bits(price_bits);
                                    let qty = ptr::read_unaligned(ptr.add(pos + 9) as *const u64);

                                    if side == 0 {
                                        book.update(Side::B, price, qty);
                                    } else if side == 1 {
                                        book.update(Side::A, price, qty);
                                    }

                                    pos += INCREMENTAL_SIZE;
                                }

                                book.last_update_seq = Some(seq_no);
                                books.insert(security_id, book);
                            }
                        }
                    },
                    _ => {}
                },
                Ok(StreamMessage::EndOfSnapshot) => {
                    in_snapshot_phase = false;
                    eprintln!("Snapshot phase completed, max_seq: {}", max_snapshot_seq);
                }
                Err(_) => break,
            }
        }

        Ok(books)
    }
}

impl Default for ImprovedProcessor {
    fn default() -> Self {
        Self::new()
    }
}
