use crate::*;
use anyhow::{bail, Context, Result};
use crossbeam::channel::Receiver;
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;

// basic with sorted vector, float stored as float, simple binary search in sorted vec
#[derive(Default, Clone)]
pub struct Basic {
    pub levels: Vec<Level>,
    is_b: bool,
}

impl Basic {
    pub fn new(is_b: bool) -> Self {
        Self {
            levels: Vec::with_capacity(100),
            is_b,
        }
    }

    pub fn find_position(&self, price: f64) -> Result<usize, usize> {
        if self.is_b {
            // h to l for bids
            self.levels
                .binary_search_by(|level| price.partial_cmp(&level.price).unwrap())
        } else {
            // l to h for asks
            self.levels
                .binary_search_by(|level| level.price.partial_cmp(&price).unwrap())
        }
    }
}

impl BookSide for Basic {
    fn update_l(&mut self, price: f64, qty: Qty) {
        match self.find_position(price) {
            Ok(pos) => {
                self.levels[pos].quantity = qty;
            }
            Err(pos) => {
                //simple insert with vec shifting
                self.levels.insert(
                    pos,
                    Level {
                        price,
                        quantity: qty,
                    },
                );
            }
        }
    }

    fn remove_l(&mut self, price: f64) {
        if let Ok(pos) = self.find_position(price) {
            self.levels.remove(pos);
        }
    }

    fn get_l(&self) -> Vec<Level> {
        self.levels.clone()
    }
}

pub struct BasicProcessor;

//simple hashmap for basic implementations
impl BasicProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_files(
        &self,
        snapshot_path: &str,
        incremental_path: &str,
    ) -> Result<HashMap<SecurityId, Lob<Basic>>> {
        let snapshot_file = File::open(snapshot_path)
            .with_context(|| format!("Failed to open snapshot file: {}", snapshot_path))?;
        let incremental_file = File::open(incremental_path)
            .with_context(|| format!("Failed to open incremental file: {}", incremental_path))?;

        let snapshot_mmap = unsafe { Mmap::map(&snapshot_file)? };
        let incremental_mmap = unsafe { Mmap::map(&incremental_file)? };

        // get latest seq_no
        let mut books = HashMap::new();
        let mut offset = 0;
        let mut max_snapshot_seq = 0u64;

        while offset + SNAPSHOT_SIZE <= snapshot_mmap.len() {
            let (security_id, seq_no, book) = self.parse_snapshot(&snapshot_mmap, offset)?;
            max_snapshot_seq = max_snapshot_seq.max(seq_no);
            books.insert(security_id, book);
            offset += SNAPSHOT_SIZE;
        }

        // offset for mmaped file
        offset = 0;

        while offset + INCREMENTAL_HEADER_SIZE <= incremental_mmap.len() {
            let (new_offset, security_id, seq_no, updates) =
                self.parse_incremental(&incremental_mmap, offset)?;

            // check for ser_no
            if seq_no > max_snapshot_seq {
                if let Some(book) = books.get_mut(&security_id) {
                    // Apply updates
                    for (side, price, qty) in updates {
                        book.update(side, price, qty);
                    }
                    book.last_update_seq = Some(seq_no);
                } else {
                    // Create new book for securities not in snapshot
                    let mut new_book = Lob::new(security_id, Basic::new(true), Basic::new(false));

                    for (side, price, qty) in updates {
                        new_book.update(side, price, qty);
                    }
                    new_book.last_update_seq = Some(seq_no);

                    books.insert(security_id, new_book);
                }
            }

            offset = new_offset;
        }

        Ok(books)
    }

    pub fn process_stream(
        &self,
        receiver: Receiver<StreamMessage>,
        processor_core: usize,
    ) -> Result<HashMap<SecurityId, Lob<Basic>>> {
        //busy polling for channel read thread
        core_affinity::set_for_current(core_affinity::CoreId { id: processor_core });

        let mut books = HashMap::new();
        // imaginary protocol sends snapshot first, then incrementals
        let mut is_snapshot = true;
        let mut max_snapshot_seq = 0u64;

        loop {
            match receiver.recv() {
                Ok(StreamMessage::Data(msg_type, data)) => match msg_type {
                    MessageType::Snapshot if is_snapshot => {
                        let (security_id, seq_no, book) = self.parse_snapshot(&data, 0)?;
                        max_snapshot_seq = max_snapshot_seq.max(seq_no);
                        books.insert(security_id, book);
                    }
                    MessageType::Incremental if !is_snapshot => {
                        let (_, security_id, seq_no, updates) = self.parse_incremental(&data, 0)?;

                        if seq_no > max_snapshot_seq {
                            if let Some(book) = books.get_mut(&security_id) {
                                for (side, price, qty) in updates {
                                    book.update(side, price, qty);
                                }
                                book.last_update_seq = Some(seq_no);
                                //if sequence is broken assume this price\qtu as new
                            } else {
                                let mut new_book =
                                    Lob::new(security_id, Basic::new(true), Basic::new(false));

                                for (side, price, qty) in updates {
                                    new_book.update(side, price, qty);
                                }
                                new_book.last_update_seq = Some(seq_no);

                                books.insert(security_id, new_book);
                            }
                        }
                    }
                    _ => {}
                },
                Ok(StreamMessage::EndOfSnapshot) => {
                    is_snapshot = false;
                    eprintln!("Snapshot phase completed, max_seq: {}", max_snapshot_seq);
                }
                Err(_) => break,
            }
        }

        Ok(books)
    }

    fn parse_snapshot(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(SecurityId, SeqNo, Lob<Basic>)> {
        let mut pos = offset;

        //Timestamp	u64	Timestamp in milliseconds
        let _timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //SeqNo	u64	Sequence number of the last processed incremental
        let seq_no = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //SecurityID	u64	Identifier of the security
        let security_id = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        let mut book = Lob::new(security_id, Basic::new(true), Basic::new(false));
        //latest seq_no
        book.last_update_seq = Some(seq_no);

        let mut b_levels = Vec::new();
        let mut a_levels = Vec::new();

        // 5 levels (8+8) * 2 * 5
        for _ in 0..5 {
            let b_price_bits = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            let b_price = f64::from_bits(b_price_bits);
            pos += 8;

            let b_qty = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            pos += 8;

            if b_price > 0.0 && b_qty > 0 {
                b_levels.push((b_price, b_qty));
            }

            let ask_price_bits = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            let ask_price = f64::from_bits(ask_price_bits);
            pos += 8;

            let ask_qty = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            pos += 8;

            if ask_price > 0.0 && ask_qty > 0 {
                a_levels.push((ask_price, ask_qty));
            }
        }

        b_levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap()); // h to l
        a_levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap()); // l to h

        for (price, qty) in b_levels {
            book.bids.levels.push(Level {
                price,
                quantity: qty,
            });
        }

        for (price, qty) in a_levels {
            book.asks.levels.push(Level {
                price,
                quantity: qty,
            });
        }

        Ok((security_id, seq_no, book))
    }

    #[allow(clippy::type_complexity)]
    fn parse_incremental(
        &self,
        data: &[u8],
        offset: usize,
    ) -> Result<(usize, SecurityId, SeqNo, Vec<(Side, f64, Qty)>)> {
        let mut pos = offset;

        // read with checks

        //Timestamp	u64	Timestamp in milliseconds
        let _timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //SeqNo	u64	Sequence number
        let seq_no = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //SecurityID	u64	Identifier of the security
        let security_id = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //NumUpdates	u64	Number of updates in this incremental message
        let num_updates = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        //broken update edge case
        let updates_size = num_updates as usize * INCREMENTAL_SIZE;
        if pos + updates_size > data.len() {
            bail!("File is broken, not enough data");
        }
        // alloc but for basic implementation ok
        let mut updates = Vec::with_capacity(num_updates as usize);

        for _ in 0..num_updates {
            let side_byte = data[pos];
            pos += 1;

            //broken side check
            let side =
                Side::from_u8(side_byte).with_context(|| format!("Invalid side: {}", side_byte))?;

            let price_bits = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            let price = f64::from_bits(price_bits);
            pos += 8;

            let qty = u64::from_le_bytes(data[pos..pos + 8].try_into()?);
            pos += 8;

            updates.push((side, price, qty));
        }

        Ok((pos, security_id, seq_no, updates))
    }
}

impl Default for BasicProcessor {
    fn default() -> Self {
        Self::new()
    }
}
