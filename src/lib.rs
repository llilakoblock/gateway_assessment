pub mod basic;
pub mod improved;

pub type SecurityId = u64;
pub type SeqNo = u64;
pub type Qty = u64;

//Timestamp	u64	Timestamp in milliseconds
//SeqNo	u64	Sequence number of the last processed incremental
//SecurityID	u64	Identifier of the security
//BidPrice1	f64	The highest bid price
//BidQtyN	u64	Quantity at the highest bid price
//AskPriceN	f64	The lowest ask price
//AskQtyN	u64	Quantity at the lowest ask price
// 5 levels

pub const SNAPSHOT_SIZE: usize = 8 + 8 + 8 + (8 + 8) * 10;

//Timestamp	u64	Timestamp in milliseconds
//SeqNo	u64	Sequence number
//SecurityID	u64	Identifier of the security
//NumUpdates	u64	Number of updates in this incremental message

pub const INCREMENTAL_HEADER_SIZE: usize = 8 + 8 + 8 + 8;

//Side	u8	0 = Bid, 1 = Ask
//Price	f64	Price level of the update
//Qty 	u64	Updated quantity (if 0, level is removed)

pub const INCREMENTAL_SIZE: usize = 1 + 8 + 8;

// simple protocol for streaming, separate structs for snap and incr
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    Snapshot = 0b01,
    Incremental = 0b10,
    EndOfSnapshot = 0b11,
}

impl MessageType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0b01 => Some(MessageType::Snapshot),
            0b10 => Some(MessageType::Incremental),
            0b11 => Some(MessageType::EndOfSnapshot),
            _ => None,
        }
    }
}

pub enum StreamMessage {
    Data(MessageType, Vec<u8>),
    EndOfSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    B = 0,
    A = 1,
}

impl Side {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Side::B),
            1 => Some(Side::A),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub quantity: Qty,
}

pub trait BookSide {
    fn update_l(&mut self, price: f64, qty: Qty);
    fn remove_l(&mut self, price: f64);
    fn get_l(&self) -> Vec<Level>;
}

pub struct Lob<B: BookSide> {
    pub security_id: SecurityId,
    pub bids: B,
    pub asks: B,
    pub last_update_seq: Option<SeqNo>,
}

impl<B: BookSide> Lob<B> {
    #[inline(always)]
    pub fn new(security_id: SecurityId, bids: B, asks: B) -> Self {
        Self {
            security_id,
            bids,
            asks,
            last_update_seq: None,
        }
    }

    #[inline(always)]
    pub fn update(&mut self, side: Side, price: f64, qty: Qty) {
        match side {
            Side::B => {
                if qty == 0 {
                    self.bids.remove_l(price);
                } else {
                    self.bids.update_l(price, qty);
                }
            }
            Side::A => {
                if qty == 0 {
                    self.asks.remove_l(price);
                } else {
                    self.asks.update_l(price, qty);
                }
            }
        }
    }
}
