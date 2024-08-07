use crate::store::Block;
use anyhow::Result;
use croaring::Treemap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub struct MemoryBuffer {
    buffer: RwLock<BufferInternal>,
}

#[derive(Default, Debug)]
pub struct BatchMemoryBlock(Vec<Vec<Block>>);
impl Deref for BatchMemoryBlock {
    type Target = Vec<Vec<Block>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for BatchMemoryBlock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct BufferSpillResult {
    flight_id: u64,
    flight_len: u64,
    blocks: Arc<BatchMemoryBlock>,
}

impl BufferSpillResult {
    pub fn flight_id(&self) -> u64 {
        self.flight_id
    }
    pub fn flight_len(&self) -> u64 {
        self.flight_len
    }
    pub fn blocks(&self) -> Arc<BatchMemoryBlock> {
        self.blocks.clone()
    }
}

#[derive(Debug)]
pub struct BufferReadResult {
    read_len: u64,
    blocks: Vec<Block>,
}

impl BufferReadResult {
    pub fn read_len(&self) -> u64 {
        self.read_len
    }
    pub fn blocks(&self) -> &Vec<Block> {
        &self.blocks
    }
}

#[derive(Debug)]
pub struct BufferInternal {
    total_size: i64,
    staging_size: i64,
    flight_size: i64,

    staging: BatchMemoryBlock,

    flight: HashMap<u64, Arc<BatchMemoryBlock>>,
    flight_counter: u64,
}

impl BufferInternal {
    fn new() -> Self {
        BufferInternal {
            total_size: 0,
            staging_size: 0,
            flight_size: 0,
            staging: Default::default(),
            flight: Default::default(),
            flight_counter: 0,
        }
    }
}

impl MemoryBuffer {
    pub fn new() -> MemoryBuffer {
        MemoryBuffer {
            buffer: RwLock::new(BufferInternal::new()),
        }
    }

    pub fn total_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().total_size);
    }

    pub fn flight_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().flight_size);
    }

    pub fn staging_size(&self) -> Result<i64> {
        return Ok(self.buffer.read().staging_size);
    }

    pub fn clear(&self, flight_id: u64, flight_size: u64) -> Result<()> {
        let mut buffer = self.buffer.write();
        let flight = &mut buffer.flight;
        let removed = flight.remove(&flight_id);
        if let Some(block_ref) = removed {
            buffer.total_size -= flight_size as i64;
            buffer.flight_size -= flight_size as i64;
        }
        Ok(())
    }

    /// todo: avoid cloning.
    pub fn get(
        &self,
        last_block_id: i64,
        batch_len: i64,
        task_ids: Option<Treemap>,
    ) -> Result<BufferReadResult> {
        /// read sequence
        /// 1. from flight (expect: last_block_id not found or last_block_id == 0)
        /// 2. from staging
        let buffer = self.buffer.read();

        let mut read_result = vec![];
        let mut read_len = 0i64;
        let mut flight_found = false;

        let mut exit = false;
        while !exit {
            exit = true;
            {
                if last_block_id == 0 {
                    flight_found = true;
                }
                for (_, batch_block) in buffer.flight.iter() {
                    for blocks in batch_block.iter() {
                        for block in blocks {
                            if !flight_found && block.block_id == last_block_id {
                                flight_found = true;
                                continue;
                            }
                            if !flight_found {
                                continue;
                            }
                            if read_len >= batch_len {
                                break;
                            }
                            read_len += block.length as i64;
                            read_result.push(block.clone());
                        }
                    }
                }
            }

            {
                for blocks in buffer.staging.iter() {
                    for block in blocks {
                        if !flight_found && block.block_id == last_block_id {
                            flight_found = true;
                            continue;
                        }
                        if !flight_found {
                            continue;
                        }
                        if read_len >= batch_len {
                            break;
                        }
                        read_len += block.length as i64;
                        read_result.push(block.clone());
                    }
                }
            }

            if !flight_found {
                flight_found = true;
                exit = false;
            }
        }

        Ok(BufferReadResult {
            read_len: read_len as u64,
            blocks: read_result,
        })
    }

    pub fn spill(&self) -> Result<BufferSpillResult> {
        let mut buffer = self.buffer.write();
        let staging: BatchMemoryBlock = { mem::replace(&mut buffer.staging, Default::default()) };
        let staging_ref = Arc::new(staging);
        let flight_id = buffer.flight_counter;

        let flight = &mut buffer.flight;
        flight.insert(flight_id, staging_ref.clone());

        let spill_size = buffer.staging_size;
        buffer.flight_counter += 1;
        buffer.flight_size += spill_size;
        buffer.staging_size = 0;

        Ok(BufferSpillResult {
            flight_id,
            flight_len: spill_size as u64,
            blocks: staging_ref.clone(),
        })
    }

    pub fn append(&self, blocks: Vec<Block>, size: u64) -> Result<()> {
        let mut buffer = self.buffer.write();
        let mut staging = &mut buffer.staging;
        staging.push(blocks);

        buffer.staging_size += size as i64;
        buffer.total_size += size as i64;

        Ok(())
    }
}

/// for tests.
impl MemoryBuffer {
    fn direct_push(&self, blocks: Vec<Block>) -> Result<()> {
        let len: u64 = blocks.iter().map(|block| block.length).sum::<i32>() as u64;
        self.append(blocks, len)
    }
}

#[cfg(test)]
mod test {
    use crate::store::mem::buffer::MemoryBuffer;
    use crate::store::Block;
    use hashlink::LinkedHashMap;
    use std::collections::LinkedList;
    use std::ops::Deref;
    use std::sync::RwLock;

    fn create_blocks(start_block_idx: i32, cnt: i32, block_len: i32) -> Vec<Block> {
        let mut blocks = vec![];
        for idx in 0..cnt {
            blocks.push(Block {
                block_id: (start_block_idx + idx) as i64,
                length: block_len,
                uncompress_length: 0,
                crc: 0,
                data: Default::default(),
                task_attempt_id: idx as i64,
            });
        }
        return blocks;
    }

    #[test]
    fn test_put_get() -> anyhow::Result<()> {
        let mut buffer = MemoryBuffer::new();

        /// case1
        buffer.direct_push(create_blocks(0, 10, 10))?;
        assert_eq!(10 * 10, buffer.total_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        /// case2
        buffer.direct_push(create_blocks(10, 10, 10))?;
        assert_eq!(10 * 10 * 2, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        /// case3: make all staging to spill
        let spill_result = buffer.spill()?;
        assert_eq!(10 * 10 * 2, spill_result.flight_len);
        assert_eq!(2, spill_result.blocks.len());
        assert_eq!(
            10 * 2,
            spill_result
                .blocks
                .deref()
                .iter()
                .flat_map(|x| x.iter())
                .count()
        );
        assert_eq!(10 * 10 * 2, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.flight_size()?);
        assert_eq!(0, buffer.staging_size()?);

        /// case4: write blocks into staging and then reading
        buffer.direct_push(create_blocks(20, 10, 10))?;
        assert_eq!(10 * 10 * 3, buffer.total_size()?);
        assert_eq!(10 * 10 * 2, buffer.flight_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);

        /// case5: read from the flight. expected blockId: 0 -> 9
        let read_result = buffer.get(0, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.read_len);
        assert_eq!(10, read_result.blocks.len());
        assert_eq!(9, read_result.blocks.last().unwrap().block_id);

        /// case6: read from flight again. expected blockId: 10 -> 19
        let read_result = buffer.get(9, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.read_len);
        assert_eq!(10, read_result.blocks.len());
        assert_eq!(19, read_result.blocks.last().unwrap().block_id);

        /// case7: read from staging. expected blockId: 20 -> 29
        let read_result = buffer.get(19, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.read_len);
        assert_eq!(10, read_result.blocks.len());
        assert_eq!(29, read_result.blocks.last().unwrap().block_id);

        /// case8: blockId not found, and then read from the flight -> staging.
        let read_result = buffer.get(100, 10 * 10, None)?;
        assert_eq!(10 * 10, read_result.read_len);
        assert_eq!(10, read_result.blocks.len());
        assert_eq!(9, read_result.blocks.last().unwrap().block_id);

        /// case9: remove the spill result after flushed
        let flight_id = spill_result.flight_id;
        let flight_len = spill_result.flight_len;
        buffer.clear(flight_id, flight_len)?;
        assert_eq!(10 * 10, buffer.total_size()?);
        assert_eq!(10 * 10, buffer.staging_size()?);
        assert_eq!(0, buffer.flight_size()?);

        Ok(())
    }

    #[test]
    fn test_linked_hashmap() {
        let mut map = LinkedHashMap::new();
        map.insert(1, 1);
        map.insert(2, 2);
        map.insert(3, 3);

        assert_eq!(1, *map.front().unwrap().0);
        assert_eq!(3, *map.back().unwrap().0);
    }

    #[test]
    fn lifetime_with_lock_test() {
        struct MyStruct {
            vec: Vec<i64>,
        }

        struct LockedStruct {
            lock: RwLock<i64>,
            inner: MyStruct,
        }

        impl LockedStruct {
            fn get_ref(&self) -> &Vec<i64> {
                let _guarder = self.lock.read().unwrap();
                &self.inner.vec
            }
        }

        let data = LockedStruct {
            lock: Default::default(),
            inner: MyStruct { vec: vec![1, 2, 3] },
        };
        assert_eq!(3, data.get_ref().len());
    }

    #[test]
    fn single_lifetime_test() {
        struct MyStruct {
            vec: Vec<i32>,
        }

        impl MyStruct {
            fn get_element<'a>(&'a self, index: usize) -> Option<&'a i32> {
                self.vec.get(index)
            }
        }

        let my_struct = MyStruct {
            vec: vec![1, 2, 3, 4, 5],
        };

        let locked = RwLock::new(my_struct);
        let guarder = locked.read().unwrap();
        if let Some(val) = guarder.get_element(2) {
            println!("The value is: {}", val);
        } else {
            println!("No value found at that index");
        }
    }

    #[test]
    fn test_linkedlist() {
        let mut list: Vec<LinkedList<i32>> = vec![LinkedList::new()];
        let mut a = list.get_mut(0).unwrap();
        a.push_front(1);

        let data = list.remove(0);
        list.push(LinkedList::new());
    }
}
