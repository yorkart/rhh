use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::mem;

struct HashElem<K, V>
where
    K: Eq + Hash,
{
    pub(crate) dist: u64,
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) hash: u64,
}

impl<K, V> HashElem<K, V>
where
    K: Eq + Hash,
{
    pub fn new(dist: u64, key: K, value: V, hash: u64) -> Self {
        Self {
            dist,
            key,
            value,
            hash,
        }
    }
}

impl<K, V> Debug for HashElem<K, V>
where
    K: Eq + Hash + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashElem")
            .field("dist", &self.dist)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("hash", &self.hash)
            .finish()
    }
}

pub struct HashMap<K, V>
where
    K: Eq + Hash + AsByte,
{
    // hashes: Vec<u64>,
    elems: Vec<Option<HashElem<K, V>>>,

    len: u64,
    capacity: u64,
    threshold: u64,
    mask: usize,
    load_factor: usize,
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash + AsByte,
{
    pub fn new() -> Self {
        Self::with_capacity(256)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_load_factor(capacity, 90)
    }

    pub fn with_capacity_load_factor(capacity: usize, load_factor: usize) -> Self {
        let mut elems = Vec::with_capacity(capacity);
        elems.resize_with(capacity, || None);
        Self {
            elems,
            len: 0,
            capacity: pow2(capacity as u64),
            threshold: (capacity as u64 * load_factor as u64) / 100,
            mask: capacity - 1,
            load_factor,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.index(key)
            .map(|i| &self.elems[i].as_ref().unwrap().value)
    }

    pub fn insert(&mut self, key: K, val: V) {
        // Grow the map if we've run out of slots.
        if self.len > self.threshold {
            self.grow();
        }

        // If the key was overwritten then decrement the size.
        let _overwritten = self.insert_raw(hash_key(&key), key, val);
    }

    fn grow(&mut self) {
        let mut new_map =
            Self::with_capacity_load_factor((self.capacity * 2) as usize, self.load_factor);
        for e in &mut self.elems {
            let e = e.take();
            if let Some(HashElem {
                hash, key, value, ..
            }) = e
            {
                new_map.insert_raw(hash, key, value);
            }
        }

        let HashMap {
            elems,
            len: n,
            capacity,
            threshold,
            mask,
            load_factor,
        } = new_map;
        self.elems = elems;
        self.len = n;
        self.capacity = capacity;
        self.threshold = threshold;
        self.mask = mask;
        self.load_factor = load_factor;
    }

    fn insert_raw(&mut self, hash: u64, key: K, val: V) -> bool {
        let mut pos = (hash & self.mask as u64) as usize;

        let mut dist = 0_u64;
        let mut entry = HashElem::new(dist, key, val, hash);

        // Continue searching until we find an empty slot or lower probe distance.
        loop {
            // Empty slot found or matching key, insert and exit.
            entry.dist = dist;
            match &self.elems[pos] {
                Some(e) => {
                    if e.key.eq(&entry.key) {
                        self.elems[pos] = Some(entry);
                        return true;
                    }
                }
                None => {
                    self.elems[pos] = Some(entry);
                    self.len += 1;
                    return false;
                }
            }

            let e = self.elems[pos].as_mut().unwrap();
            // println!("collision: {:?} <=> {:?}", &e.key, &entry.key);

            // If the existing elem has probed less than us, then swap places with
            // existing elem, and keep going to find another slot for that elem.
            let elem_dist = distance(e.hash, pos, self.capacity);
            if elem_dist < dist {
                // Update current distance.
                dist = elem_dist;

                entry.dist = dist;
                mem::swap(e, &mut entry);
            }

            pos = (pos + 1) & self.mask;
            dist += 1;
        }
    }

    /// index returns the position of key in the hash map.
    fn index(&self, key: &K) -> Option<usize> {
        let hash = hash_key(key);
        let mut pos = (hash & self.mask as u64) as usize;

        let mut dist = 0_u64;
        loop {
            let e = self.elems[pos].as_ref()?;
            if dist > distance(e.hash, pos, self.capacity) {
                return None;
            } else if e.hash == hash && e.key.eq(key) {
                return Some(pos);
            }

            pos = (pos + 1) & self.mask;
            dist += 1;
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    // fn distance_histogram(&self) -> Vec<usize>{
    //
    // }
}

impl<K, V> HashMap<K, V>
where
    K: Eq + Hash + AsByte + Debug,
    V: Debug,
{
    pub(crate) fn print(&self) {
        self.elems.iter().enumerate().for_each(|(i, e)| {
            if let Some(e) = e {
                println!("{} -> {:?}", i, e);
            }
        })
    }
}

pub trait AsByte {
    fn as_byte(&self) -> &[u8];
}

impl AsByte for String {
    fn as_byte(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsByte for str {
    fn as_byte(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsByte for Vec<u8> {
    fn as_byte(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsByte for &'a [u8] {
    fn as_byte(&self) -> &[u8] {
        self
    }
}

/// hash_key computes a hash of key. Hash is always non-zero.
pub fn hash_key<K>(key: &K) -> u64
where
    K: Eq + Hash + AsByte + ?Sized,
{
    let mut xx_hash = twox_hash::XxHash64::with_seed(0);
    xx_hash.write(key.as_byte());
    let mut h = xx_hash.finish();

    // println!("hash 1: {}, {:?}", h, h.to_be_bytes());

    if h == 0 {
        h = 1;
    } else {
        let h_ptr = &h as *const u64 as *const i64;
        unsafe {
            if *h_ptr < 0 {
                // influxdb, what the fuck!!! why change the hash value?
                h = (0 - *h_ptr) as u64;
            }
        }
    }

    h

    // let n = h.to_be_bytes();
    // let mut h = i64::from_be_bytes(n);
    //
    // // let mut xx_hash = twox_hash::XxHash64::with_seed(0);
    // // key.hash(&mut xx_hash);
    // // let mut h = xx_hash.finish();
    // // println!("hash 2: {}", h);
    //
    // if h == 0 {
    //     h = 1;
    // } else if h < 0 {
    //     // influxdb, what the fuck!!! why change the hash value?
    //     h = 0 - h;
    // }
    //
    // h as u64
}

/// hash_u64 computes a hash of an int64. Hash is always non-zero.
pub fn hash_u64(key: u64) -> u64 {
    let buf = key.to_be_bytes();
    hash_key(&buf.as_ref())
}

/// distance returns the probe distance for a hash in a slot index.
/// NOTE: Capacity must be a power of 2.
pub fn distance(hash: u64, i: usize, capacity: u64) -> u64 {
    let mask = capacity - 1;
    let dist = ((i as u64) + capacity - (hash & mask)) & mask;
    dist
}

/// pow2 returns the number that is the next highest power of 2.
/// Returns v if it is a power of 2.
fn pow2(v: u64) -> u64 {
    let mut i = 2_u64;
    loop {
        if i < (1_u64 << 62) {
            if i >= v {
                return i;
            }

            i *= 2;
        } else {
            break;
        }
    }

    panic!("unreachable")
}

#[cfg(test)]
mod tests {
    use crate::{hash_key, HashMap};

    #[test]
    fn test_hash() {
        let n = hash_key("xyz");
        println!("{}", n);
    }

    #[test]
    fn test_hash_map() {
        let mut m = HashMap::new();

        // m.insert("16".as_bytes().to_vec(), "16".to_string());
        // m.insert("45".as_bytes().to_vec(), "45".to_string());
        // m.insert("56".as_bytes().to_vec(), "56".to_string());
        // m.insert("79".as_bytes().to_vec(), "79".to_string());
        // m.insert("83".as_bytes().to_vec(), "83".to_string());

        let size = 512;
        for i in 0..size {
            let key = i.to_string();
            m.insert(key.clone(), key);
        }
        m.print();

        for i in 0..size {
            let key = i.to_string();
            let val = m.get(&key).unwrap();
            assert_eq!(val.as_str(), key.as_str());
            println!("{}:{}", key, val);
        }
    }
}
