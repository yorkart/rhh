use std::borrow::Borrow;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::mem;

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
        Self::with_capacity_and_factor(capacity, 90)
    }

    pub fn with_capacity_and_factor(capacity: usize, load_factor: usize) -> Self {
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

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: ?Sized + Eq + Hash + AsByte,
        K: Borrow<Q>,
    {
        self.index(key)
            .map(|i| &self.elems[i].as_ref().unwrap().value)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        Q: ?Sized + Eq + Hash + AsByte,
        K: Borrow<Q>,
    {
        self.index(key)
            .map(|i| &mut self.elems[i].as_mut().unwrap().value)
    }

    pub fn keys(&self) -> Keys<'_, K, V> {
        Keys::new(self.iter())
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter::new(self)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&'_ K, &'_ mut V)> {
        self.elems
            .iter_mut()
            .filter_map(|e| e.as_mut().map(|e| (&e.key, &mut e.value)))
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
            Self::with_capacity_and_factor((self.capacity * 2) as usize, self.load_factor);
        for e in &mut self.elems {
            let e = e.take();
            if let Some(HashElem {
                hash, key, value, ..
            }) = e
            {
                new_map.insert_raw(hash, key, value);
            }
        }

        mem::swap(&mut new_map, self);
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
    fn index<Q>(&self, key: &Q) -> Option<usize>
    where
        Q: ?Sized + Eq + Hash + AsByte,
        K: Borrow<Q>,
    {
        let hash = hash_key(key);
        let mut pos = (hash & self.mask as u64) as usize;

        let mut dist = 0_u64;
        loop {
            let e = self.elems[pos].as_ref()?;
            if dist > distance(e.hash, pos, self.capacity) {
                return None;
            } else if e.hash == hash && key.borrow().eq(e.key.borrow()) {
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
}

impl<K, V> Debug for HashMap<K, V>
where
    K: Eq + Hash + AsByte + Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

// impl<K, V> HashMap<K, V>
// where
//     K: Eq + Hash + AsByte + Debug,
//     V: Debug,
// {
//     pub(crate) fn print(&self) {
//         self.elems.iter().enumerate().for_each(|(i, e)| {
//             if let Some(e) = e {
//                 println!("{} -> {:?}", i, e);
//             }
//         })
//     }
// }

pub struct Iter<'a, K: 'a, V: 'a>
where
    K: Eq + Hash + AsByte,
{
    map: &'a HashMap<K, V>,
    at: usize,
    num_found: usize,
}

impl<'a, K: 'a, V: 'a> Iter<'a, K, V>
where
    K: Eq + Hash + AsByte,
{
    pub fn new(map: &'a HashMap<K, V>) -> Self {
        Self {
            map,
            at: 0,
            num_found: 0,
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Eq + Hash + AsByte,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.at >= self.map.elems.len() {
                return None;
            }

            let e = &self.map.elems[self.at];
            self.at += 1;

            if let Some(e) = e {
                self.num_found += 1;
                return Some((&e.key, &e.value));
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = self.map.len() as usize - self.num_found;
        (hint, Some(hint))
    }
}

pub struct Keys<'a, K: 'a, V: 'a>
where
    K: Eq + Hash + AsByte,
{
    inner: Iter<'a, K, V>,
}

impl<'a, K: 'a, V: 'a> Keys<'a, K, V>
where
    K: Eq + Hash + AsByte,
{
    pub fn new(inner: Iter<'a, K, V>) -> Self {
        Self { inner }
    }
}

impl<'a, K, V> Iterator for Keys<'a, K, V>
where
    K: Eq + Hash + AsByte,
{
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((k, _)) => Some(k),
            None => None,
        }
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
        assert_eq!(91681375387435871, n);
    }

    #[test]
    fn test_hash_map() {
        let mut m = HashMap::new();

        let size = 512;
        for i in 0..size {
            let key = i.to_string();
            m.insert(key.clone(), key);
        }
        // m.print();

        for i in 0..size {
            let key = i.to_string();
            let val = m.get(&key).unwrap();
            assert_eq!(val.as_str(), key.as_str());
        }

        for (k, v) in m.iter_mut() {
            println!("{} => {}", k, v);
        }
        for (k, v) in m.iter() {
            println!("{} => {}", k, v);
        }
    }
}
