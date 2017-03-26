use ::Unsigned;

macro_rules! per_cache_line {
    ($t:ty) => {{ ::std::cmp::max(64 / ::std::mem::size_of::<$t>(), 4) }}
}

macro_rules! lines_per_page {
    () => {{ 2 * 4096 / 64 }}
}

/// A few buffers capable of radix sorting by least significant byte.
///
/// The sorter allows the use of multiple different key bytes, determined by a type `U: Unsigned`.
/// Currently, one is allowed to mix and match these as records are pushed, which may be a design
/// bug.
pub struct RadixSorter<T> {
    shuffler: RadixShuffler<T>,
}

impl<T> RadixSorter<T> {
    /// Constructs a new radix sorter.
    pub fn new() -> RadixSorter<T> {
        RadixSorter {
            shuffler: RadixShuffler::new(),
        }
    }
    /// Pushes a sequence of elements into the sorter.
    #[inline]
    pub fn extend<U: Unsigned, F: Fn(&T)->U, I: Iterator<Item=T>>(&mut self, iterator: I, function: &F) {
        for element in iterator {
            self.push(element, function);
        }
    }
    /// Pushes a single element into the sorter.
    #[inline]
    pub fn push<U: Unsigned, F: Fn(&T)->U>(&mut self, element: T, function: &F) {
        self.shuffler.push(element, &|x| (function(x).as_u64() % 256) as u8);
    }
    /// Pushes a batch of elements into the sorter, and transfers ownership of the containing allocation.
    #[inline]
    pub fn push_batch<U: Unsigned, F: Fn(&T)->U>(&mut self, batch: Vec<T>, function: &F) {
        self.shuffler.push_batch(batch,  &|x| (function(x).as_u64() % 256) as u8);
    }
    /// Sorts a sequence of batches, re-using the allocations where possible and re-populating `batches`.
    pub fn sort<U: Unsigned, F: Fn(&T)->U>(&mut self, batches: &mut Vec<Vec<T>>, function: &F) {
        for batch in batches.drain(..) { 
            self.push_batch(batch, function); 
        }
        self.finish_into(batches, function);
    }
    /// Finishes a sorting session by allocating and populating a sequence of batches.
    pub fn finish<U: Unsigned, F: Fn(&T)->U>(&mut self, function: &F) -> Vec<Vec<T>> {
        let mut result = Vec::new();
        self.finish_into(&mut result, function);
        result
    }
    /// Finishes a sorting session by populating a supplied sequence.
    pub fn finish_into<U: Unsigned, F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, function: &F) {
        self.shuffler.finish_into(target);
        for byte in 1..(<U as Unsigned>::bytes()) { 
            self.reshuffle(target, &|x| ((function(x).as_u64() >> (8 * byte)) % 256) as u8);
        }
    }
    /// Consumes supplied buffers for future re-use by the sorter.
    ///
    /// This method is equivalent to `self.rebalance(buffers, usize::max_value())`.
    pub fn recycle(&mut self, buffers: &mut Vec<Vec<T>>) {
        self.rebalance(buffers, usize::max_value());
    }
    /// Either consumes from or pushes into `buffers` to leave `intended` spare buffers with the sorter.
    pub fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        self.shuffler.rebalance(buffers, intended);
    }
    #[inline(always)]
    fn reshuffle<F: Fn(&T)->u8>(&mut self, buffers: &mut Vec<Vec<T>>, function: &F) {
        for buffer in buffers.drain(..) {
            self.shuffler.push_batch(buffer, function);
        }
        self.shuffler.finish_into(buffers);
    }
}

struct RadixShuffler<T> {
    fronts: Vec<Vec<T>>,
    buffers: Vec<Vec<Vec<T>>>, // for each byte, a list of segments
    stashed: Vec<Vec<T>>,      // spare segments
    default_capacity: usize,
}

impl<T> RadixShuffler<T> {

    /// Creates a new `RadixShuffler` with a default capacity of `1024`.
    fn new() -> RadixShuffler<T> {
        RadixShuffler::with_capacities(lines_per_page!() * per_cache_line!(T))
    }

    /// Creates a new `RadixShuffler` with a specified default capacity.
    fn with_capacities(size: usize) -> RadixShuffler<T> {
        let mut buffers = vec![]; for _ in 0..256 { buffers.push(Vec::new()); }
        let mut fronts = vec![]; for _ in 0..256 { fronts.push(Vec::new()); }

        RadixShuffler {
            buffers: buffers,
            stashed: vec![],
            fronts: fronts,
            default_capacity: size,
        }
    }

    /// Pushes a batch of elements into the `RadixShuffler` and stashes the memory backing the batch.
    #[inline]
    fn push_batch<F: Fn(&T)->u8>(&mut self, mut elements: Vec<T>, function: &F) {
        for element in elements.drain(..) {
            self.push(element, function);
        }
        // TODO : determine some discipline for when to keep buffers vs not.
        // if elements.capacity() == self.default_capacity {
            self.stashed.push(elements);
        // }
    }

    /// Pushes an element into the `RadixShuffler`, into a one of `256` arrays based on its least
    /// significant byte.
    #[inline]
    fn push<F: Fn(&T)->u8>(&mut self, element: T, function: &F) {

        let byte = function(&element) as usize;

        // write the element to our scratch buffer space and consider it taken care of.
        // test the buffer capacity first, so that we can leave them uninitialized.
        unsafe {
            if self.fronts.get_unchecked(byte).len() == self.fronts.get_unchecked(byte).capacity() {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::with_capacity(self.default_capacity));
                let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                if complete.len() > 0 {
                    self.buffers[byte].push(complete);
                }
            }

            let len = self.fronts.get_unchecked(byte).len();
            ::std::ptr::write((*self.fronts.get_unchecked_mut(byte)).get_unchecked_mut(len), element);
            self.fronts.get_unchecked_mut(byte).set_len(len + 1);
        }
    }

    /// Finishes the shuffling into a target vector.
    fn finish_into(&mut self, target: &mut Vec<Vec<T>>) {
        for byte in 0..256 {
            if self.fronts[byte].len() > 0 {
                let replacement = self.stashed.pop().unwrap_or_else(|| Vec::new());
                let complete = ::std::mem::replace(&mut self.fronts[byte], replacement);
                if complete.len() > 0 {
                    self.buffers[byte].push(complete);
                }
            }
        }

        for byte in 0..256 {
            target.extend(self.buffers[byte].drain(..));
        }
    }

    fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize) {
        while self.stashed.len() > intended {
            buffers.push(self.stashed.pop().unwrap());
        }
        while self.stashed.len() < intended && buffers.len() > 0 {
            let mut buffer = buffers.pop().unwrap();
            buffer.clear();
            self.stashed.push(buffer);
        }
    }
}

mod test {

    #[test]
    fn test1() {

        let size = 1_000_000;

        let mut vector = Vec::<usize>::with_capacity(size);
        for index in 0..size {
            vector.push(index);
        }
        for index in 0..size {
            vector.push(size - index);
        }

        let mut sorter = super::RadixSorter::new();

        for &element in &vector {
            sorter.push(element, &|&x| x);
        }

        vector.sort();

        let mut result = Vec::new();
        for element in sorter.finish(&|&x| x).into_iter().flat_map(|x| x.into_iter()) {
            result.push(element);
        }

        assert_eq!(result, vector);

    }

    #[test]
    fn test_large() {

        let size = 1_000_000;

        let mut vector = Vec::<[usize; 16]>::with_capacity(size);
        for index in 0..size {
            vector.push([index; 16]);
        }
        for index in 0..size {
            vector.push([size - index; 16]);
        }

        let mut sorter = super::RadixSorter::new();

        for &element in &vector {
            sorter.push(element, &|&x| x[0]);
        }

        vector.sort_by(|x, y| x[0].cmp(&y[0]));

        let mut result = Vec::new();
        for element in sorter.finish(&|&x| x[0]).into_iter().flat_map(|x| x.into_iter()) {
            result.push(element);
        }

        assert_eq!(result, vector);
    }
}
