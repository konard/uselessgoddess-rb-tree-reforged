use std::{fmt, hash::Hash, marker::PhantomData, sync::atomic::{AtomicU64, Ordering}};

pub struct Id<T> {
    inner: u64,
    _tag: PhantomData<T>,
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Id<T> {}
impl<T> Eq for Id<T> {}
impl<T> PartialEq for Id<T> {
    fn eq(&self, o: &Self) -> bool {
        self.inner == o.inner
    }
}
impl<T> Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, s: &mut H) {
        self.inner.hash(s)
    }
}
impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Id({})", self.inner)
    }
}

pub struct IdGen<T>(AtomicU64, PhantomData<T>);

impl<T> Default for IdGen<T> {
    fn default() -> Self {
        Self(AtomicU64::new(0), PhantomData)
    }
}

impl<T> IdGen<T> {
    pub fn next(&self) -> Id<T> {
        Id { inner: self.0.fetch_add(1, Ordering::Relaxed), _tag: PhantomData }
    }
}

impl<T> Id<T> {
    pub fn inner(self) -> u64 {
        self.inner
    }

    pub fn from_raw(inner: u64) -> Self {
        Id { inner, _tag: PhantomData }
    }
}

pub struct WorkerTag;
pub struct SessionTag;
pub struct PartyTag;
pub struct GroupTag;

pub type WorkerId = Id<WorkerTag>;
pub type SessionId = Id<SessionTag>;
pub type PartyId = Id<PartyTag>;
pub type GroupId = Id<GroupTag>;
