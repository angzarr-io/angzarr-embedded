//! Tags: how a query names a *kind* of node. Open — content defines them —
//! interned to small ids, each optionally carrying a small value.

/// An interned tag.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TagId(pub u16);

/// Every tag name a game uses, interned in the order first seen. Ids are
/// stable for as long as the registry is built the same way.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct TagRegistry {
    names: Vec<String>,
}

impl TagRegistry {
    pub fn new() -> TagRegistry {
        TagRegistry::default()
    }

    /// The id for `name`, adding it if it is new.
    pub fn intern(&mut self, name: &str) -> TagId {
        if let Some(id) = self.get(name) {
            return id;
        }
        let id = TagId(u16::try_from(self.names.len()).expect("too many tags"));
        self.names.push(name.to_string());
        id
    }

    pub fn get(&self, name: &str) -> Option<TagId> {
        self.names
            .iter()
            .position(|n| n == name)
            .map(|i| TagId(i as u16))
    }

    pub fn name(&self, id: TagId) -> &str {
        &self.names[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

/// A node's tags, sorted by id, each with a value — a grade, a count; `0`
/// when a tag is simply present.
#[derive(Clone, Default, PartialEq, Eq, Hash, Debug)]
pub struct TagSet {
    tags: Vec<(TagId, i32)>,
}

impl TagSet {
    pub fn new() -> TagSet {
        TagSet::default()
    }

    /// Add a tag, or change its value.
    pub fn set(&mut self, tag: TagId, value: i32) {
        match self.tags.binary_search_by_key(&tag, |(t, _)| *t) {
            Ok(i) => self.tags[i].1 = value,
            Err(i) => self.tags.insert(i, (tag, value)),
        }
    }

    pub fn with(mut self, tag: TagId, value: i32) -> TagSet {
        self.set(tag, value);
        self
    }

    /// Remove a tag. Returns whether it was there.
    pub fn remove(&mut self, tag: TagId) -> bool {
        match self.tags.binary_search_by_key(&tag, |(t, _)| *t) {
            Ok(i) => {
                self.tags.remove(i);
                true
            }
            Err(_) => false,
        }
    }

    pub fn has(&self, tag: TagId) -> bool {
        self.value(tag).is_some()
    }

    pub fn value(&self, tag: TagId) -> Option<i32> {
        self.tags
            .binary_search_by_key(&tag, |(t, _)| *t)
            .ok()
            .map(|i| self.tags[i].1)
    }

    /// Every tag and its value, in id order.
    pub fn iter(&self) -> impl Iterator<Item = (TagId, i32)> + '_ {
        self.tags.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }
}

/// Anything that carries tags — what the tag queries read.
pub trait Tagged {
    fn tags(&self) -> &TagSet;
}

impl Tagged for TagSet {
    fn tags(&self) -> &TagSet {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_is_stable_and_idempotent() {
        let mut r = TagRegistry::new();
        let roof = r.intern("Rooftop");
        let door = r.intern("Doorway");
        assert_eq!(r.intern("Rooftop"), roof);
        assert_eq!(r.name(door), "Doorway");
        assert_eq!(r.get("Sewer"), None);
    }

    #[test]
    fn a_set_stays_sorted_whatever_the_order_tags_arrive_in() {
        let a = TagSet::new().with(TagId(5), 0).with(TagId(1), 2);
        let b = TagSet::new().with(TagId(1), 2).with(TagId(5), 0);
        assert_eq!(a, b);
        assert_eq!(a.iter().map(|(t, _)| t).collect::<Vec<_>>(), vec![TagId(1), TagId(5)]);
    }

    #[test]
    fn tags_change() {
        let mut s = TagSet::new().with(TagId(3), 1);
        s.set(TagId(3), 4); // the smoke thickens
        assert_eq!(s.value(TagId(3)), Some(4));
        assert!(s.remove(TagId(3))); // and clears
        assert!(!s.has(TagId(3)));
        assert!(!s.remove(TagId(3)));
    }
}
