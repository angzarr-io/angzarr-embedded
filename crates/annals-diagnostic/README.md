# annals-diagnostic

Localizable errors. Zero dependencies and nothing specific to any game.

Every Annals error implements `Diagnostic`:

```rust
pub trait Diagnostic: fmt::Debug + fmt::Display {
    fn code(&self) -> &'static str;               // the Fluent message id
    fn args(&self) -> Vec<(&'static str, Arg)>;   // the message's named args
}

pub enum Arg { Int(i64), Uint(u64), Str(String), Nested(Box<dyn Diagnostic>) }
```

- **The code** is stable, kebab-case and starts with the crate's name:
  `annals-yaml-too-new`. A code is never reused for another meaning.
- **The args** are data: numbers, ids, paths, the text that was read.
  Never English. A wrapped cause is `Arg::Nested`.
- **`Display`** is the English fallback, for logs and for a code no catalog
  has.

Each crate ships an en-US Fluent catalog,
`crates/<crate>/locales/en-US/<crate>.ftl`, with a message for every code.
Annals renders nothing: the game client looks the code up in its catalogs
and passes the args by name, rendering a nested cause first.

A game's own errors implement `Diagnostic` too, with codes starting with the
game's name. Where an Annals error holds a game's error (a migration step's,
a content decoder's) it keeps it as a `Report`: the code, args and English
captured, so the holder stays `Clone` and `Eq`. `Report::new(code,
english).arg(name, value)` raises one without an error type of its own.
