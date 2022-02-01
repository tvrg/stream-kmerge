# Stream KMerge

[K-way merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm) for rust streams.

[![build_status](https://github.com/tvrg/stream-kmerge/actions/workflows/ci.yml/badge.svg)](https://github.com/tvrg/stream-kmerge/actions)

How to use with Cargo:

```toml
[dependencies]
stream-kmerge = "0.1"
```

How to use in your crate:

```rust
use stream_kmerge::kmerge_by;
```

## License

Dual-licensed to be compatible with the Rust project.

Licensed under the Apache License, Version 2.0
https://www.apache.org/licenses/LICENSE-2.0 or the MIT license
https://opensource.org/licenses/MIT, at your
option. This file may not be copied, modified, or distributed
except according to those terms.

Portions of the project have been copied from
https://github.com/rust-itertools/itertools which is copyrighted under the
Apache License, Version 2.0 https://www.apache.org/licenses/LICENSE-2.0 or the
MIT license https://opensource.org/licenses/MIT, at your option. See
https://github.com/rust-itertools/itertools/blob/6c4fc2f8e745fe4780578dfa4feb44ccccffb521/src/kmerge_impl.rs
for the original code.
