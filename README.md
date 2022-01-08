# Stream KMerge

[K-way merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm) for rust streams.

[![build_status](https://github.com/tvrg/stream-kmerge/actions/workflows/ci.yml/badge.svg)](https://github.com/tvrg/stream-kmerge/actions)

How to use with Cargo:

```toml
[dependencies]
stream-kmerge = { git = "https://github.com/tvrg/stream-kmerge.git" }
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
