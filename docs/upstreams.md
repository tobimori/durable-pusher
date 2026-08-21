# Upstream snapshots

The ignored `.repos` directory contains shallow clones. These clones are implementation references.

| Reference  | Commit                                     | Package version  |
| ---------- | ------------------------------------------ | ---------------- |
| Effect v4  | `436f10d1efccec308426532ff3f88df9a96434f3` | `4.0.0-rc.111`   |
| Alchemy v2 | `252b2e1687a372b33404c1653ed6941654ce80ac` | `2.0.0-beta.72`  |
| anti-slop  | `6d538555cb151d4121ed51a27db81890eacf8ae9` | source reference |

The application uses the applicable published package versions. It does not import code from
`.repos`.
