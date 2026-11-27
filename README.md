# mydb

Rust로 구현하는 학습용 OLTP 데이터베이스. PostgreSQL 와이어 프로토콜 호환을 목표로 하고, PG 방식 MVCC/WAL을 거쳐 공유 스토리지 기반 HA까지 확장한다.

- 설계 결정: [`DESIGN.md`](DESIGN.md) (유일한 기준)
- 작업 규칙 (Claude Code): [`CLAUDE.md`](CLAUDE.md)
- 진행 상황: [`docs/PROGRESS.md`](docs/PROGRESS.md)
- 환경 구성과 재개 방법: [`docs/SETUP.md`](docs/SETUP.md)
- 구현 중 미결 사항: [`docs/notes.md`](docs/notes.md)

```sh
cargo build && cargo test
```
