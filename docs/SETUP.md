# 작업 환경과 재개 방법

새 VM에서 이 저장소를 받아 작업을 이어가기 위한 문서다.

## 기준 환경 (지금까지 개발한 VM)

| 항목 | 값 |
|---|---|
| OS | Red Hat Enterprise Linux 9.8 (EL9 계열, DESIGN의 대상 환경) |
| 커널 | 5.14.0 (x86_64) |
| 파일시스템 | XFS (작업 디렉터리와 `target/`이 XFS 위에 있음) |
| Rust | rustc / cargo 1.100.0 (rustup, stable), edition 2024 |
| 리소스 | 4 vCPU, 메모리 약 3.5GB |

## 새 VM 준비

1. EL9 계열(Rocky/Alma/RHEL 9) x86_64를 권장한다. 다른 리눅스도 되지만 아래 조건은 필요하다.
   - 작업 디렉터리와 `target/`이 **O_DIRECT를 지원하는 파일시스템**(XFS, ext4)이어야 한다. tmpfs는 안 된다.
   - 데이터 디렉터리가 있는 디바이스의 논리 블록 크기를 sysfs(`/sys/dev/block/<maj>:<min>/queue/logical_block_size`)에서 읽을 수 있어야 하고, 4096 이하여야 한다. 컨테이너나 NFS에서는 기동이 거부된다 (DESIGN r3).
2. 빌드 도구: `sudo dnf install -y git gcc` (링커용 C 컴파일러).
3. Rust: https://rustup.rs 의 설치 스크립트로 rustup을 설치한 뒤 `rustup toolchain install stable` 실행.
   `Cargo.toml`의 `rust-version`보다 낮은 툴체인이면 빌드가 거부된다.
   rustfmt와 clippy가 필요하다: `rustup component add rustfmt clippy`.
4. 저장소 받기: `git clone <저장소 URL> ~/work/mydb && cd ~/work/mydb`

## 확인 (작업 완료 조건과 같은 4개 명령)

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

- `cargo test`에는 실제 O_DIRECT 파일 I/O 테스트와 kill -9 테스트(`tests/crash.rs`, 자기 자신을 작업 모드로 다시 실행해 SIGKILL)가 들어 있다. 이 테스트들은 `target/tmp` 아래에 임시 디렉터리를 만든다.
- 처음 실행할 때 crates.io에서 의존성을 내려받는다 (인터넷 필요).

## Claude Code로 이어서 작업하기

1. 저장소 루트에서 Claude Code를 실행한다. `CLAUDE.md`가 자동으로 읽힌다.
2. 진행 상황은 `docs/PROGRESS.md`, 모든 설계 결정은 `DESIGN.md`, 구현 중 남긴 미결 사항은 `docs/notes.md`에 있다.
3. 첫 요청 예: "docs/PROGRESS.md와 CLAUDE.md의 현재 단계를 읽고, 그 단계의 설계부터 이어서 진행해줘."
4. 이전 VM에서의 대화 기록은 넘어오지 않는다. 필요한 맥락은 위 세 문서와 커밋 메시지에 남겨 두었다.
