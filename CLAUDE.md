# CLAUDE.md

Rust로 구현하는 학습용 OLTP 데이터베이스. PostgreSQL 와이어 프로토콜 호환, PG 방식 MVCC/WAL,
이후 공유 스토리지 기반 HA까지 확장한다.

## 설계 문서
- 모든 설계 결정은 `DESIGN.md`에 있다. 이 파일이 유일한 기준이다.
- 작업을 시작할 때 `DESIGN.md`에서 해당 단계(tasks)와 관련 decisions를 먼저 읽는다. 문서 전체를 매번 다시 요약하지 않는다.
- `DESIGN.md`의 decisions, ha_and_backup_design, open_questions는 사용자가 명시적으로 지시할 때만 수정한다.

## 현재 단계
- 1단계 10번: 제약조건 (NOT NULL, CHECK, PK, UNIQUE)
  (단계가 끝나면 사용자가 이 줄을 갱신한다)

## 진행 상황과 환경
- 진행 상황과 남은 한계는 `docs/PROGRESS.md`, 환경 구성과 재개 방법은 `docs/SETUP.md`, 구현 중 미결 사항은 `docs/notes.md`.
- 단계를 마치면 `docs/PROGRESS.md`를 갱신한다.

## 작업 방식
- 새 모듈은 코드를 쓰기 전에 설계(자료구조, 레이아웃, 트레이드오프, PG/Oracle과의 비교)를 짧게 제시하고 동의를 받는다.
- 한 번에 하나의 작은 단위만 구현한다. 컴파일되고 테스트가 통과하는 상태로 끝낸다.
- 구현 중 decisions와 충돌하거나 decisions에 없는 설계 판단이 필요해지면, 임의로 우회하거나 정하지 말고 멈춘다.
  "결정 필요" 제목으로 문제, 선택지, 각 트레이드오프를 짧게 요약해 보고한다. 사용자는 이를 채팅 리뷰로 가져간다.
- open_questions에 있는 항목이 필요한 지점에 도달하면 먼저 확인을 요청한다.
- 답변은 한국어, 코드 식별자와 주석은 영어.

## Rust 규칙
- unsafe는 꼭 필요한 곳에만, 안전한 API로 감싸고, 모든 unsafe 블록에 `// SAFETY:` 주석.
- 에러는 thiserror. 라이브러리 코드에서 unwrap/expect 금지 (테스트 코드는 허용).
- `panic = "abort"` (release, dev 프로필 모두).
- 비동기 런타임(tokio 등) 사용 금지. 모든 I/O는 동기.

## 명령어
- 빌드: `cargo build`
- 테스트: `cargo test`
- 린트: `cargo clippy --all-targets -- -D warnings`
- 포맷: `cargo fmt --check`
- 작업 완료 조건: 위 네 명령이 모두 통과.

## 커밋
- 작업 단위마다 커밋한다. 메시지는 영어, 첫 줄에 모듈명 접두사 (예: `storage: add slotted page layout`).
- 커밋 전에 사용자의 확인을 받는다.
