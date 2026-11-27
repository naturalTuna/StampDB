# 진행 상황

마지막 갱신: 2026-11-27 (1단계 6번 진행 중, 4/7 단위 완료)

## 진행률

| 기준 | 진행률 |
|---|---|
| 전체 로드맵 22개 항목 중 완료 | 5개 (약 23%) |
| 1단계(단일 노드, psql 접속까지) 13개 중 완료 | 5개 (약 38%) |
| 작업량 가중 추정 (전체) | 약 18~20% |

남은 큰 덩어리: 트랜잭션/MVCC/lock manager(6), vacuum(7), 카탈로그(8), SQL 계층(9), 제약조건(10, 11), 통계(12), 서버(13), 그리고 2·3단계 HA.

## 완료한 단계

| 단계 | 내용 | 주요 커밋 |
|---|---|---|
| 1-1 | 디스크 매니저 + 페이지 포맷: aligned buffer, 페이지 헤더, slotted page, CRC32C 체크섬, 파일 접근 trait(fencing 토큰), O_DIRECT 로컬 백엔드, DiskManager | `9b86420`..`ca7a42f` |
| 1-2 | 버퍼 풀: 파티션 매핑, PinGuard/PageRef/PageMut, clock-sweep(PG give-up 규칙), 복사 후 체크섬 flush, FlushControl, extend/flush/drop | `e561e5a`..`ad98a09` |
| 1-3 | Heap + 튜플: 데이터 타입과 varlena, 35B 튜플 헤더(64비트 xid, cmin/cmax 분리), null bitmap, heap insert/fetch/delete/update/scan, 기회적 hint bit | `b65d811`..`48e6358` |
| 1-4 | B+Tree: memcomparable 키 + TID 접미사, Lehman-Yao(right-link, high key), split/루트 split, 90:10, 범위 스캔, 삭제, 동시성 테스트 | `6fb5b00`..`9a805ec` |
| 1-5 | WAL + 복구: 레코드 형식, 세그먼트/control 파일, FPW, hint FPI, WAL-before-data, heap/btree physiological 로깅과 redo, INCOMPLETE_SPLIT 마무리, 복구, 체크포인트, pending sync, 체크포인트 스레드, bgwriter, 세그먼트 삭제, kill -9 테스트 | `1f9ca23`..`a9432b5` |

DESIGN.md 변경 이력: r3(블록 크기 sysfs, CRC32C, sync 토큰) / r4(hint bit 설정 방식, cmin/cmax, 패딩 없음, NUMERIC·타입 OID) / r5(B+Tree 키·TID·NULL·split) / r6(WAL 방식, abort 정책, control 파일, 세그먼트, kill -9 테스트).

## 모듈 지도

```
src/storage   페이지, slotted page, 체크섬, 파일 접근 trait, O_DIRECT 백엔드, DiskManager
src/buffer    버퍼 풀, PinGuard, FlushControl
src/types     데이터 타입과 직렬화 (varlena)
src/access    tuple(튜플 형식), heap, btree(key/page/tree), redo 분기
src/txn       Xid, CommandId (6단계에서 확장)
src/wal       record, log(WAL 쓰기/읽기), control, recovery(Engine, initdb, checkpoint), background
tests/        O_DIRECT 통합 테스트, WAL, 버퍼 풀 스트레스, kill -9
docs/         SETUP.md(환경), PROGRESS.md(이 문서), notes.md(구현 중 미결 사항)
```

테스트: 단위 149개 + 통합 23개 (모두 통과).

## 진행 중: 1단계 6번 (트랜잭션 + MVCC + lock manager)

결정은 DESIGN.md r7로 확정됨. 구현 단위와 상태:

1. [완료] txn 기반: TxnId, ProcArray, Snapshot, TransactionManager trait
2. [완료] CLOG(RelFileId 1, 버퍼 풀) + COMMIT/ABORT/ZERO_PAGE WAL + LocalTxnManager + next_xid 복구
3. [완료] 가시성 판단 `access::visibility::satisfies_mvcc` (스냅샷 우선, hint 제안)
4. [완료] lock manager `lock`: PG 8모드, FIFO, transaction lock, wait_for_xid, lock_timeout
5. [다음] deadlock 검출: deadlock_timeout 경과 시 wait-for 그래프 사이클 검사, 요청 쪽 에러 (LockError::Deadlock 자리 있음)
6. MVCC heap DML: 레코드에 쓰는 쪽 xid 기록(rec.xid, 복구 시 next_xid 계산에 필요), xmin/xmax, TM_Result류 충돌 결과, xid 대기, RC 최신 버전 추적 헬퍼, SI 직렬화 오류, 가시 튜플만 내보내는 스캔과 hint 일괄 설정
7. 격리 수준 시나리오 테스트 + 크래시 후 미커밋 행 비가시 확인, 이후 PROGRESS 갱신

## 이전 단계에서 넘어온 한계와 미결 사항

- 트랜잭션이 없어 크래시 직전 작업이 heap에 남을 수 있다 (6단계에서 커밋되지 않은 행으로 처리).
- "커밋됨" hint는 커밋 레코드 flush 후에만 설정하는 규칙: 6단계.
- heap/btree의 동시 delete/update 보호(행 잠금, 가시성): 6단계.
- FSM 없음, 빈 B+Tree 페이지 삭제 없음: 7단계(vacuum).
- TOAST 없음 (튜플은 한 페이지 이내).
- smgr DROP 레코드 없음, timeline 1 고정(16단계).
- WAL: insert lock 하나, flush 시 버퍼 복사, 메모리 버퍼 상한 없음.
- 버퍼 풀 크기는 동시 pin 수보다 충분히 커야 한다 (13단계에서 기동 시 검사; B+Tree insert는 레벨당 약 2개 pin).
- ha 모드에서 모든 victim의 flush가 보류될 때의 동작: 미결 (`docs/notes.md`, 20단계 전 결정).
- 관측성(tracing) 미결: 지금은 stderr 출력.
