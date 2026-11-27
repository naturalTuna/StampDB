# 진행 상황

마지막 갱신: 2026-11-27 (1단계 6번 완료, 7번 설계 전)

## 진행률

| 기준 | 진행률 |
|---|---|
| 전체 로드맵 22개 항목 중 완료 | 6개 (약 27%) |
| 1단계(단일 노드, psql 접속까지) 13개 중 완료 | 6개 (약 46%) |
| 작업량 가중 추정 (전체) | 약 23~25% |

남은 큰 덩어리: vacuum(7), 카탈로그(8), SQL 계층(9), 제약조건(10, 11), 통계(12), 서버(13), 그리고 2·3단계 HA.

## 완료한 단계

| 단계 | 내용 | 주요 커밋 |
|---|---|---|
| 1-1 | 디스크 매니저 + 페이지 포맷: aligned buffer, 페이지 헤더, slotted page, CRC32C 체크섬, 파일 접근 trait(fencing 토큰), O_DIRECT 로컬 백엔드, DiskManager | `9b86420`..`ca7a42f` |
| 1-2 | 버퍼 풀: 파티션 매핑, PinGuard/PageRef/PageMut, clock-sweep(PG give-up 규칙), 복사 후 체크섬 flush, FlushControl, extend/flush/drop | `e561e5a`..`ad98a09` |
| 1-3 | Heap + 튜플: 데이터 타입과 varlena, 35B 튜플 헤더(64비트 xid, cmin/cmax 분리), null bitmap, heap insert/fetch/delete/update/scan, 기회적 hint bit | `b65d811`..`48e6358` |
| 1-4 | B+Tree: memcomparable 키 + TID 접미사, Lehman-Yao(right-link, high key), split/루트 split, 90:10, 범위 스캔, 삭제, 동시성 테스트 | `6fb5b00`..`9a805ec` |
| 1-6 | 트랜잭션 + MVCC + lock manager: TxnId/ProcArray/스냅샷/trait, CLOG(버퍼 풀 relation)와 커밋·abort WAL, 가시성 판단(스냅샷 우선), PG 8모드 lock manager, deadlock 검출, lock_timeout, Transaction(RC/SI), heap MVCC DML(TM_Result, xid 대기, SI 직렬화 오류, 갱신 체인), 가시 스캔과 hint 일괄 설정, 격리 수준 시나리오 테스트 | `0ecc010`..(이 커밋) |
| 1-5 | WAL + 복구: 레코드 형식, 세그먼트/control 파일, FPW, hint FPI, WAL-before-data, heap/btree physiological 로깅과 redo, INCOMPLETE_SPLIT 마무리, 복구, 체크포인트, pending sync, 체크포인트 스레드, bgwriter, 세그먼트 삭제, kill -9 테스트 | `1f9ca23`..`a9432b5` |

DESIGN.md 변경 이력: r3(블록 크기 sysfs, CRC32C, sync 토큰) / r4(hint bit 설정 방식, cmin/cmax, 패딩 없음, NUMERIC·타입 OID) / r5(B+Tree 키·TID·NULL·split) / r6(WAL 방식, abort 정책, control 파일, 세그먼트, kill -9 테스트) / r7(CLOG, lock 모드, 행 대기, xid 발급 시점, 동기 커밋, SAVEPOINT 제외).

## 모듈 지도

```
src/storage   페이지, slotted page, 체크섬, 파일 접근 trait, O_DIRECT 백엔드, DiskManager
src/buffer    버퍼 풀, PinGuard, FlushControl
src/types     데이터 타입과 직렬화 (varlena)
src/access    tuple(튜플 형식), heap(MVCC DML 포함), btree(key/page/tree), visibility, redo 분기
src/txn       Xid/TxnId, ProcArray, Snapshot, CLOG, LocalTxnManager, Transaction(RC/SI)
src/lock      lock manager (PG 8모드, FIFO, xid 대기, deadlock 검출, lock_timeout)
src/wal       record, log(WAL 쓰기/읽기), control, recovery(Engine, initdb, checkpoint), background
tests/        O_DIRECT 통합 테스트, WAL, 버퍼 풀 스트레스, kill -9, 격리 수준 시나리오
docs/         SETUP.md(환경), PROGRESS.md(이 문서), notes.md(구현 중 미결 사항)
```

테스트: 단위 185개 + 통합 28개 (모두 통과).

## 다음 단계: 1단계 7번

수동 VACUUM (heap + 인덱스 + 빈 B+Tree 페이지 삭제). 설계안을 제시하고 동의를 받은 뒤 구현한다.
설계 시 다룰 것: 가장 오래된 스냅샷 xmin 추적(지금 없음), heap 페이지 정리(prune)와 line pointer 상태, 인덱스 엔트리 일괄 삭제, B+Tree 2단계 페이지 삭제(HALF_DEAD/DELETED 플래그 자리 있음), FSM 도입 여부, vacuum의 WAL 기록과 hint, CLOG 잘라내기 시점.

## 이전 단계에서 넘어온 한계와 미결 사항

- (6단계에서 해소) 미커밋 행 비가시, 커밋 flush 후 hint, 동시 delete/update 보호.
- B+Tree 엔트리에는 가시성 정보가 없다: 인덱스 스캔은 heap에서 다시 확인해야 한다 (`fetch_visible`). unique 검사는 10단계.
- Read Committed의 WHERE 재평가(EvalPlanQual 본체)는 9단계 실행기에서. 지금은 `latest_version` 헬퍼까지.
- 가장 오래된 스냅샷 xmin 추적이 없다 (7단계 vacuum 전에 필요).
- lock manager는 전역 mutex 하나 (PG는 16 파티션). 행 갱신 대기는 공정성 보장 없음 (r7).
- CLOG 잘라내기 없음. lock_timeout 등 세션 설정은 13단계에서 노출.
- 크래시 한 번마다 인덱스 엔트리 없는 미커밋 heap 행이 남을 수 있음 (보이지 않음, vacuum 대상).
- FSM 없음, 빈 B+Tree 페이지 삭제 없음: 7단계(vacuum).
- TOAST 없음 (튜플은 한 페이지 이내).
- smgr DROP 레코드 없음, timeline 1 고정(16단계).
- WAL: insert lock 하나, flush 시 버퍼 복사, 메모리 버퍼 상한 없음.
- 버퍼 풀 크기는 동시 pin 수보다 충분히 커야 한다 (13단계에서 기동 시 검사; B+Tree insert는 레벨당 약 2개 pin).
- ha 모드에서 모든 victim의 flush가 보류될 때의 동작: 미결 (`docs/notes.md`, 20단계 전 결정).
- 관측성(tracing) 미결: 지금은 stderr 출력.
