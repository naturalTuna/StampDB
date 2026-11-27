# 진행 상황

마지막 갱신: 2026-11-27 (1단계 7번 완료, 8번 설계 전)

## 진행률

| 기준 | 진행률 |
|---|---|
| 전체 로드맵 22개 항목 중 완료 | 7개 (약 32%) |
| 1단계(단일 노드, psql 접속까지) 13개 중 완료 | 7개 (약 54%) |
| 작업량 가중 추정 (전체) | 약 26~28% |

남은 큰 덩어리: 카탈로그(8), SQL 계층(9), 제약조건(10, 11), 통계(12), 서버(13), 그리고 2·3단계 HA.

## 완료한 단계

| 단계 | 내용 | 주요 커밋 |
|---|---|---|
| 1-1 | 디스크 매니저 + 페이지 포맷: aligned buffer, 페이지 헤더, slotted page, CRC32C 체크섬, 파일 접근 trait(fencing 토큰), O_DIRECT 로컬 백엔드, DiskManager | `9b86420`..`ca7a42f` |
| 1-2 | 버퍼 풀: 파티션 매핑, PinGuard/PageRef/PageMut, clock-sweep(PG give-up 규칙), 복사 후 체크섬 flush, FlushControl, extend/flush/drop | `e561e5a`..`ad98a09` |
| 1-3 | Heap + 튜플: 데이터 타입과 varlena, 35B 튜플 헤더(64비트 xid, cmin/cmax 분리), null bitmap, heap insert/fetch/delete/update/scan, 기회적 hint bit | `b65d811`..`48e6358` |
| 1-4 | B+Tree: memcomparable 키 + TID 접미사, Lehman-Yao(right-link, high key), split/루트 split, 90:10, 범위 스캔, 삭제, 동시성 테스트 | `6fb5b00`..`9a805ec` |
| 1-7 | 수동 VACUUM: xmin horizon 등록, HeapTupleSatisfiesVacuum, heap prune/vacuum(DEAD→UNUSED), 메모리 FSM과 insert 재사용, B+Tree bulk delete(물리 순서 스캔), 빈 leaf 2단계 삭제(HALF_DEAD→DELETED), vacuum 진입점, 통합 테스트 | `522717c`..(이 커밋) |
| 1-6 | 트랜잭션 + MVCC + lock manager: TxnId/ProcArray/스냅샷/trait, CLOG(버퍼 풀 relation)와 커밋·abort WAL, 가시성 판단(스냅샷 우선), PG 8모드 lock manager, deadlock 검출, lock_timeout, Transaction(RC/SI), heap MVCC DML(TM_Result, xid 대기, SI 직렬화 오류, 갱신 체인), 가시 스캔과 hint 일괄 설정, 격리 수준 시나리오 테스트 | `0ecc010`..(이 커밋) |
| 1-5 | WAL + 복구: 레코드 형식, 세그먼트/control 파일, FPW, hint FPI, WAL-before-data, heap/btree physiological 로깅과 redo, INCOMPLETE_SPLIT 마무리, 복구, 체크포인트, pending sync, 체크포인트 스레드, bgwriter, 세그먼트 삭제, kill -9 테스트 | `1f9ca23`..`a9432b5` |

DESIGN.md 변경 이력: r3(블록 크기 sysfs, CRC32C, sync 토큰) / r4(hint bit 설정 방식, cmin/cmax, 패딩 없음, NUMERIC·타입 OID) / r5(B+Tree 키·TID·NULL·split) / r6(WAL 방식, abort 정책, control 파일, 세그먼트, kill -9 테스트) / r7(CLOG, lock 모드, 행 대기, xid 발급 시점, 동기 커밋, SAVEPOINT 제외) / r8(vacuum horizon, ShareUpdateExclusive, 메모리 FSM, B+Tree 페이지 삭제 범위).

## 모듈 지도

```
src/storage   페이지, slotted page, 체크섬, 파일 접근 trait, O_DIRECT 백엔드, DiskManager
src/buffer    버퍼 풀, PinGuard, FlushControl
src/types     데이터 타입과 직렬화 (varlena)
src/access    tuple(튜플 형식), heap(MVCC DML 포함), btree(key/page/tree), visibility, redo 분기
src/txn       Xid/TxnId, ProcArray, Snapshot, CLOG, LocalTxnManager, Transaction(RC/SI)
src/lock      lock manager (PG 8모드, FIFO, xid 대기, deadlock 검출, lock_timeout)
src/vacuum    수동 VACUUM 진입점 (prune → 인덱스 bulk delete → UNUSED → 빈 leaf 삭제)
src/wal       record, log(WAL 쓰기/읽기), control, recovery(Engine, initdb, checkpoint), background
tests/        O_DIRECT 통합 테스트, WAL, 버퍼 풀 스트레스, kill -9, 격리 수준 시나리오, vacuum
docs/         SETUP.md(환경), PROGRESS.md(이 문서), notes.md(구현 중 미결 사항)
```

테스트: 단위 약 200개 + 통합 약 32개 (모두 통과).

## 다음 단계: 1단계 8번

카탈로그(시스템 테이블). 설계안을 제시하고 동의를 받은 뒤 구현한다.
먼저 open_questions의 "DDL의 트랜잭션성"(카탈로그를 MVCC 테이블로 할지)을 사용자에게 확인해야 한다 (8단계 전 결정).
지금은 relation 생성(smgr 기록, BTree::create)이 커밋과 묶여 있지 않아, WAL이 flush되기 전에 크래시하면 DDL이 사라진다.

## 이전 단계에서 넘어온 한계와 미결 사항

- (6단계에서 해소) 미커밋 행 비가시, 커밋 flush 후 hint, 동시 delete/update 보호.
- B+Tree 엔트리에는 가시성 정보가 없다: 인덱스 스캔은 heap에서 다시 확인해야 한다 (`fetch_visible`). unique 검사는 10단계.
- Read Committed의 WHERE 재평가(EvalPlanQual 본체)는 9단계 실행기에서. 지금은 `latest_version` 헬퍼까지.
- (7단계에서 해소) xmin horizon 추적.
- vacuum은 죽은 TID를 전부 메모리에 담는다 (PG는 maintenance_work_mem 단위 배치).
- FSM은 메모리 기반이라 재시작 후 다음 vacuum까지 비어 있다 (r8). update의 새 버전은 FSM을 쓰지 않는다.
- 삭제된 B+Tree 페이지는 재사용하지 않는다. 부모의 마지막 자식인 빈 leaf와 빈 내부 페이지는 삭제하지 않는다 (r8).
- 빈 leaf 삭제는 부모를 찾으려고 부모 레벨을 왼쪽부터 훑는다 (큰 인덱스에서 느림).
- CLOG 잘라내기 없음.
- lock manager는 전역 mutex 하나 (PG는 16 파티션). 행 갱신 대기는 공정성 보장 없음 (r7).
- CLOG 잘라내기 없음. lock_timeout 등 세션 설정은 13단계에서 노출.
- 크래시 한 번마다 인덱스 엔트리 없는 미커밋 heap 행이 남을 수 있음 (보이지 않음, vacuum 대상).
- TOAST 없음 (튜플은 한 페이지 이내).
- smgr DROP 레코드 없음, timeline 1 고정(16단계).
- WAL: insert lock 하나, flush 시 버퍼 복사, 메모리 버퍼 상한 없음.
- 버퍼 풀 크기는 동시 pin 수보다 충분히 커야 한다 (13단계에서 기동 시 검사; B+Tree insert는 레벨당 약 2개 pin).
- ha 모드에서 모든 victim의 flush가 보류될 때의 동작: 미결 (`docs/notes.md`, 20단계 전 결정).
- 관측성(tracing) 미결: 지금은 stderr 출력.
