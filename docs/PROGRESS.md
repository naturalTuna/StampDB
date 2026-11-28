# 진행 상황

마지막 갱신: 2026-11-28 (1단계 12번 완료)

## 진행률

| 기준 | 진행률 |
|---|---|
| 전체 로드맵 22개 항목 중 완료 | 12개 (약 55%) |
| 1단계(단일 노드, psql 접속까지) 13개 중 완료 | 12개 (약 92%) |
| 작업량 가중 추정 (전체) | 약 43% |

남은 큰 덩어리: 서버(13), 그리고 2·3단계 HA.

## 완료한 단계

| 단계 | 내용 | 주요 커밋 |
|---|---|---|
| 1-1 | 디스크 매니저 + 페이지 포맷: aligned buffer, 페이지 헤더, slotted page, CRC32C 체크섬, 파일 접근 trait(fencing 토큰), O_DIRECT 로컬 백엔드, DiskManager | `9b86420`..`ca7a42f` |
| 1-2 | 버퍼 풀: 파티션 매핑, PinGuard/PageRef/PageMut, clock-sweep(PG give-up 규칙), 복사 후 체크섬 flush, FlushControl, extend/flush/drop | `e561e5a`..`ad98a09` |
| 1-3 | Heap + 튜플: 데이터 타입과 varlena, 35B 튜플 헤더(64비트 xid, cmin/cmax 분리), null bitmap, heap insert/fetch/delete/update/scan, 기회적 hint bit | `b65d811`..`48e6358` |
| 1-4 | B+Tree: memcomparable 키 + TID 접미사, Lehman-Yao(right-link, high key), split/루트 split, 90:10, 범위 스캔, 삭제, 동시성 테스트 | `6fb5b00`..`9a805ec` |
| 1-12 | 테이블 통계 + autovacuum: stats 모듈(메모리 카운터, 트랜잭션 종료 시 pgstat 규칙으로 반영, 시스템 테이블 포함), prune의 live/recently dead 집계, SQL VACUUM [테이블](25001, 테이블별 트랜잭션), autovacuum 스레드(50 + 0.2 × live 임계식, lock 즉시 실패 시 건너뜀, 삭제된 테이블 통계 정리), pg_stat_user_tables 가상 뷰, 동시 DML + 크래시 통합 테스트 | `ee55ba0`..(이 커밋) |
| 1-11 | 키 잠금 + FOREIGN KEY: lock manager의 Key 태그(상한, 53200), sys_constraint FK 컬럼, 참조 unique 인덱스 해석·컬럼 순서·타입 호환, 참조되는 테이블/인덱스 DROP 거부, Writer(제약·인덱스·FK 일원화), 자식 Share/부모 Exclusive 키 잠금과 검사(RC 최신 상태, SI 직렬화 오류), CASCADE/SET NULL/SET DEFAULT(재귀, 동시 갱신 추적), foreign_keys.slt | `e05041c`..`79b7d1b` |
| 1-10 | 제약조건: sys_constraint, attdefault, CREATE TABLE의 컬럼/테이블 제약(PK=unique 인덱스+NOT NULL, UNIQUE, CHECK 텍스트 저장 후 재바인딩), PG 제약 이름 규칙, NOT NULL→CHECK(이름 순)→인덱스 순서의 검사(EPQ 재계산 행 포함), DEFAULT(키워드, DEFAULT VALUES), 제약 인덱스 DROP 거부, constraints.slt | `f5c0dfb`..(이 커밋) |
| 1-9 | SQL 계층: SQLSTATE 오류, PG 텍스트 입출력(float8 최단 표현, timestamp), 식 평가(3값 논리, overflow, 캐스트), 바인더(PG 타입 규칙, unknown/numeric 리터럴, 집계·GROUP BY·HAVING·ORDER BY·DISTINCT·LIMIT, INNER/LEFT/CROSS JOIN), Volcano 실행기(seq/index scan, nested loop/hash join, hash 집계, 정렬), 세션과 트랜잭션 블록(25P02), DDL, INSERT/UPDATE/DELETE(RC EvalPlanQual, SI 직렬화 오류), 규칙 기반 플래너(push-down, 인덱스 범위, hash join), sqllogictest(PG 18로 기대값 생성), SQL 크래시 테스트 | `12479e5`..(이 커밋) |
| 1-8 | 카탈로그: 부트스트랩 xid, OID 할당(NEXTOID WAL), pending file deletes(COMMIT/ABORT 레코드의 relation 목록), B+Tree unique 검사(10단계에서 앞당김), sys_class/sys_attribute/sys_index와 인덱스, catalog::initdb, MVCC 조회, 트랜잭션 CREATE/DROP TABLE, CREATE/DROP INDEX(기존 행 색인), DML용 인덱스 유지 헬퍼, 카탈로그 vacuum | `73bdc31`..(이 커밋) |
| 1-7 | 수동 VACUUM: xmin horizon 등록, HeapTupleSatisfiesVacuum, heap prune/vacuum(DEAD→UNUSED), 메모리 FSM과 insert 재사용, B+Tree bulk delete(물리 순서 스캔), 빈 leaf 2단계 삭제(HALF_DEAD→DELETED), vacuum 진입점, 통합 테스트 | `522717c`..(이 커밋) |
| 1-6 | 트랜잭션 + MVCC + lock manager: TxnId/ProcArray/스냅샷/trait, CLOG(버퍼 풀 relation)와 커밋·abort WAL, 가시성 판단(스냅샷 우선), PG 8모드 lock manager, deadlock 검출, lock_timeout, Transaction(RC/SI), heap MVCC DML(TM_Result, xid 대기, SI 직렬화 오류, 갱신 체인), 가시 스캔과 hint 일괄 설정, 격리 수준 시나리오 테스트 | `0ecc010`..(이 커밋) |
| 1-5 | WAL + 복구: 레코드 형식, 세그먼트/control 파일, FPW, hint FPI, WAL-before-data, heap/btree physiological 로깅과 redo, INCOMPLETE_SPLIT 마무리, 복구, 체크포인트, pending sync, 체크포인트 스레드, bgwriter, 세그먼트 삭제, kill -9 테스트 | `1f9ca23`..`a9432b5` |

DESIGN.md 변경 이력: r3(블록 크기 sysfs, CRC32C, sync 토큰) / r4(hint bit 설정 방식, cmin/cmax, 패딩 없음, NUMERIC·타입 OID) / r5(B+Tree 키·TID·NULL·split) / r6(WAL 방식, abort 정책, control 파일, 세그먼트, kill -9 테스트) / r7(CLOG, lock 모드, 행 대기, xid 발급 시점, 동기 커밋, SAVEPOINT 제외) / r8(vacuum horizon, ShareUpdateExclusive, 메모리 FSM, B+Tree 페이지 삭제 범위) / r9(트랜잭션 DDL) / r10(카탈로그 이름·캐시 없음·relname unique·부트스트랩 xid·pending deletes·OID·CREATE INDEX lock) / r11(SQL 범위, 규칙 기반 플래너, 메모리 내 처리, 타입 규칙과 NUMERIC 전 처리, 출력 형식, 세션 규칙, sqllogictest용 PG) / r12(sys_constraint, CHECK·DEFAULT 텍스트 저장, PK 구성, 10단계 범위) / r13(키 잠금, FK 검사 방식, CASCADE/SET NULL/SET DEFAULT를 open_questions에서 이동) / r14(테이블 통계, VACUUM 명령, autovacuum, pg_stat_user_tables, 플래너 통계 제외).

## 모듈 지도

```
src/storage   페이지, slotted page, 체크섬, 파일 접근 trait, O_DIRECT 백엔드, DiskManager
src/buffer    버퍼 풀, PinGuard, FlushControl
src/types     데이터 타입과 직렬화 (varlena)
src/access    tuple(튜플 형식), heap(MVCC DML 포함), btree(key/page/tree), visibility, redo 분기
src/txn       Xid/TxnId, ProcArray, Snapshot, CLOG, LocalTxnManager, Transaction(RC/SI)
src/lock      lock manager (PG 8모드, FIFO, xid 대기, deadlock 검출, lock_timeout)
src/stats     테이블 통계 (메모리 카운터, 트랜잭션별 집계 반영, vacuum 결과)
src/vacuum    수동 VACUUM 진입점 (prune → 인덱스 bulk delete → UNUSED → 빈 leaf 삭제)
src/catalog   시스템 카탈로그 (schema, initdb, 조회, CREATE/DROP TABLE/INDEX, 인덱스 유지)
src/sql       error(SQLSTATE), datum(텍스트 입출력), expr(식 평가), binder, plan, planner, exec(Volcano), session(트랜잭션 블록, DML),
              writer(행 쓰기: 제약, 인덱스, FK와 연쇄 동작), ri(FK 검사와 키 잠금), autovacuum(백그라운드 스레드), sysviews(pg_stat_user_tables)
src/wal       record, log(WAL 쓰기/읽기), control, recovery(Engine, initdb, checkpoint), background
tests/        O_DIRECT 통합 테스트, WAL, 버퍼 풀 스트레스, kill -9, 격리 수준 시나리오, vacuum, 카탈로그, SQL 크래시,
              sqllogictest(tests/slt/*.slt, 기대값은 PostgreSQL 18 C locale이 생성; docs/SETUP.md)
docs/         SETUP.md(환경), PROGRESS.md(이 문서), notes.md(구현 중 미결 사항)
```

테스트: 단위 약 262개 + 통합 약 37개 + sqllogictest 8개 파일 (모두 통과).

## 다음 단계: 1단계 13번

서버: 동기 프로토콜 루프(pgwire 코덱) + 세션 관리 + trust 인증 + 접속 허용 목록. 설계안을 정하고 추천안으로 진행한다.
open_questions의 pg_catalog 호환 범위(답변: 기본)와 관측성(로그) 항목이 이 단계에서 필요해진다. 함께 노출할 것: 세션 설정(lock_timeout 등), 키 잠금 상한, autovacuum 설정, pg_stat_activity, pg_locks.

## 이전 단계에서 넘어온 한계와 미결 사항

- (6단계에서 해소) 미커밋 행 비가시, 커밋 flush 후 hint, 동시 delete/update 보호.
- B+Tree 엔트리에는 가시성 정보가 없다: 인덱스 스캔은 heap에서 다시 확인해야 한다 (`fetch_visible`). unique 검사는 10단계.
- (9단계에서 해소) Read Committed의 EvalPlanQual.
- (7단계에서 해소) xmin horizon 추적.
- vacuum은 죽은 TID를 전부 메모리에 담는다 (PG는 maintenance_work_mem 단위 배치).
- FSM은 메모리 기반이라 재시작 후 다음 vacuum까지 비어 있다 (r8). update의 새 버전은 FSM을 쓰지 않는다.
- 삭제된 B+Tree 페이지는 재사용하지 않는다. 부모의 마지막 자식인 빈 leaf와 빈 내부 페이지는 삭제하지 않는다 (r8).
- 빈 leaf 삭제는 부모를 찾으려고 부모 레벨을 왼쪽부터 훑는다 (큰 인덱스에서 느림).
- CLOG 잘라내기 없음.
- 카탈로그 캐시 없음: 조회마다 카탈로그 인덱스와 heap을 읽는다 (r10). 인덱스의 sys_index 행 찾기는 sys_index 스캔.
- 크래시 당시 진행 중이던 CREATE의 파일은 고아로 남는다 (PG와 같은 한계).
- unique 오류 후 트랜잭션은 반드시 abort해야 한다 (넣은 heap 행이 남음). savepoint 없음.
- (9단계에서 해소) SQL DDL.
- SQL 미지원: 서브쿼리, CTE, 윈도 함수, UNION, RIGHT/FULL JOIN, USING/NATURAL, RETURNING, EXPLAIN.
- 제약: ALTER TABLE ADD/DROP CONSTRAINT 없음 (r12). CHECK/DEFAULT는 문장마다 텍스트를 다시 파싱·바인딩한다 (캐시 없음). 이름 길이 63바이트 절단 없음. UNIQUE NULLS NOT DISTINCT 미지원.
- 열 목록 없는 `INSERT INTO t SELECT * FROM 더 좁은 테이블`은 PG와 달리 오류 (VALUES와 명시적 select 목록은 PG처럼 앞 컬럼만 채움).
- FK: NO ACTION도 행 단위 즉시 검사 (PG는 문장 끝 검사라 `UPDATE p SET id = id + 1`처럼 키를 밀어내는 문장이 PG에서는 되고 여기서는 실패할 수 있음, r13). 자식 쪽 검사도 행 단위라 한 INSERT 안에서 뒤 행을 참조하는 앞 행은 실패. MATCH FULL, DEFERRABLE, ALTER TABLE로 FK 추가 없음. 자식 인덱스가 없으면 부모 삭제마다 자식 전체 스캔.
- 키 잠금 상한(100만)은 아직 설정으로 노출하지 않음 (13단계).
- 통계(r14): 메모리에만 있어 재시작(정상 종료 포함)하면 초기화된다 (PG는 정상 종료 시 보존). vacuum이 덮어쓰는 동안 반영된 변경 일부가 사라질 수 있다(PG와 같은 경쟁). seq_scan/idx_scan 등 읽기 통계, n_mod_since_analyze, pg_stat_all_tables/sys 뷰 없음. 시각은 timestamp(UTC).
- VACUUM은 테이블 하나 또는 전체만 (sqlparser가 `VACUUM a, b`와 `VACUUM (옵션)`을 파싱하지 못함). FULL 등 옵션 0A000. pg_stat_user_tables는 쓰기 시도 시 42P01(PG는 뷰 변경 오류).
- autovacuum: 스레드 하나가 테이블을 순서대로 처리, 실행 중 충돌 lock 요청이 와도 스스로 취소하지 않음 (DDL이 기다림). 삽입 기반 autovacuum과 테이블별 설정 없음. 설정은 구조체로만 받음 (13단계에서 노출). 오류는 stderr.
- 플래너 통계 없음: 규칙 기반 플래너 유지 (r14). 부모의 다른 컬럼만 바꾸는 UPDATE는 키 잠금을 잡지 않는다 (PG의 FOR NO KEY UPDATE와 같은 효과).
- 커밋 `e05041c`(DESIGN r13 문서 커밋)에 키 잠금 코드가 함께 들어갔다 (다음 커밋 `a48db1b`가 그 설명).
- NUMERIC 없음: 소수 리터럴은 float8로 흡수되는 곳에서만 허용, avg(정수)·sum(int8)은 0A000 (r11). 정수와 소수 리터럴 비교는 float8로 한다 (int8의 2^53 넘는 값에서 PG와 다를 수 있음).
- 여러 문장을 한 번에 보내면 문장마다 따로 자동 커밋한다 (PG는 묵시적 트랜잭션 하나).
- SI 트랜잭션의 첫 스냅샷은 테이블 lock 대기 전에 잡힌다 (PG는 lock 후).
- 인덱스 스캔은 시작할 때 범위의 TID를 모두 모은다. hash join/집계/정렬/nested loop의 내부 입력은 전부 메모리 (r11).
- 결과는 문장 단위로 모두 모은 뒤 반환한다 (13단계에서 스트리밍 검토).
- NOTICE/WARNING은 문자열 목록 (13단계에서 프로토콜 메시지로).
- 플래너는 Project/Aggregate 위의 조건을 내려보내지 않는다. 인덱스 선택은 `컬럼 op 상수` 형태만 (IN, IS NULL, 타입 변환된 컬럼 제외).
- UPDATE는 HOT 없이 모든 인덱스에 새 엔트리를 넣는다.
- lock manager는 전역 mutex 하나 (PG는 16 파티션). 행 갱신 대기는 공정성 보장 없음 (r7).
- CLOG 잘라내기 없음. lock_timeout 등 세션 설정은 13단계에서 노출.
- 크래시 한 번마다 인덱스 엔트리 없는 미커밋 heap 행이 남을 수 있음 (보이지 않음, vacuum 대상).
- TOAST 없음 (튜플은 한 페이지 이내).
- smgr DROP 레코드 없음, timeline 1 고정(16단계).
- WAL: insert lock 하나, flush 시 버퍼 복사, 메모리 버퍼 상한 없음.
- 버퍼 풀 크기는 동시 pin 수보다 충분히 커야 한다 (13단계에서 기동 시 검사; B+Tree insert는 레벨당 약 2개 pin).
- ha 모드에서 모든 victim의 flush가 보류될 때의 동작: 미결 (`docs/notes.md`, 20단계 전 결정).
- 관측성(tracing) 미결: 지금은 stderr 출력.
