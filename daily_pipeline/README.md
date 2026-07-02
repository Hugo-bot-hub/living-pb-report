# 데일리 전섹션 자동 재추출 파이프라인 (STAGED)

대시보드의 **모든 숫자**를 매일 결정적으로 재추출한다.

## 구성
- `queries.py` — 28개 쿼리 매니페스트. 날짜 상대(DATE_ADD)라 항상 최신. 상품/설정 상수.
- `build.py` — 결정적 빌더. `tmp/{name}.json`(Athena data 배열) → `tf-data.json` + `tf-mattress-data.json` 전 섹션 재구성. **fail-safe**: 3월정합(3918642=11,553,700·3640244=45,449,200) + array 불변식 assert 통과 시에만 파일 기록 → 실패 시 기존 데이터 무손상. daily는 history 보존+최근50일 갱신.
- `run.ps1` — 러너(robust). claude -p 가 (a)queries 전부 MCP실행→tmp 저장 (b)build.py 실행 (c)성공시 commit/push. tf-update-inner.ps1(크래시격리) 재사용.

## 재구성 섹션 (18상품, 빠짐없이)
tf-data.json: daily(퍼널∩UV+GMV) · srpMatrix(14d,best=P5) · srpKeywordRank(통합검색량 2w) · srpKeywords · scoreTs/featTs(60d) · inflow(14d 4윈도우) · inflowCvr(7d) · ages(월1회) · benchmarks{bedding,mattress,bed}(7d) · categoryBenchmark · meta
tf-mattress-data.json: decomp(객단가,커버제외) · attach_frame_option · cosell_by_tier+summary · mat_to_frame_by_tier+summary

## 활성화 (검증 후 스케줄러 전환)
현재 STAGED — 스케줄러 미등록. 라이브 대시보드 리스크 없이 검증 후 전환.
1. **1회 수동 실행**: `powershell -File C:\Users\yys\living-pb-report\daily_pipeline\run.ps1`
2. 로그(`C:\Users\yys\tf-daily-full.log`)에 성공 + git 커밋 확인. `git show`로 tf-data.json diff 점검(핵심 숫자 현행과 유사한지).
3. 대시보드 URL 렌더 이상 없으면 스케줄러 전환:
   - 기존 `tf-update-robust.ps1` → `tf-update-robust-v17-backup.ps1` 백업
   - `PB_TF_Update` Action 을 `run.ps1` 로 교체 (Register-ScheduledTask 재등록)
4. 다음날 06:30 자동실행 결과 검증.

⚠️ build.py 는 실패 시 기록하지 않으므로, 잘못돼도 라이브 데이터는 안전. 다만 스케줄러 전환 전 1회 수동검증 권장.
