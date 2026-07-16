# 데일리 전섹션 재추출 러너 v2 (LLM 없음 · 배치 병렬 · ~1분)
# (a) python: queries.py → _queries.json (월요일 아니면 ages·lead_growth 제외 = 주1회 실행, 비용절감)
# (b) node athena_batch.mjs: MCP(mcp-remote) 직접 병렬 실행 + 1000행캡 자동 페이지네이션 → tmp/{name}.json
# (c) python build.py: 결정적 재구성 (3월정합 assert 통과시에만 기록 = 라이브안전)
# (d) BUILD OK 시에만 commit/push
$ErrorActionPreference = 'Continue'
$repo   = 'C:\Users\yys\living-pb-report'
$log    = 'C:\Users\yys\tf-daily-full.log'
$alert  = 'C:\Users\yys\tf-alert.log'
$py     = 'C:\Users\yys\AppData\Local\Programs\Python\Python313\python.exe'
$node   = 'C:\Program Files\nodejs\node.exe'
Set-Location $repo
function Log($m){ $t=(Get-Date).ToString('yyyy-MM-dd HH:mm:ss'); Add-Content $log "[$t] $m" -Encoding UTF8 }
Log "######## [daily-batch] START ########"

# (a) 쿼리 매니페스트 덤프 (월요일만 ages·lead_growth 포함; 그 외 요일엔 제외 → build.py가 기존 leadGrowth/ages 섹션 보존)
& $py -c "import daily_pipeline.queries as q,json,datetime; d=dict(q.QUERIES); [d.pop(k,None) for k in (('ages','lead_growth') if datetime.date.today().weekday()!=0 else ())]; json.dump(d,open('daily_pipeline/_queries.json','w',encoding='utf-8'),ensure_ascii=False); print('dumped',len(d))" *>> $log
if ($LASTEXITCODE -ne 0){ Log "[ALERT] queries dump 실패"; Add-Content $alert "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] [ALERT] daily-batch queries dump 실패" -Encoding UTF8; exit 1 }

# (a2) 소스 적재 게이트 (8MB·2초). 어제분 UV/퍼널이 아직 없으면 34쿼리 풀런(~160GB)은 통째로 낭비 —
# 없는 데이터로 어제 날짜를 다시 빌드하고 SUCCESS로 끝나 겉보기 정상이 됨. 헬스체크가 09:20~10:00
# 10분마다 재시도하므로, 적재 즉시 다음 시도에서 풀런된다.
# fail-open: 게이트 판정 불가(쿼리 실패·파싱 실패)면 풀런 진행 = 게이트 없던 기존 동작.
$gateDir = "$repo\daily_pipeline\_gate"
$gateSql = "SELECT MIN(mx) AS mx FROM (SELECT CAST(MAX(base_dt) AS VARCHAR) AS mx FROM ba_preserved.commerce_daily_user_count_v3 WHERE CAST(base_dt AS VARCHAR) >= CAST(DATE_ADD('day',-5,CURRENT_DATE) AS VARCHAR) AND base='product' AND paid_type='total' UNION ALL SELECT CAST(MAX(period) AS VARCHAR) AS mx FROM ba_preserved.comm_product_perform WHERE yyyymm IN (date_format(CURRENT_DATE,'%Y%m'), date_format(DATE_ADD('month',-1,CURRENT_DATE),'%Y%m')) AND period_base='day' AND period >= CAST(DATE_ADD('day',-5,CURRENT_DATE) AS VARCHAR))"
Remove-Item "$gateDir\*.json" -Force -ErrorAction SilentlyContinue
# node의 JSON.parse는 BOM에서 깨지므로 BOM 없이 기록
[IO.File]::WriteAllText("$repo\daily_pipeline\_gate.json", (@{readiness=$gateSql} | ConvertTo-Json -Compress), (New-Object Text.UTF8Encoding $false))
& $node "$repo\daily_pipeline\athena_batch.mjs" "daily_pipeline\_gate.json" "daily_pipeline\_gate" 1 *>> $log
$srcMax = $null
try { $srcMax = (Get-Content "$gateDir\readiness.json" -Raw -Encoding UTF8 | ConvertFrom-Json).data[0][0] } catch { }
$want = (Get-Date).Date.AddDays(-1)
$gateOk = $true
if ($srcMax) { try { $gateOk = ([datetime]::ParseExact($srcMax,'yyyy-MM-dd',$null) -ge $want) } catch { $gateOk = $true } }
if (-not $gateOk) {
  Log "######## [daily-batch] SKIP: 소스 미적재 src_max=$srcMax (want>=$($want.ToString('yyyy-MM-dd'))). 풀런 생략 — 헬스체크가 재시도 ########"
  exit 0
}
Log "[daily-batch] gate OK src_max=$srcMax"

# (b) 배치 병렬 추출 (페이지네이션)
# tmp 선청소 필수: 쿼리 실패 시 파일을 안 남겨야 build.py가 기존 섹션값을 보존함.
# (안 지우면 실패한 쿼리가 직전 실행 결과를 남겨 build.py가 stale 데이터를 최신으로 오인)
Remove-Item "$repo\daily_pipeline\tmp\*.json" -Force -ErrorAction SilentlyContinue
& $node "$repo\daily_pipeline\athena_batch.mjs" "daily_pipeline\_queries.json" "daily_pipeline\tmp" 5 *>> $log
$batchCode = $LASTEXITCODE
Log "[daily-batch] athena_batch exit=$batchCode"

# (c) 결정적 빌드 (fail-safe: assert 통과시에만 파일기록)
$buildOut = & $py "daily_pipeline\build.py" 2>&1
$buildOut | ForEach-Object { Add-Content $log $_ -Encoding UTF8 }
$ok = ($buildOut -match 'BUILD OK') -and ($buildOut -match '3.*OK')

if ($ok) {
  & git add tf-data.json tf-mattress-data.json *>> $log
  & git commit -m "data: 데일리 전섹션 자동 재추출 (배치 파이프라인)" *>> $log
  & git push origin main *>> $log
  $lu = (Get-Content (Join-Path $repo 'tf-data.json') -Raw -Encoding UTF8 | ConvertFrom-Json).meta.lastUpdate
  Log "######## [daily-batch] SUCCESS lastUpdate=$lu ########"
  exit 0
} else {
  Log "######## [daily-batch] BUILD 실패 - 커밋 안함(라이브 무손상) ########"
  Add-Content $alert "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] [ALERT] daily-batch BUILD 실패 (batch exit=$batchCode)" -Encoding UTF8
  exit 1
}
