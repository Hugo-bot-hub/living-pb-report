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
