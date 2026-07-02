# 데일리 전섹션 재추출 러너 (STAGED — 아직 스케줄러 미등록).
# (a) claude -p 가 queries.py 전 쿼리를 MCP 실행 → tmp/{name}.json 저장
# (b) build.py 로 tf-data.json + tf-mattress-data.json 결정적 재구성
# (c) 성공(3월정합 통과) 시에만 commit/push
# 라이브보호: build.py 는 assert 통과 시에만 파일 기록 → 실패 시 기존 데이터 무손상.
$ErrorActionPreference = 'Continue'
$repo   = 'C:\Users\yys\living-pb-report'
$log    = 'C:\Users\yys\tf-daily-full.log'
$claude = 'C:\Users\yys\AppData\Roaming\npm\claude.cmd'
$inner  = 'C:\Users\yys\tf-update-inner.ps1'  # 기존 크래시격리 자식(프롬프트=TF_PROMPT) 재사용
Set-Location $repo
New-Item -ItemType Directory -Force (Join-Path $repo 'daily_pipeline\tmp') | Out-Null

$prompt = @'
데일리 대시보드 전섹션 재추출. 작업 폴더 C:\Users\yys\living-pb-report. 순서대로:
1) `python -c "import daily_pipeline.queries as q,json;print(json.dumps(list(q.QUERIES)))"` 로 쿼리명 목록 확보. 오늘이 월요일이 아니면 목록에서 'ages' 제외(연령은 주1회).
2) 각 name 마다: `python -c "import daily_pipeline.queries as q;print(q.QUERIES['NAME'])"` 로 SQL 얻어 mcp__ohouse-athena-mcp__execute_athena_query 실행. 결과 JSON의 data 배열을 daily_pipeline/tmp/{name}.json 에 {"data":[...]} 형식으로 Write 저장. 결과가 대용량으로 파일 persist되면 그 파일의 data를 읽어 저장. 각 쿼리 status=SUCCEEDED 확인.
3) 전 쿼리 저장 후 `python daily_pipeline/build.py` 실행. 출력에 "BUILD OK" 와 "3월정합 OK" 있어야 성공.
4) build.py 가 assert 실패/에러면 **commit 금지**, 로그만. 성공이면: git add tf-data.json tf-mattress-data.json && git commit -m "data: 데일리 전섹션 자동 재추출" && git push origin main.
5) 결과 1줄 보고(성공/실패, lastUpdate, 갱신 섹션 수).
net GMV=필터없이 SUM(gmv). 쿼리는 queries.py 그대로 사용(수정 금지).
'@

$ts0 = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
Add-Content $log "`n######## [daily-full] START $ts0 ########" -Encoding UTF8
$env:TF_PROMPT = $prompt
$env:TF_MODEL = ''
$beforeIds = @((Get-Process claude,node -ErrorAction SilentlyContinue).Id)
try {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName='powershell.exe'; $psi.Arguments='-NoProfile -ExecutionPolicy Bypass -File "'+$inner+'"'
  $psi.UseShellExecute=$false; $psi.CreateNoWindow=$true
  $p=[System.Diagnostics.Process]::Start($psi)
  if($p.WaitForExit(2700000)){ $code=$p.ExitCode } else { taskkill.exe /PID $p.Id /T /F *> $null; $code=-2 }
} catch { $code=-1 }
# 좀비 청소(이번 실행 잔존분만)
$leak=@(Get-Process claude,node -ErrorAction SilentlyContinue | Where-Object { $beforeIds -notcontains $_.Id })
foreach($lp in $leak){ try{ Stop-Process -Id $lp.Id -Force -ErrorAction SilentlyContinue }catch{} }
$after = (Get-Content (Join-Path $repo 'tf-data.json') -Raw -Encoding UTF8 | ConvertFrom-Json).meta.lastUpdate
Add-Content $log "[daily-full] END exit=$code lastUpdate=$after $(Get-Date -Format 'HH:mm:ss')" -Encoding UTF8
