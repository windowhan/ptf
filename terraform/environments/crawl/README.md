# environments/crawl — tailnet IP-다양성 크롤 워커

IP가 다른 spot VM들을 띄워 크롤 egress IP를 분산하는 풀이다.
오케스트레이터와 상태 DB는 GCP 밖(운영자 머신)에서 돌고, 워커는
Tailscale 오버레이로 DB에 붙는다 — Cloud SQL / Pub/Sub / 전용 VPC
없이 VM + 외부 IP만 쓴다.

```text
[operator machine]                        [GCP spot e2-micro × N]
  postgres (docker :55432)  ◄── tailnet ──  worker (RUNTIME_DSN→100.x)
  control (planner loop)                    external IP = crawl egress
```

## 워커가 하는 일 (부팅 시)

1. Secret Manager에서 `TAILSCALE_AUTH_KEY`와 `RUNTIME_DSN`을
   instance SA 토큰으로 가져온다.
2. COS에 tailscale static 바이너리를 깔고 `tailscale up` — 호스트
   데몬이라 `--network host` 컨테이너가 tailnet을 직접 탄다.
3. `python -m distributed_runtime worker` 실행 — DB 폴링으로
   `execution_revision=crawl`인 unit만 claim한다.

## 셋업

```bash
# 1) 맥에 tailscale — https://tailscale.com/download (brew install --cask tailscale)
tailscale ip -4   # → 100.x.x.x (이 주소로 원격 워커가 PG를 찾음)

# 2) 로컬 PG가 tailnet에서 보이도록 — docker 포트가 0.0.0.0에 publish돼
#    있으면 추가 작업 없음 (현재 ptf-pg는 0.0.0.0:55432->5432)

# 3) tailnet에서 재사용 가능한 ephemeral auth key 생성
#    https://login.tailscale.com/admin/settings/keys → "Generate auth key"
#    ✓ Reusable  ✓ Ephemeral (spot 재생성 시 노드 자동 정리)

# 4) terraform으로 시크릿 껍데기 생성 후 값 주입
terraform apply -var="project=..." -var="image=..."  # 1차: 시크릿+MIG
echo -n 'tskey-auth-XXXX' \
  | gcloud secrets versions add crawl-tailscale-auth-key --data-file=-
echo -n 'postgresql://postgres:postgres@100.x.x.x:55432/runtime' \
  | gcloud secrets versions add crawl-worker-dsn --data-file=-
# 시크릿이 생기기 전에 뜬 워커는 startup fetch 실패로 대기 → 재생성되면 흡수
```

워커 이미지는 루트 `Dockerfile`로 빌드 (`dist/`에 `distributed_runtime`
+ `crawler_product` wheel이 필요):

```bash
python -m build --wheel && (cd examples/crawler_example && python -m build --wheel)
cp examples/crawler_example/dist/*.whl dist/
docker build -t us-central1-docker.pkg.dev/<project>/runtime/crawl-worker:TAG .
docker push ...
```

## 실행

맥에서 control만 띄우고 (planner가 run을 unit으로 쪼갬), run은
`execution_revision="crawl"`로 제출:

```python
client = RuntimeClient(
    engine, application="crawler-product",
    planner_revision=RevisionId("crawl"),
    execution_revision=RevisionId("crawl"),
)
run = await client.submit(workload="crawl.fetch", version="1.0.0",
                          input={"urls": [...]})
```

`pool_revision=crawl`인 원격 워커만 이 unit들을 claim한다 — 로컬
워커(`local`)와 claim이 갈리므로 섞여도 안전하다.

## 쿼터 상한 (기본값)

| 쿼터 | 값 | 의미 |
|---|---|---|
| `IN_USE_ADDRESSES` | 8/region | 리전당 egress IP 수 |
| `INSTANCES` | 24/region | |
| `CPUS_ALL_REGIONS` | 32 global | e2-micro ≈ 2vCPU → 전체 ~16대 |

`regions`에 리전을 추가하면 IP가 리전당 8개씩 늘고, 쿼터 증설 신청하면
그 이상. spot이라 preempt될 때마다 MIG가 재생성 → IP 자동 로테이션.

## 비용

spot e2-micro ≈ $2/월/대. size_per_region=4 × 1리전 ≈ **$8/월에 4 IP**.
대역폭은 response(인바운드)가 무료라 크롤링은 사실상 요청만 유출.

## 주의

- `RUNTIME_DSN`에 로컬 PG 비밀번호가 들어가므로 Secret Manager 경유,
  컨테이너 env로만 전달 (디스크/메타데이터에 남지 않음).
- tailnet ACL로 `tag:crawl`에 PG 포트만 열어두면 워커 키가 새어도
  피해가 작다 (auth key 생성 시 태그 지정 권장).
- preempt 시 진행 중 unit은 claim_expiry 경과 후 다른 워커가 재claim —
  멱등성은 unit_key로 보장.
