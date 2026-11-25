[English](README.md) | **한국어** | [日本語](README.ja.md) | [简体中文](README.zh-CN.md)

# bmo-arbiter

여러 서비스가 그래픽 카드를 함께 쓰는 GPU 노드 클러스터를 위한 카드 중재자입니다. 두 개의 프로그램으로 이루어져
있습니다.

- **노드 중재자**는 모든 GPU 노드마다 하나씩 실행되는 프로세스로, 그 노드의 카드 잠금을 관리합니다. 누가 어떤 순서로
  카드를 쓸 수 있는지, 모든 점유자가 티켓으로 차례를 기다리는 큐, 카드를 즉시 차지하는 청구자, 사용 중에 카드를
  차지하는 사람의 데스크톱, 그리고 엔진 메모리 중 호스트 메모리나 덤프 저장소로 내렸다가 다시 올리는 단위를
  다룹니다. 자기 카드를 다른 노드의 엔진에 빌려줄 수도 있습니다.
- **마스터** `bmo-arbiter`는 클러스터에 하나 있는 프로세스로, 어떤 서비스가 어느 노드에 설지 결정하고, 노드 중재자를
  설치하며, 배치 규칙에 따라 모든 카드의 메모리를 계획하고, 클러스터 전체와 그 배치를 제공합니다.

중재자는 어떤 서비스도 이름으로 알지 못합니다. 각 서비스가 스스로를 선언하고, 노드 중재자에게 자기 이름을 밝히며,
마스터의 어댑터 동사에 응답합니다. 중재자는 업스트림 유닛입니다. 하니스는 다른 유닛이 하나도 실행되지 않은 상태에서도
MCP 서버 `bmo-arbiter mcp`를 통해 카드에서 차례를 얻습니다. 슈퍼바이저는 도구 호스트에서 마스터의 사용량 라우트를
호출하는 호스팅 도구 `bmo/gpu`로 모든 카드가 무엇을 하고 있는지 읽습니다.

## 구성

| 폴더 | 내용 |
|---|---|
| `src/` | 마스터의 패키지: 라이브러리와 `bmo-arbiter` 바이너리 |
| `crates/` | 중재자가 공개하는 크레이트: 모든 본문과 모든 라우트의 호출을 담은 `client/`, 그리고 MCP 서버, `bmo-arbiter` 스킬, OpenAPI 문서 생성기를 담은 `plugin/` |
| `packages/` | `docs/openapi.json`에서 생성되는 TypeScript 클라이언트 `arbiter-client/` |
| `card/` | 노드 중재자: Python 패키지, 고정된 `pyproject.toml`, 노드 설정과 사용자 유닛. 있는 그대로 모든 노드에 업로드됨 |
| `tests/` | 저장소 검사와 모든 테스트: 마스터의 테스트는 Cargo 테스트로, 노드 중재자의 Python 테스트는 `tests/card/` 아래에 |
| `tools/` | 중재자 자체의 호스팅 도구: 슈퍼바이저가 모든 카드를 읽는 `gpu/`. 배포 시 배치되고 등록됨 |
| `docs/` | 중재자가 하는 일에 대한 정식 기술과 OpenAPI 문서 |
| `deploy/` | 배포가 배포 트리에서 링크하는 마스터의 systemd 사용자 유닛 |

루트에는 Cargo 매니페스트, MIT 라이선스, 한국어 대장을 곁들인 `testbed.toml`, `install.toml`(새 머신에 유닛을 설치하는 데 필요한 것), `rustfmt.toml`, `CLAUDE.md`가
있습니다.

## 문서

| 문서 | 내용 |
|---|---|
| `docs/api.md` | 경계: 두 프로그램의 모든 라우트, 클라이언트와 하니스 플러그인, 중재자가 노출하거나 호출하는 모든 프로토콜, 파일, 동사, 설정, 코드, 버전 규칙, 그리고 공유 풀에서 수용이 결정되는 위치 |
| `docs/openapi.json` | 같은 라우트를 OpenAPI 3.1 계약으로 표현한 것. 클라이언트의 타입에서 생성 |
| `docs/node-arbiter.md` | 노드 중재자: 잠금, 점유자, 청구자, 데스크톱, 키퍼, 홀드, 대여, 설치 |
| `docs/placement.md` | 단위, 호스트 메모리, 덤프 저장소, 마스터와 슬레이브, 서비스 배치, 규칙 |
| `docs/PERSONAL-DATA.md` | 저장소에 개인정보를 두지 않는 규칙과, 추적되는 모든 파일에 이를 적용하는 검사 |

## 버전

- **버전은 세트 전체의 것이며,** 빌드할 때 git 태그에서 읽습니다. 마스터가 이 버전으로 답하고 노드 중재자에게
  전하며, 노드 중재자는 이를 자신의 버전으로 표시합니다. 모든 크레이트와 TypeScript 패키지의 버전은 움직이지 않는
  자리표시 값입니다.
- **버전을 어떻게 읽고 전하는지**는 `docs/api.md`의 「Versions」에 있습니다.

## 명령

```sh
cargo check --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
cargo run -p bmo-arbiter-plugin --example openapi > docs/openapi.json
bun run --cwd packages/arbiter-client generate
bun run --cwd packages/arbiter-client check
```

## 라이선스

MIT. [LICENSE](LICENSE)를 참고하십시오.
