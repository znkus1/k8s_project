# 항공데이터 지연시간 예측 ML서비스

## 프로젝트 개요
- **프로젝트 한 줄 설명**:
    - 항공데이터를 입력시에 지연시간을 예측하는 ML서비스 제공.
    - 쿠버네티스를 활용하여 MLOps 파이프라인을 구축.
- **프로젝트 배경 또는 기획 의도**:
    - 항공 데이터(csv)를 이용하여 EDA를 진행, 어떠한 서비스를 할 지 분석하였고 항공 정보를 입력하면 지연시간을 예측하는 ML 서비스를 제공.
- **팀 프로젝트** : 5인
- **팀 프로젝트 참여 인원 및 본인 역할**:
    - 항공 데이터 EDA.
    - Kubernetes 환경에서 Spark 앱 배포.
    - Pyspark를 이용한 머신러닝 모델 학습.
- **진행 기간 및 소속**:
    - 2023.09.26 - 2023.11.03 (4주).

## 기술 스택 및 데이터셋 설명
- **활용 기술 스택**:
    - AWS EC2, Kubernetes, Flask, Kafka, Spark, Airflow, miniO, PostgreSQL, Prometheus, Grafana, Loki, ArgoCD.
- **데이터 출처, 용량, 특징 등**:
    - 항공데이터(csv파일) 약 240만 행의 데이터.
      
![archi](https://github.com/znkus1/youtube-re-project/assets/130662133/d055b460-a778-49a3-9696-7a72e3d106c1)

## 프로젝트 진행 단계
- 쿠버네티스 클러스터 구축.
- CI/CD, 모니터링.
- 데이터 파이프라인 구축.
- 머신러닝 모델 학습.
- 웹서버 구축.

## 프로젝트 세부 과정
- **쿠버네티스(Kubernetes) 클러스터 구축**:
    - AWS EC2 환경에서 쿠버네티스 클러스터를 구축.
- **CI/CD 모니터링**:
    - github action을 활용.
    - Prometheus, Grafana를 컨테이너 로그 모니터링.
- **데이터파이프라인 구축**:
    - Kafka를 통해 데이터 수집하여 PostgreSQL에 저장.
    - Airflow 일정한 주기로 데이터를 DB에서 MiniO에 적재.
- **Spark를 활용한 머신러닝 모델 학습**:
    - pyspark를 이용해서 머신러닝 코드 작성 (선형회귀 모델).
    - 학습 후에 모델을 MiniO에 저장.
- **Flask 웹서버를 이용해서 사용자가 예측 결과를 직접 확인**.
<img width="1361" alt="IMAGE" src="https://github.com/znkus1/youtube-re-project/assets/130662133/8e35a20d-f82e-4d99-9e41-43ccb2ce7aaa">

## 프로젝트 회고
- **잘 한점**:
    - 작업과정을 Github wiki에 정리함으로 모든 팀원들이 과정들을 쉽게 확인하고 공유 가능하게 함.
- **한계점 및 개선 방안**:
    - 쿠버네티스에 대한 이해도를 높일 수 있었으며 향후에 쿠버네티스 클러스터를 구축함에 있어서 AWS에 EKS 서비스를 사용해서 구현 해볼 계획.

