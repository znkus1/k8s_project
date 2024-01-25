### 프로젝트 개요

- 항공데이터를 입력시에 지연시간을 예측하는 ML서비스 제공
- 쿠버네티스를 활용하여 MLOps 파이프라인을 구축

### 팀원 및 역할

- 팀원(5인)
- 항공 데이터 EDA
- Kubernetes 환경에서 Spark 앱 배포
- Pyspark를 이용한 머신러닝 모델 학습

### 프로젝트 기간

2023.09.26 - 2023.11.03 (4주)

### 사용 기술

AWS EC2, Kubernetes, Flask, Kafka, Spark, Airflow, miniO, PostgreSQL,
Prometheus, Grafana, Loki, ArgoCD

### 아키텍처

![스크린샷 2024-01-25 오전 10.25.59.png](https://prod-files-secure.s3.us-west-2.amazonaws.com/523729f7-133e-4867-b5f9-14f93e19bf95/fa55a760-2994-478c-8b29-3db13dc808dd/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2024-01-25_%E1%84%8B%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%AB_10.25.59.png)

### 프로젝트 세부사항

- AWS EC2 환경에서 쿠버네티스 클러스터 구축
- Kafka, Airflow를 이용해서 일정한 주기로 데이터 수집 및 저장
- Spark를 이용한 머신러닝 모델 학습
- Flask 웹서버를 이용해서 사용자가 예측 결과를 직접 확인
- Prometheus, Grafana 등을 이용한 모니터링
- Github Action, ArgoCD를 이용한  CI / CD 구현

### 프로젝트 리뷰

- 쿠버네티스에 대한 이해도를 높일 수 있었으며 향후 EKS 서비스를 사용해서 쿠버네티스 클러스터 구현 해볼 계획
- 
