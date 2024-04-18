# 사전에 수행해야하는 커맨드
# mkdir data-lake 폴더 생성 (이름 원하는 대로)
# aws configue --profile <IAM에 등록해놓고, Access Key 발급받은 Profile 이름> 

# apt 및 apt-get 패키지 관리자 업데이트
# -y 옵션은 yes or no 묻는 질문에 yes로 답합니다.
sudo apt update -y
sudo apt-get update -y

# 도커 및 도커-컴포즈 설치
sudo apt install docker -y
sudo apt install docker-compose -y

# 도커 컴포즈 구동
sudo docker-compose -f glue.yml up
