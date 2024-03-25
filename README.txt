WSL에서 Docker-compose를 이용한  aws glue 셋업
0. aws-cli 다운로드 (아래 명령어 실행)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

1. 다운로드 받은 압축 파일 해제 (아래 명령어 실행)
unzip awscliv2.zip

2. 압축 해제한 aws폴더 내에 install 파일 실행 (아래 명령어 실행)
sudo ./aws/install

3. 잘 설치 되었는지 확인 (아래 명령어 실행)
aws --version

4. aws IAM에서 발급받은 access_key와 secret_access, region, profile_name 등을 등록합니다(아래 명령어 실행)
aws configure

5. ~/.aws 폴더 내에 config, credentials 두 개 파일이 생성된지 확인 (아래 명령어 실행)
ls ~/.aws

6. setup.sh, change-auth.sh, docker-compose.yml 파일을 작업을 수행할 폴더로 적절히 이동시킵니다.(ex /home/user/data-lake) 그리고 도커 패키지 설치(아래 명령어 실행)
source setup.sh

7. docker-compose.yml 내에 environment 설정 내 AWS_PROFILE 값에(저의 경우 kimjm) ~/.aws/config 에 기재한 profile 값으로 변경

8. 생성된 도커 컨테이너가 파일을 읽고 쓸 수 있게 권한을 수정(아래 명령어 실행)
source change-auth.sh

9. 이후에는 ctrl + c 로 도커 컴포즈 종료가 가능하며, -d 옵션(daemon, 데몬)을 주어 백그라운드로 실행할 수도 있습니다(로그를 보기 번거롭기에 개발환경에서는 보통 사용하지 않습니다)
docker-compose up -d

10. docker-compose.yml 이 아닌 직관적인 파일 이름(glue.yml로 변경)으로 변경하고 싶으시다면 변경후 -f 옵션(file)을 사용할 수 있습니다.
docker-compose -f glue.yml up


etc..
- docker-compose 파일 내에 volume영역의 경우 좌측(호스트) 경로에 해당 파일/폴더가 존재하지 않으면 생성하며 구동됩니다.
즉, jupyter_workspace 폴더를 안 만들고 docker-compose up 을 실행해도 실행 경로에서 1-depth 나아가 jupyter_workspace폴더 생성합니다.
- 이렇게 docker가 생성한 폴더는 보통 권한이 도커에게 있습니다. 그래서 로컬 개발환경에서는 "chmod -R 777 폴더이름" 등의 명령어로 권한을 느슨하게 해줍니다
이 내용은  change-auth.sh 파일에서 처리합니다.