# 도커가 aws 자격 증명을 읽을 수 있게 권한을 느슨하게
sudo chmod -R 777 ~/.aws/config ~/.aws/credentials

# 도커가 jupyter_workspace 폴더에 파일을 읽고 쓸 수 있도록 권한을 느슨하게
sudo chmod -R 777 jupyter_workspace